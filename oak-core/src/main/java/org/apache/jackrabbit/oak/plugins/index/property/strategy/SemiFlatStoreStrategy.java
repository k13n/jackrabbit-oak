package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy.TRAVERSING_WARN;

import java.util.Deque;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.query.FilterIterators;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

public class SemiFlatStoreStrategy implements IndexStoreStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(SemiFlatStoreStrategy.class);
    private static final String MATCH_PREFIX = "match_";
    private final int DEPTH;

    public SemiFlatStoreStrategy(int depth) {
        LOG.debug("create SemiFlatStoreStrategy with depth: {}", depth);
        DEPTH = depth;
    }

    @Override
    public void update(
            NodeBuilder index, String path,
            @Nullable final String indexName,
            @Nullable final NodeBuilder indexMeta,
            Set<String> beforeKeys, Set<String> afterKeys) {
        for (String key : beforeKeys) {
            remove(index, key, path);
        }
        for (String key : afterKeys) {
            insert(index, key, path);
        }
    }

    private void insert(NodeBuilder index, String indexedValue, String path) {
        LOG.debug("index for value \"{}\" the path \"{}\"", indexedValue, path);
        path = flattenPath(path);
        NodeBuilder builder = index.child(indexedValue);

        String lastName = ROOT_PATH;
        Iterator<String> pathElements = PathUtils.elements(path).iterator();
        while (pathElements.hasNext()) {
            String name = pathElements.next();
            if (pathElements.hasNext()) {
                builder = builder.child(name);
            } else {
                boolean added = false;
                for (String child : builder.getChildNodeNames()) {
                    if (isAncestorOrSelf(name, child)) {
                        // e.g. node "a-b" exists and node "a" needs to be indexed
                        // then mark "a-b" with a matched property for node "a"
                        builder = builder.child(child);
                        added = true;
                        break;
                    } else if (isAncestorOrSelf(child, name)) {
                        // e.g. node "a" exists and node "a-b" needs to be indexed
                        // then rename child node "a" to "a-b"
                        rename(builder, child, name);
                        builder = builder.getChildNode(name);
                        added = true;
                        break;
                    }
                }
                if (!added) {
                    builder = builder.child(name);
                }
            }
            lastName = name;
        }
        builder.setProperty(matchName(lastName), true);
    }

    static String matchName(String name) {
        if (PathUtils.denotesRoot(name)) {
            return MATCH_PREFIX;
        } else {
            return MATCH_PREFIX + name;
        }
    }

    static String matchedName(String match) {
        if (MATCH_PREFIX.equals(match)) {
            return ROOT_PATH;
        } else {
            return match.substring(MATCH_PREFIX.length());
        }
    }

    /**
     * Check if a node name from a flattened path is a (direct or indirect)
     * ancestor of another node name (again from a flattened path).
     * For example, a-b is an ancestor of a-b-c
     * For example, a-c is not an ancestor of a-b-c
     * For example, /a-b is an ancestor of /a-b-c
     *
     * @param ancestor the ancestor name
     * @param path     the potential offspring path
     * @return true if the path is an offspring of the ancestor
     */
    boolean isAncestorOrSelf(String ancestor, String path) {
        if (ancestor.isEmpty() || path.isEmpty()) {
            return false;
        }
        if (ancestor.equals(path)) {
            return true;
        }
        if (PathUtils.denotesRoot(ancestor) && path.charAt(0) == '/') {
            return true;
        }
        return path.startsWith(ancestor) && (
                path.charAt(ancestor.length()) == '/' ||
                path.charAt(ancestor.length()) == '-');
    }

    /**
     * Rename a child of some parent node
     *
     * @param parent        parent whose child is to be renamed
     * @param oldChildName  current name of the child
     * @param newChildName  new name of the child
     */
    private static void rename(NodeBuilder parent, String oldChildName, String newChildName) {
        boolean success = parent.getChildNode(oldChildName).moveTo(parent, newChildName);
        if (!success) {
            NodeBuilder oldChild = parent.child(oldChildName);
            NodeBuilder newChild = parent.child(newChildName);
            for (PropertyState property : oldChild.getProperties()) {
                newChild.setProperty(property);
            }
            oldChild.remove();
        }
    }

    private void remove(NodeBuilder index, String indexedValue, String path) {
        LOG.debug("remove for indexed value \"{}\" the path \"{}\"", indexedValue, path);
        path = flattenPath(path);
        NodeBuilder builder = index.getChildNode(indexedValue);

        // Collect all builders along the given path
        Deque<NodeBuilder> builders = Queues.newArrayDeque();
        builders.addFirst(builder);

        if (PathUtils.denotesRoot(path)) {
            builder.removeProperty(matchName(ROOT_PATH));
            prune(index, builders);
            return;
        }

        Iterator<String> pathElements = PathUtils.elements(path).iterator();
        while (pathElements.hasNext() && builder.exists()) {
            String name = pathElements.next();
            if (pathElements.hasNext()) {
                builder = builder.getChildNode(name);
                builders.addFirst(builder);
            } else {
                for (String child : builder.getChildNodeNames()) {
                    if (isAncestorOrSelf(name, child)) {
                        builder = builder.getChildNode(child);
                        builder.removeProperty(matchName(name));
                        builders.addFirst(builder);
                        prune(index, builders);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Physically prune a list of nodes from the index
     *
     * @param index    the current index
     * @param builders list of nodes to prune
     */
    void prune(NodeBuilder index, Deque<NodeBuilder> builders) {
        for (NodeBuilder node : builders) {
            if (hasAnyMatchValue(node) || node.getChildNodeCount(1) > 0) {
                return;
            } else if (node.exists()) {
                node.remove();
            }
        }
    }

    /**
     * Check if the given node has any match property, i.e. any
     * property name that starts with MATCH_PREFIX
     *
     * @param node the node to check for any match property
     * @return true if node has at least one match property, false otherwise
     */
    private static boolean hasAnyMatchValue(NodeBuilder node) {
        for (PropertyState property : node.getProperties()) {
            if (property.getName().startsWith(MATCH_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterable<String> query(final Filter filter, final String indexName,
            final NodeState indexMeta, final Iterable<String> values) {
        if (LOG.isDebugEnabled()) {
            String allValues = (values == null) ? null : Joiner.on(", ").join(values);
            LOG.debug("query index \"{}\"; values: \"{}\"; path filter \"{}\"; query: {}",
                    indexName, allValues, filter.getPath(), filter.getQueryStatement());
        }
        return new Iterable<String>() {
            @Override public Iterator<String> iterator() {
                NodeState index = indexMeta.getChildNode(INDEX_CONTENT_NODE_NAME);
                Deque<ChildNodeEntry> roots = Queues.newArrayDeque();
                if (values == null) {
                    // only interested property existence, i.e. property
                    // can have any value
                    Iterables.addAll(roots, index.getChildNodeEntries());
                } else {
                    for (String propertyValue : values) {
                        NodeState property = index.getChildNode(propertyValue);
                        if (property.exists()) {
                            roots.add(new MemoryChildNodeEntry("", property));
                        }
                    }
                }
                return new QueryIndexIterator(roots, filter, indexName);
            }
        };
    }

    private class QueryIndexIterator extends AbstractIterator<String> {
        private Deque<ChildNodeEntry> nodes;
        private Deque<String> paths;
        private Queue<String> bufferedMatches;
        private Set<String> resultPaths;
        private String filterPath;
        private int nrMatchesFound;
        private int nrNodesRead;
        private final Filter filter;
        private final String indexName;

        public QueryIndexIterator(Deque<ChildNodeEntry> roots, Filter filter, String indexName) {
            this.indexName = indexName;
            this.filter = filter;
            nodes = roots;
            filterPath = flattenPath(filter.getPath());
            paths = Queues.newArrayDeque();
            bufferedMatches = Queues.newArrayDeque();
            resultPaths = Sets.newHashSet();
            checkIfRootMatches();
            for (int i = 0; i < nodes.size(); ++i) {
                paths.add(ROOT_PATH);
            }
        }

        private void checkIfRootMatches() {
            Deque<ChildNodeEntry> newNodes = Queues.newArrayDeque();
            String matchName = matchName(ROOT_PATH);
            for (ChildNodeEntry rootEntry : nodes) {
                NodeState root = rootEntry.getNodeState();
                if (root.hasProperty(matchName)) {
                    addMatch(ROOT_PATH);
                }
                Iterables.addAll(newNodes, root.getChildNodeEntries());
            }
            nodes = newNodes;
        }

        @Override
        protected String computeNext() {
            if (bufferedMatches.isEmpty()) {
                searchForMoreMatches();
            }
            String path = bufferedMatches.poll();
            if (path == null) {
                return endOfData();
            } else {
                path = unflattenPath(path);
                path = PathUtils.relativize(ROOT_PATH, path);
                return path;
            }
        }

        private void searchForMoreMatches() {
            while (bufferedMatches.isEmpty() && !nodes.isEmpty()) {
                String parentPath = paths.removeLast();
                ChildNodeEntry entry = nodes.removeLast();
                NodeState node = entry.getNodeState();

                for (PropertyState property : node.getProperties()) {
                    if (property.getName().startsWith(MATCH_PREFIX)) {
                        String name = matchedName(property.getName());
                        addMatch(PathUtils.concat(parentPath, name));
                    }
                }

                String currentPath = PathUtils.concat(parentPath, entry.getName());
                if (isValidCandidate(currentPath)) {
                    for (ChildNodeEntry childEntry : node.getChildNodeEntries()) {
                        nodes.addLast(childEntry);
                        paths.addLast(currentPath);
                        nrNodesRead++;
                    }
                }
            }
        }

        private void addMatch(String path) {
            if (isAncestorOrSelf(filterPath, path) && !resultPaths.contains(path)) {
                bufferedMatches.offer(path);
                resultPaths.add(path);
                if ((++nrMatchesFound) % TRAVERSING_WARN == 0) {
                    FilterIterators.checkReadLimit(nrMatchesFound, filter.getQueryEngineSettings());
                    LOG.warn("Traversed {} nodes ({} index entries) using index {} with filter {}",
                            nrMatchesFound, nrNodesRead, indexName, filter);
                }
            }
        }

        private boolean isValidCandidate(String path) {
            return isAncestorOrSelf(filterPath, path) ||
                    isAncestorOrSelf(path, filterPath);
        }
    }

    @Override
    public long count(NodeState root, NodeState indexMeta, Set<String> values,
            int max) {
        // TODO: Compute an accurate count estimation
        return 1;
    }

    @Override
    public long count(Filter filter, NodeState root, NodeState indexMeta,
            Set<String> values, int max) {
        // TODO: Compute an accurate count estimation
        return 1;
    }

    /**
     * Flattens the path by a factor of DEPTH. See test cases
     * for examples
     *
     * @param path  the path to transform
     * @return transformed path
     */
    String flattenPath(String path) {
        if ("".equals(path) || PathUtils.denotesRoot(path)) {
            return path;
        }

        path = escapeDash(path);
        StringBuilder sb = new StringBuilder(path.length());
        sb.append('/');

        Iterator<String> iterator = PathUtils.elements(path).iterator();
        while (iterator.hasNext()) {
            for (int i = 0; i < DEPTH && iterator.hasNext(); ++i) {
                sb.append(iterator.next());
                if (i < DEPTH - 1 && iterator.hasNext()) {
                    sb.append('-');
                }
            }
            if (iterator.hasNext()) {
                sb.append('/');
            }
        }

        return sb.toString();
    }

    /**
     * Escape each dash with \- and each \ with \\
     *
     * @param s string to escape
     * @return escaped string
     */
    private static String escapeDash(String s) {
        // inspired by org.apache.jackrabbit.oak.commons.sort.EscapeUtils.escape(String)
        if (s.indexOf('-') < 0 && s.indexOf('\\') < 0) {
            return s;
        }
        int len = s.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '-':
                    sb.append("\\-");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Un-Flatten a path previously flattened with encodePath(String). See test cases
     * for examples
     *
     * @param path path to un-flatten
     * @return un-flattened path
     */
    String unflattenPath(String path) {
        // inspired by org.apache.jackrabbit.oak.commons.sort.EscapeUtils.unescape(String)
        int len = path.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = path.charAt(i);
            if (c == '\\') {
                checkState(i < len - 1, "Expected one more char after '\\' at [%s] in [%s]", i, path);
                char nextChar = path.charAt(i + 1);
                switch (nextChar) {
                    case '-':
                        sb.append('-');
                        i++;
                        break;
                    case '\\':
                        sb.append('\\');
                        i++;
                        break;
                    default:
                        String msg = String.format("Unexpected char [%c] found at %d of [%s]. " +
                                "Expected '\\' or 'r' or 'n", nextChar, i, path);
                        throw new IllegalArgumentException(msg);
                }
            } else if (c == '-') {
                sb.append('/');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    @Override
    public boolean exists(NodeBuilder index, String key) {
        // unsupported because it's also unsupported in ContentMirrorStoreStrategy
        throw new UnsupportedOperationException();
    }

}
