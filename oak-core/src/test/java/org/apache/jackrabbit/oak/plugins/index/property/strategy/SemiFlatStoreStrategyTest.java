package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.junit.Assert;

public class SemiFlatStoreStrategyTest {
    private static final Set<String> EMPTY = Sets.newHashSet();
    private static final String DEFVAL = "val";
    private static final Set<String> VALUES = Sets.newHashSet(DEFVAL);
    private static final Filter EMPTY_FILTER = new FilterImpl();

    private SemiFlatStoreStrategy store;
    private NodeBuilder indexMeta;
    private NodeBuilder index;

    @Before
    public void setUp() {
        store = new SemiFlatStoreStrategy(2);
        indexMeta = EMPTY_NODE.builder();
        index = indexMeta.child(INDEX_CONTENT_NODE_NAME);
    }

    @After
    public void tearDown() {
        store = null;
        indexMeta = null;
        index = null;
    }

    @Test
    public void testInsert() {
        store.update(index, "/", null, null, EMPTY, VALUES);
        checkPathExists(index, DEFVAL, "");

        store.update(index, "/a", null, null, EMPTY, VALUES);
        checkPathExists(index, DEFVAL + "/a", "a");

        store.update(index, "/a/b", null, null, EMPTY, VALUES);
        checkPathExists(index, DEFVAL + "/a-b", "a-b");

        store.update(index, "/a/b/c/d", null, null, EMPTY, VALUES);
        checkPathExists(index, DEFVAL + "/a-b/c-d", "c-d");

        store.update(index, "/a/e/f/g/h", null, null, EMPTY, VALUES);
        checkPathExists(index, DEFVAL + "/a-e/f-g/h", "h");

        store.update(index, "/a/e/f", null, null, EMPTY, VALUES);
        checkPathExists(index, DEFVAL + "/a-e/f-g");
        checkPathNotExists(index, DEFVAL + "/a-e/f");
    }

    @Test
    public void testRemove() {
        Iterable<String> matchIterator;

        store.update(index, "/", null, null, EMPTY, VALUES);
        store.update(index, "/a/b/c/d", null, null, EMPTY, VALUES);
        store.update(index, "/a/b/f", null, null, EMPTY, VALUES);
        store.update(index, "/a", null, null, EMPTY, VALUES);

        // should be a NOP, shouldn't change anything, /a/b/c wasn't indexed
        store.update(index, "/a/b/c", null, null, VALUES, EMPTY);
        checkPathExists(index, DEFVAL + "/a-b/c-d");

        store.update(index, "/a/b/c/d", null, null, VALUES, EMPTY);
        checkPathNotExists(index, DEFVAL + "/a-b/c-d");
        checkPathNotExists(index, DEFVAL + "/a-b/c");
        checkPathExists(index, DEFVAL + "/a-b");
        checkPathExists(index, DEFVAL + "/a-b/f");

        store.update(index, "/", null, null, VALUES, EMPTY);
        checkPathExists(index, DEFVAL + "/a-b/f");
        checkPathExists(index, DEFVAL + "/a-b");
        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), null);
        checkResultContainsPaths(matchIterator, "a", "a/b/f");

        store.update(index, "/a/b/f", null, null, VALUES, EMPTY);
        checkPathExists(index, DEFVAL + "/a-b");
        checkPathNotExists(index, DEFVAL + "/a");

        store.update(index, "/a", null, null, VALUES, EMPTY);
        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), null);
        checkPathNotExists(index, DEFVAL);
    }

    @Test
    public void testQueryCheckIfPropertyExists() {
        Iterable<String> matchIterator;

        store.update(index, "/", null, null, EMPTY, VALUES);
        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), null);
        checkResultContainsPaths(matchIterator, "");

        store.update(index, "/a", null, null, EMPTY, VALUES);
        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), null);
        checkResultContainsPaths(matchIterator, "", "a");

        store.update(index, "/a/b/c/d/e/f/g", null, null, EMPTY, VALUES);
        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), null);
        checkResultContainsPaths(matchIterator, "", "a", "a/b/c/d/e/f/g");

        store.update(index, "/a/b/c", null, null, EMPTY, VALUES);
        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), null);
        checkResultContainsPaths(matchIterator, "", "a", "a/b/c/d/e/f/g", "a/b/c");
    }

    @Test
    public void testQueryWithExactPropertyValue() {
        Iterable<String> matchIterator;

        Set<String> val1 = Sets.newHashSet("val1");
        Set<String> val2 = Sets.newHashSet("val2");

        store.update(index, "/a", null, null, EMPTY, val1);
        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), val1);
        checkResultContainsPaths(matchIterator, "a");

        store.update(index, "/b", null, null, EMPTY, val2);
        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), val2);
        checkResultContainsPaths(matchIterator, "b");

        store.update(index, "/a", null, null, EMPTY, val2);
        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), val2);
        checkResultContainsPaths(matchIterator, "b", "a");
    }

    @Test
    public void testQueryResultsForDuplicates() {
        Iterator<String> matchIterator;

        Set<String> val1 = Sets.newHashSet("val1");
        Set<String> val2 = Sets.newHashSet("val2");

        store.update(index, "/a", null, null, EMPTY, val1);
        store.update(index, "/a", null, null, EMPTY, val2);

        matchIterator = store.query(EMPTY_FILTER, "", indexMeta.getNodeState(), null).iterator();
        Assert.assertEquals("a", matchIterator.next());
        Assert.assertFalse("result contains duplicates", matchIterator.hasNext());
    }

    @Test
    public void testQueryWithPathFilter() {
        Iterable<String> matchIterator;

        store.update(index, "/", null, null, EMPTY, VALUES);
        store.update(index, "/a/b", null, null, EMPTY, VALUES);
        store.update(index, "/a/b/c/d", null, null, EMPTY, VALUES);

        FilterImpl filter = new FilterImpl();
        filter.setPath("/a/b/c");
        matchIterator = store.query(filter, "", indexMeta.getNodeState(), null);
        checkResultContainsPaths(matchIterator, "a/b/c/d");

        filter = new FilterImpl();
        filter.setPath("/a/b");
        matchIterator = store.query(filter, "", indexMeta.getNodeState(), null);
        checkResultContainsPaths(matchIterator, "a/b", "a/b/c/d");
        filter = new FilterImpl();

        filter.setPath("/");
        matchIterator = store.query(filter, "", indexMeta.getNodeState(), null);
        checkResultContainsPaths(matchIterator, "", "a/b", "a/b/c/d");
        filter = new FilterImpl();
    }

    private void checkResultContainsPaths(Iterable<String> matchIterator, String... paths) {
        Set<String> matches = Sets.newHashSet();
        Iterables.addAll(matches, matchIterator);

        Assert.assertTrue("found more matches than expected " + matches,
                matches.size() == paths.length);

        for (String expectedPath : paths) {
            Assert.assertTrue("path not returned: " + expectedPath,
                    matches.contains(expectedPath));
        }
    }

    private void checkPathExists(NodeBuilder index, String path, String matchName) {
        NodeBuilder node = checkPathExists(index, path);
        Assert.assertTrue(node.hasProperty(SemiFlatStoreStrategy.matchName(matchName)));
    }

    private NodeBuilder checkPathExists(NodeBuilder index, String path) {
        NodeBuilder node = index;
        for (String p : PathUtils.elements(path)) {
            Assert.assertTrue("Missing child node " + p + " on path " + path,
                    node.hasChildNode(p));
            node = node.getChildNode(p);
        }
        return node;
    }

    private void checkPathNotExists(NodeBuilder index, String path) {
        NodeBuilder node = index;
        Iterator<String> it = PathUtils.elements(path).iterator();
        while (it.hasNext()) {
            String p = it.next();
            if (it.hasNext()) {
                Assert.assertTrue("Missing child node " + p + " on path " + path,
                        node.hasChildNode(p));
                node = node.getChildNode(p);
            } else {
                Assert.assertFalse("Existing node " + p + " on path " + path,
                        node.hasChildNode(p));
            }
        }
    }



    @Test
    public void testFlattenPath() {
        checkFlattening(1, new String[][] {
            // simple ones
            { "", "" },
            { "/", "/" },
            { "/a", "/a" },
            { "/a/b", "/a/b" },
            { "/a/b/c", "/a/b/c" },
            // test dash escaping
            { "/a-b", "/a\\-b" },
            { "/a-b/c", "/a\\-b/c" },
        });
        checkFlattening(3, new String[][] {
            // simple ones
            { "", "" },
            { "/", "/" },
            { "/a", "/a" },
            { "/a/b", "/a-b" },
            { "/a/b/c", "/a-b-c" },
            { "/a/b/c/d", "/a-b-c/d" },
            { "/a/b/c/d/e", "/a-b-c/d-e" },
            // test dash escaping
            { "/a-b", "/a\\-b" },
            { "/a-b/c", "/a\\-b-c" },
            { "/a-b/c-d", "/a\\-b-c\\-d" },
            { "/a-b/c-d/e", "/a\\-b-c\\-d-e" },
            { "/a-b/c-d/e/f", "/a\\-b-c\\-d-e/f" },
        });
    }

    private void checkFlattening(int depth, String[][] paths) {
        for (String[] values : paths) {
            store = new SemiFlatStoreStrategy(depth);
            Assert.assertEquals(values[1], store.flattenPath(values[0]));
            Assert.assertEquals(values[0], store.unflattenPath(values[1]));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnflattenPathError() {
        store.unflattenPath("/a\\b");
    }

    @Test
    public void testIsAncestorOrSelf() {
        Assert.assertTrue(store.isAncestorOrSelf("/", "/"));
        Assert.assertTrue(store.isAncestorOrSelf("/a", "/a"));
        Assert.assertTrue(store.isAncestorOrSelf("/a-b/c-d", "/a-b/c-d"));

        Assert.assertTrue(store.isAncestorOrSelf("/", "/a"));
        Assert.assertTrue(store.isAncestorOrSelf("/a", "/a/b"));
        Assert.assertTrue(store.isAncestorOrSelf("/a/b", "/a/b/c"));
        Assert.assertTrue(store.isAncestorOrSelf("/a", "/a-b"));
        Assert.assertTrue(store.isAncestorOrSelf("/a-b", "/a-b-c"));

        // test dash encoding
        Assert.assertTrue(store.isAncestorOrSelf("/a\\-b", "/a\\-b-c"));
        Assert.assertTrue(store.isAncestorOrSelf("/a\\-b-c-d/e", "/a\\-b-c-d/e-f"));

        Assert.assertFalse(store.isAncestorOrSelf("/", "a-b"));
        Assert.assertFalse(store.isAncestorOrSelf("/a-b", "a-b"));
    }
}
