/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils;
import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils.PropertyIndex;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.plugins.document.Commit;

/**
 * Test for detecting the number of conflicts that occur while creating many
 * nodes with the same indexed property value.
 * <p>
 * This test spreads out the created nodes over a number of parent.
 */
public abstract class IndexConflictsTest extends AbstractTest<Void> {
    private static final String NODE_TYPE = "oak:Unstructured";
    private static final String ROOT_NODE_NAME = "test" + TEST_ID;
    private static final String INDEXED_PROPERTY = "indexedProperty";
    private static final int NODES_PER_RUN = 50;
    private static final int CLUSTER_SIZE = Integer.getInteger("clusterSize", 1);
    private static final boolean VERBOSE = Boolean.getBoolean("verbose");

    private AtomicInteger nodeCounter;
    private Map<Class<? extends Exception>, Integer> errorCounter;

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        return fixture.setUpCluster(CLUSTER_SIZE);
    }

    @Override
    public void beforeSuite() throws RepositoryException {
        nodeCounter = new AtomicInteger();
        errorCounter = new HashMap<Class<? extends Exception>, Integer>();

        // create a new root node under which all children are organized
        Session session = loginWriter();
        session.getRootNode().addNode(ROOT_NODE_NAME, NODE_TYPE);
        session.save();

        // create an index
        PropertyIndex index = new OakIndexUtils.PropertyIndex();
        index.property(INDEXED_PROPERTY);
        index.create(session);
        session.save();

        // wait for root node and index to propagate
        optimisticWaitForEventualConsistency();
    }

    public static void optimisticWaitForEventualConsistency() {
        try {
            // wait, hoping that in the meantime changes propagate
            // to all cluster nodes
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void beforeTest() throws RepositoryException {
        // nothing so far
    }

    @Override
    public void runTest() throws Exception {
        long threadId = Thread.currentThread().getId();

        // get a session to *some* repository in the cluster
        Session session = loginRandomClusterWriter(threadId);
        Node rootNode = session.getNode("/" + ROOT_NODE_NAME);

        // use always the same node name per thread to avoid
        // too much garbage in the index (see OAK-2620 and OAK-1557)
        String nodeName = "node" + threadId;

        for (int i = 0; i < NODES_PER_RUN; i++) {
            try {
                // generate a new node and assign the indexed property
                Node newNode = rootNode.addNode(nodeName, NODE_TYPE);
                newNode.setProperty(INDEXED_PROPERTY, propertyValue());
                session.save();
                nodeCounter.incrementAndGet();

                // remove the newly created node
                newNode.remove();
                session.save();
            } catch (RepositoryException e) {
                synchronized (errorCounter) {
                    Integer counter = errorCounter.getOrDefault(e.getClass(), 0);
                    errorCounter.put(e.getClass(), counter + 1);
                }
                if (VERBOSE) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected abstract String propertyValue();

    @Override
    public void afterTest() throws RepositoryException {
        // nothing so far
    }

    @Override
    protected void afterSuite() throws Exception {
        System.out.println("Cluster Size: " + CLUSTER_SIZE);
        System.out.println("Nodes created: " + nodeCounter.get());
        System.out.println("Conflicts: " + Commit.conflictCounter.get());
        if (!errorCounter.isEmpty()) {
            System.out.println("Exceptions thrown: ");
            for (Map.Entry<Class<? extends Exception>, Integer> entry : errorCounter.entrySet()) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue());
            }
        }
        System.out.println();
        Commit.conflictCounter.set(0);
    }

}
