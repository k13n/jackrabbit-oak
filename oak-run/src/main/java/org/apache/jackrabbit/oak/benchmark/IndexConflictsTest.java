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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils;
import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils.PropertyIndex;
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

    private AtomicInteger nodeCounter;

    @Override
    public void beforeSuite() throws RepositoryException {
        nodeCounter = new AtomicInteger();

        // create a new root node under which all children are organized
        Session session = loginWriter();
        session.getRootNode().addNode(ROOT_NODE_NAME, NODE_TYPE);
        session.save();

        // create an index
        PropertyIndex index = new OakIndexUtils.PropertyIndex();
        index.property(INDEXED_PROPERTY);
        index.create(session);
        session.save();
    }

    @Override
    public void beforeTest() throws RepositoryException {
        // nothing so far
    }

    @Override
    public void runTest() throws Exception {
        Session session = loginWriter();
        Node rootNode = session.getNode("/" + ROOT_NODE_NAME);
        try {
            // use always the same name per thread to avoid
            // too much garbage in the index (see OAK-2621 and OAK-1557)
            String nodeName = "node" + Thread.currentThread().getId();
            for (int i = 0; i < NODES_PER_RUN; i++) {
                // generate a new node and assign the indexed property
                Node newNode = rootNode.addNode(nodeName, NODE_TYPE);
                newNode.setProperty(INDEXED_PROPERTY, propertyValue());
                nodeCounter.incrementAndGet();
                session.save();

                // remove the newly created node
                newNode.remove();
                session.save();
            }
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract String propertyValue();

    @Override
    public void afterTest() throws RepositoryException {
        // nothing so far
    }

    @Override
    protected void afterSuite() throws Exception {
        System.out.println("Nodes created: " + nodeCounter.get());
        System.out.println("Conflicts: " + Commit.conflictCounter.get());
        System.out.println();
        Commit.conflictCounter.set(0);
    }

}
