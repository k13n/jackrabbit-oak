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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EmptyPrincipalProviderTest extends AbstractSecurityTest {

    private PrincipalProvider principalProvider = EmptyPrincipalProvider.INSTANCE;
    private Principal testPrincipal;

    @Override
    public void before() throws Exception {
        super.before();
        testPrincipal = getTestUser().getPrincipal();
    }

    @Test
    public void testGetPrincipal() {
        assertNull(principalProvider.getPrincipal(EveryonePrincipal.NAME));
        assertNull(principalProvider.getPrincipal(testPrincipal.getName()));
    }

    @Test
    public void testGetGroupMembership() {
        assertTrue(principalProvider.getGroupMembership(EveryonePrincipal.getInstance()).isEmpty());
        assertTrue(principalProvider.getGroupMembership(new PrincipalImpl(EveryonePrincipal.NAME)).isEmpty());
        assertTrue(principalProvider.getGroupMembership(testPrincipal).isEmpty());
    }

    @Test
    public void testGetPrincipals() throws Exception {
        assertTrue(principalProvider.getPrincipals(getTestUser().getID()).isEmpty());
    }

    @Test
    public void testFindPrincipalsByHint() {
        assertFalse(principalProvider.findPrincipals(EveryonePrincipal.NAME, PrincipalManager.SEARCH_TYPE_ALL).hasNext());
        assertFalse(principalProvider.findPrincipals(EveryonePrincipal.NAME.substring(0, 1), PrincipalManager.SEARCH_TYPE_ALL).hasNext());
        assertFalse(principalProvider.findPrincipals(testPrincipal.getName(), PrincipalManager.SEARCH_TYPE_ALL).hasNext());
        assertFalse(principalProvider.findPrincipals(testPrincipal.getName().substring(0, 2), PrincipalManager.SEARCH_TYPE_ALL).hasNext());
    }

    @Test
    public void testFindPrincipalsByType() {
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL).hasNext());
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP).hasNext());
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP).hasNext());
    }
}