/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

import org.mockito.Mockito;
import org.opensearch.test.OpenSearchTestCase;

public class ModelProfileOnNodeTests extends OpenSearchTestCase {
    // "equals: same reference -> true"
    public void test_equals_returnsTrue_whenSameReference() {
        ModelProfile dummyProfile = Mockito.mock(ModelProfile.class);
        ModelProfileOnNode node = new ModelProfileOnNode("node-1", dummyProfile);

        assertTrue("equals should return true when comparing the same reference", node.equals(node));
    }

    // "equals: null argument ⇒ false"
    public void test_equals_returnsFalse_whenArgumentIsNull() {
        ModelProfile dummyProfile = Mockito.mock(ModelProfile.class);
        ModelProfileOnNode node = new ModelProfileOnNode("node-1", dummyProfile);

        assertFalse("equals should return false when argument is null", node.equals(null));
    }

    // "equals: different class ⇒ false"
    public void test_equals_returnsFalse_whenDifferentClass() {
        ModelProfile dummyProfile = Mockito.mock(ModelProfile.class);
        ModelProfileOnNode node = new ModelProfileOnNode("node-1", dummyProfile);

        Object other = "some string";          // any non-ModelProfileOnNode object will do
        assertFalse("equals should return false when classes differ", node.equals(other));
    }

    // "getModelProfile returns the same instance that was supplied to the constructor"
    public void test_getModelProfile_returnsOriginalInstance() {
        ModelProfile dummyProfile = Mockito.mock(ModelProfile.class);
        ModelProfileOnNode node = new ModelProfileOnNode("node-1", dummyProfile);

        assertEquals("getModelProfile should return the same instance passed to the constructor", dummyProfile, node.getModelProfile());
    }
}
