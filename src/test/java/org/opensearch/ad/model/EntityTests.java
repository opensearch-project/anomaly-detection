/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad.model;

import java.util.Collections;
import java.util.Optional;
import java.util.TreeMap;

import org.opensearch.ad.AbstractADTest;

public class EntityTests extends AbstractADTest {
    /**
     * Test that toStrign has no random string, but only attributes
     */
    public void testToString() {
        TreeMap<String, String> attributes = new TreeMap<>();
        String name1 = "host";
        String val1 = "server_2";
        String name2 = "service";
        String val2 = "app_4";
        attributes.put(name1, val1);
        attributes.put(name2, val2);
        String detectorId = "detectorId";
        Entity entity = Entity.createEntityFromOrderedMap(attributes);
        assertEquals("host=server_2,service=app_4", entity.toString());
    }

    public void test_getModelId_returnId_withNoAttributes() {
        String detectorId = "id";
        Entity entity = Entity.createEntityByReordering(Collections.emptyMap());

        Optional<String> modelId = entity.getModelId(detectorId);

        assertTrue(modelId.isPresent());
        assertEquals(detectorId, modelId.get());
    }
}
