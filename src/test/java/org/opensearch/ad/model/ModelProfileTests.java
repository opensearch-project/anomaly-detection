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

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.ModelProfile;

import test.org.opensearch.ad.util.JsonDeserializer;

public class ModelProfileTests extends AbstractTimeSeriesTest {

    public void testToXContent() throws IOException {
        ModelProfile profile1 = new ModelProfile(
            randomAlphaOfLength(5),
            Entity.createSingleAttributeEntity(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            0
        );
        XContentBuilder builder = getBuilder(profile1);
        String json = builder.toString();
        assertTrue(JsonDeserializer.hasChildNode(json, CommonName.ENTITY_KEY));
        assertFalse(JsonDeserializer.hasChildNode(json, CommonName.MODEL_SIZE_IN_BYTES));

        ModelProfile profile2 = new ModelProfile(randomAlphaOfLength(5), null, 1);

        builder = getBuilder(profile2);
        json = builder.toString();

        assertFalse(JsonDeserializer.hasChildNode(json, CommonName.ENTITY_KEY));
        assertTrue(JsonDeserializer.hasChildNode(json, CommonName.MODEL_SIZE_IN_BYTES));

    }

    private XContentBuilder getBuilder(ModelProfile profile) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        profile.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder;
    }
}
