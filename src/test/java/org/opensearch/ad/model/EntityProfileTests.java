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

import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.AbstractTimeSeriesTest;

import test.org.opensearch.ad.util.JsonDeserializer;

public class EntityProfileTests extends AbstractTimeSeriesTest {
    public void testMerge() {
        EntityProfile profile1 = new EntityProfile(null, -1, -1, null, null, EntityState.INIT);
        EntityProfile profile2 = new EntityProfile(null, -1, -1, null, null, EntityState.UNKNOWN);
        profile1.merge(profile2);
        assertEquals(profile1.getState(), EntityState.INIT);
        assertTrue(profile1.toString().contains(EntityState.INIT.toString()));
    }

    public void testToXContent() throws IOException, JsonPathNotFoundException {
        EntityProfile profile1 = new EntityProfile(null, -1, -1, null, null, EntityState.INIT);

        XContentBuilder builder = jsonBuilder();
        profile1.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertEquals("INIT", JsonDeserializer.getTextValue(json, ADCommonName.STATE));

        EntityProfile profile2 = new EntityProfile(null, -1, -1, null, null, EntityState.UNKNOWN);

        builder = jsonBuilder();
        profile2.toXContent(builder, ToXContent.EMPTY_PARAMS);
        json = builder.toString();

        assertTrue(false == JsonDeserializer.hasChildNode(json, ADCommonName.STATE));
    }

    public void testToXContentTimeStampAboveZero() throws IOException, JsonPathNotFoundException {
        EntityProfile profile1 = new EntityProfile(null, 1, 1, null, null, EntityState.INIT);

        XContentBuilder builder = jsonBuilder();
        profile1.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertEquals("INIT", JsonDeserializer.getTextValue(json, ADCommonName.STATE));

        EntityProfile profile2 = new EntityProfile(null, 1, 1, null, null, EntityState.UNKNOWN);

        builder = jsonBuilder();
        profile2.toXContent(builder, ToXContent.EMPTY_PARAMS);
        json = builder.toString();

        assertTrue(false == JsonDeserializer.hasChildNode(json, ADCommonName.STATE));
    }
}
