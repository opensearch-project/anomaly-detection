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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.model;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;

import test.com.amazon.opendistroforelasticsearch.ad.util.JsonDeserializer;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;

public class EntityProfileTests extends AbstractADTest {
    public void testMerge() {
        EntityProfile profile1 = new EntityProfile(null, null, null, -1, -1, null, null, EntityState.INIT);

        EntityProfile profile2 = new EntityProfile(null, null, null, -1, -1, null, null, EntityState.UNKNOWN);

        profile1.merge(profile2);
        assertEquals(profile1.getState(), EntityState.INIT);
    }

    public void testToXContent() throws IOException, JsonPathNotFoundException {
        EntityProfile profile1 = new EntityProfile(null, null, null, -1, -1, null, null, EntityState.INIT);

        XContentBuilder builder = jsonBuilder();
        profile1.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = Strings.toString(builder);

        assertEquals("INIT", JsonDeserializer.getTextValue(json, CommonName.STATE));

        EntityProfile profile2 = new EntityProfile(null, null, null, -1, -1, null, null, EntityState.UNKNOWN);

        builder = jsonBuilder();
        profile2.toXContent(builder, ToXContent.EMPTY_PARAMS);
        json = Strings.toString(builder);

        assertTrue(false == JsonDeserializer.hasChildNode(json, CommonName.STATE));
    }
}
