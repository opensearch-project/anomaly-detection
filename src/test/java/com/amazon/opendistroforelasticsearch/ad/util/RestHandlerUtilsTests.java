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

package com.amazon.opendistroforelasticsearch.ad.util;

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.builder;
import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.randomFeature;

import java.io.IOException;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RestHandlerUtilsTests extends OpenSearchTestCase {

    public void testGetSourceContext() {
        RestRequest request = new FakeRestRequest();
        FetchSourceContext context = RestHandlerUtils.getSourceContext(request);
        assertArrayEquals(new String[] { "ui_metadata" }, context.excludes());
    }

    public void testGetSourceContextForKibana() {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        builder.withHeaders(ImmutableMap.of("User-Agent", ImmutableList.of("Kibana", randomAlphaOfLength(10))));
        FetchSourceContext context = RestHandlerUtils.getSourceContext(builder.build());
        assertNull(context);
    }

    public void testCreateXContentParser() throws IOException {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new FakeRestChannel(request, false, 1);
        XContentBuilder builder = builder().startObject().field("test", "value").endObject();
        BytesReference bytesReference = BytesReference.bytes(builder);
        XContentParser parser = RestHandlerUtils.createXContentParser(channel, bytesReference);
        parser.close();
    }

    public void testValidateAnomalyDetectorWithTooManyFeatures() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(randomFeature(), randomFeature()));
        String error = RestHandlerUtils.validateAnomalyDetector(detector, 1);
        assertEquals("Can't create anomaly features more than 1", error);
    }

    public void testValidateAnomalyDetectorWithDuplicateFeatureNames() throws IOException {
        String featureName = randomAlphaOfLength(5);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(randomFeature(featureName, randomAlphaOfLength(5)), randomFeature(featureName, randomAlphaOfLength(5)))
            );
        String error = RestHandlerUtils.validateAnomalyDetector(detector, 2);
        assertEquals("Detector has duplicate feature names: " + featureName + "\n", error);
    }

    public void testValidateAnomalyDetectorWithDuplicateAggregationNames() throws IOException {
        String aggregationName = randomAlphaOfLength(5);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList
                    .of(randomFeature(randomAlphaOfLength(5), aggregationName), randomFeature(randomAlphaOfLength(5), aggregationName))
            );
        String error = RestHandlerUtils.validateAnomalyDetector(detector, 2);
        assertEquals("Detector has duplicate feature aggregation query names: " + aggregationName, error);
    }
}
