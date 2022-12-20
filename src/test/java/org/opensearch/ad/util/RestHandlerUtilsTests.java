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

package org.opensearch.ad.util;

import static org.opensearch.ad.TestHelpers.builder;
import static org.opensearch.ad.TestHelpers.randomFeature;
import static org.opensearch.ad.util.RestHandlerUtils.OPENSEARCH_DASHBOARDS_USER_AGENT;

import java.io.IOException;

import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RestHandlerUtilsTests extends OpenSearchTestCase {

    public void testGetSourceContextFromOpenSearchDashboardEmptyExcludes() {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        builder.withHeaders(ImmutableMap.of("User-Agent", ImmutableList.of(OPENSEARCH_DASHBOARDS_USER_AGENT, randomAlphaOfLength(10))));
        SearchSourceBuilder testSearchSourceBuilder = new SearchSourceBuilder();
        testSearchSourceBuilder.fetchSource(new String[] { "a" }, new String[0]);
        FetchSourceContext sourceContext = RestHandlerUtils.getSourceContext(builder.build(), testSearchSourceBuilder);
        assertArrayEquals(new String[] { "a" }, sourceContext.includes());
        assertEquals(0, sourceContext.excludes().length);
        assertEquals(1, sourceContext.includes().length);
    }

    public void testGetSourceContextFromClientWithExcludes() {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        SearchSourceBuilder testSearchSourceBuilder = new SearchSourceBuilder();
        testSearchSourceBuilder.fetchSource(new String[] { "a" }, new String[] { "b" });
        FetchSourceContext sourceContext = RestHandlerUtils.getSourceContext(builder.build(), testSearchSourceBuilder);
        assertEquals(sourceContext.excludes().length, 2);
    }

    public void testGetSourceContextFromClientWithoutSource() {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        SearchSourceBuilder testSearchSourceBuilder = new SearchSourceBuilder();
        FetchSourceContext sourceContext = RestHandlerUtils.getSourceContext(builder.build(), testSearchSourceBuilder);
        assertEquals(sourceContext.excludes().length, 1);
        assertEquals(sourceContext.includes().length, 0);
    }

    public void testGetSourceContextOpenSearchDashboardWithoutSources() {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        builder.withHeaders(ImmutableMap.of("User-Agent", ImmutableList.of(OPENSEARCH_DASHBOARDS_USER_AGENT, randomAlphaOfLength(10))));
        SearchSourceBuilder testSearchSourceBuilder = new SearchSourceBuilder();
        FetchSourceContext sourceContext = RestHandlerUtils.getSourceContext(builder.build(), testSearchSourceBuilder);
        assertNull(sourceContext);
    }

    public void testCreateXContentParser() throws IOException {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new FakeRestChannel(request, false, 1);
        XContentBuilder builder = builder().startObject().field("test", "value").endObject();
        BytesReference bytesReference = BytesReference.bytes(builder);
        XContentParser parser = RestHandlerUtils.createXContentParser(channel, bytesReference);
        parser.close();
    }

    public void testisExceptionCausedByInvalidQueryNotSearchPhaseException() {
        assertFalse(RestHandlerUtils.isExceptionCausedByInvalidQuery(new IllegalArgumentException()));
    }

    public void testValidateAnomalyDetectorWithTooManyFeatures() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(randomFeature(), randomFeature()));
        String error = RestHandlerUtils.checkAnomalyDetectorFeaturesSyntax(detector, 1);
        assertEquals("Can't create more than 1 anomaly features", error);
    }

    public void testValidateAnomalyDetectorWithDuplicateFeatureNames() throws IOException {
        String featureName = randomAlphaOfLength(5);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(randomFeature(featureName, randomAlphaOfLength(5)), randomFeature(featureName, randomAlphaOfLength(5)))
            );
        String error = RestHandlerUtils.checkAnomalyDetectorFeaturesSyntax(detector, 2);
        assertEquals("Detector has duplicate feature names: " + featureName, error);
    }

    public void testValidateAnomalyDetectorWithDuplicateAggregationNames() throws IOException {
        String aggregationName = randomAlphaOfLength(5);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList
                    .of(randomFeature(randomAlphaOfLength(5), aggregationName), randomFeature(randomAlphaOfLength(5), aggregationName))
            );
        String error = RestHandlerUtils.checkAnomalyDetectorFeaturesSyntax(detector, 2);
        assertEquals("Detector has duplicate feature aggregation query names: " + aggregationName, error);
    }
}
