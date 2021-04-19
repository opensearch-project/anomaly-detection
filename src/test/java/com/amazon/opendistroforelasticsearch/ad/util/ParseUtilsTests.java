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

package com.amazon.opendistroforelasticsearch.ad.util;

import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.addUserBackendRolesFilter;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorType;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.google.common.collect.ImmutableList;

public class ParseUtilsTests extends OpenSearchTestCase {

    public void testToInstant() throws IOException {
        long epochMilli = Instant.now().toEpochMilli();
        XContentBuilder builder = XContentFactory.jsonBuilder().value(epochMilli);
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        Instant instant = ParseUtils.toInstant(parser);
        assertEquals(epochMilli, instant.toEpochMilli());
    }

    public void testToInstantWithNullToken() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value((Long) null);
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        XContentParser.Token token = parser.currentToken();
        assertEquals(token, XContentParser.Token.VALUE_NULL);
        Instant instant = ParseUtils.toInstant(parser);
        assertNull(instant);
    }

    public void testToInstantWithNullValue() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value(randomLong());
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        parser.nextToken();
        XContentParser.Token token = parser.currentToken();
        assertNull(token);
        Instant instant = ParseUtils.toInstant(parser);
        assertNull(instant);
    }

    public void testToInstantWithNotValue() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().nullField("test").endObject();
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        Instant instant = ParseUtils.toInstant(parser);
        assertNull(instant);
    }

    public void testToAggregationBuilder() throws IOException {
        XContentParser parser = TestHelpers.parser("{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}");
        AggregationBuilder aggregationBuilder = ParseUtils.toAggregationBuilder(parser);
        assertNotNull(aggregationBuilder);
        assertEquals("aa", aggregationBuilder.getName());
    }

    public void testParseAggregatorsWithAggregationQueryString() throws IOException {
        AggregatorFactories.Builder agg = ParseUtils
            .parseAggregators("{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}", TestHelpers.xContentRegistry(), "test");
        assertEquals("test", agg.getAggregatorFactories().iterator().next().getName());
    }

    public void testParseAggregatorsWithAggregationQueryStringAndNullAggName() throws IOException {
        AggregatorFactories.Builder agg = ParseUtils
            .parseAggregators("{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}", TestHelpers.xContentRegistry(), null);
        assertEquals("aa", agg.getAggregatorFactories().iterator().next().getName());
    }

    public void testGenerateInternalFeatureQuery() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        long startTime = randomLong();
        long endTime = randomLong();
        SearchSourceBuilder builder = ParseUtils.generateInternalFeatureQuery(detector, startTime, endTime, TestHelpers.xContentRegistry());
        for (Feature feature : detector.getFeatureAttributes()) {
            assertTrue(builder.toString().contains(feature.getId()));
        }
    }

    public void testGenerateInternalFeatureQueryTemplate() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        String builder = ParseUtils.generateInternalFeatureQueryTemplate(detector, TestHelpers.xContentRegistry());
        for (Feature feature : detector.getFeatureAttributes()) {
            assertTrue(builder.contains(feature.getId()));
        }
    }

    public void testAddUserRoleFilterWithNullUser() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        addUserBackendRolesFilter(null, searchSourceBuilder);
        assertEquals("{}", searchSourceBuilder.toString());
    }

    public void testAddUserRoleFilterWithNullUserBackendRole() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        addUserBackendRolesFilter(
            new User(randomAlphaOfLength(5), null, ImmutableList.of(randomAlphaOfLength(5)), ImmutableList.of(randomAlphaOfLength(5))),
            searchSourceBuilder
        );
        assertEquals(
            "{\"query\":{\"bool\":{\"must\":[{\"nested\":{\"query\":{\"terms\":{\"user.backend_roles.keyword\":[],"
                + "\"boost\":1.0}},\"path\":\"user\",\"ignore_unmapped\":false,\"score_mode\":\"none\",\"boost\":1.0}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}}",
            searchSourceBuilder.toString()
        );
    }

    public void testAddUserRoleFilterWithEmptyUserBackendRole() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        addUserBackendRolesFilter(
            new User(
                randomAlphaOfLength(5),
                ImmutableList.of(),
                ImmutableList.of(randomAlphaOfLength(5)),
                ImmutableList.of(randomAlphaOfLength(5))
            ),
            searchSourceBuilder
        );
        assertEquals(
            "{\"query\":{\"bool\":{\"must\":[{\"nested\":{\"query\":{\"terms\":{\"user.backend_roles.keyword\":[],"
                + "\"boost\":1.0}},\"path\":\"user\",\"ignore_unmapped\":false,\"score_mode\":\"none\",\"boost\":1.0}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}}",
            searchSourceBuilder.toString()
        );
    }

    public void testAddUserRoleFilterWithNormalUserBackendRole() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String backendRole1 = randomAlphaOfLength(5);
        String backendRole2 = randomAlphaOfLength(5);
        addUserBackendRolesFilter(
            new User(
                randomAlphaOfLength(5),
                ImmutableList.of(backendRole1, backendRole2),
                ImmutableList.of(randomAlphaOfLength(5)),
                ImmutableList.of(randomAlphaOfLength(5))
            ),
            searchSourceBuilder
        );
        assertEquals(
            "{\"query\":{\"bool\":{\"must\":[{\"nested\":{\"query\":{\"terms\":{\"user.backend_roles.keyword\":"
                + "[\""
                + backendRole1
                + "\",\""
                + backendRole2
                + "\"],"
                + "\"boost\":1.0}},\"path\":\"user\",\"ignore_unmapped\":false,\"score_mode\":\"none\",\"boost\":1.0}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}}",
            searchSourceBuilder.toString()
        );
    }

    public void testBatchFeatureQuery() throws IOException {
        String index = randomAlphaOfLength(5);
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Feature feature1 = TestHelpers.randomFeature(true);
        Feature feature2 = TestHelpers.randomFeature(false);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(index),
                ImmutableList.of(feature1, feature2),
                null,
                now,
                AnomalyDetectorType.HISTORICAL_MULTI_ENTITY.name(),
                1,
                TestHelpers.randomDetectionDateRange(),
                false
            );

        long startTime = now.minus(10, ChronoUnit.DAYS).toEpochMilli();
        long endTime = now.plus(10, ChronoUnit.DAYS).toEpochMilli();
        SearchSourceBuilder searchSourceBuilder = ParseUtils
            .batchFeatureQuery(detector, startTime, endTime, TestHelpers.xContentRegistry());
        assertEquals(
            "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"range\":{\""
                + detector.getTimeField()
                + "\":{\"from\":"
                + startTime
                + ",\"to\":"
                + endTime
                + ",\"include_lower\":true,\"include_upper\":false,\"format\":\"epoch_millis\",\"boost\""
                + ":1.0}}},{\"bool\":{\"must\":[{\"term\":{\"user\":{\"value\":\"kimchy\",\"boost\":1.0}}}],\"filter\":"
                + "[{\"term\":{\"tag\":{\"value\":\"tech\",\"boost\":1.0}}}],\"must_not\":[{\"range\":{\"age\":{\"from\":10,"
                + "\"to\":20,\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}}],\"should\":[{\"term\":{\"tag\":"
                + "{\"value\":\"wow\",\"boost\":1.0}}},{\"term\":{\"tag\":{\"value\":\"elasticsearch\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"minimum_should_match\":\"1\",\"boost\":1.0}}],\"adjust_pure_negative"
                + "\":true,\"boost\":1.0}},\"aggregations\":{\"feature_aggs\":{\"composite\":{\"size\":1000,\"sources\":"
                + "[{\"date_histogram\":{\"date_histogram\":{\"field\":\""
                + detector.getTimeField()
                + "\",\"missing_bucket\":false,\"order\":\"asc\","
                + "\"fixed_interval\":\"60s\"}}}]},\"aggregations\":{\""
                + feature1.getId()
                + "\":{\"value_count\":{\"field\":\"ok\"}}}}}}",
            searchSourceBuilder.toString()
        );
    }

    public void testBatchFeatureQueryWithoutEnabledFeature() throws IOException {
        String index = randomAlphaOfLength(5);
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(index),
                ImmutableList.of(TestHelpers.randomFeature(false)),
                null,
                now,
                AnomalyDetectorType.HISTORICAL_MULTI_ENTITY.name(),
                1,
                TestHelpers.randomDetectionDateRange(),
                false
            );

        long startTime = now.minus(10, ChronoUnit.DAYS).toEpochMilli();
        long endTime = now.plus(10, ChronoUnit.DAYS).toEpochMilli();

        AnomalyDetectionException exception = expectThrows(
            AnomalyDetectionException.class,
            () -> ParseUtils.batchFeatureQuery(detector, startTime, endTime, TestHelpers.xContentRegistry())
        );
        assertEquals("No enabled feature configured", exception.getMessage());
    }

    public void testBatchFeatureQueryWithoutFeature() throws IOException {
        String index = randomAlphaOfLength(5);
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(index),
                ImmutableList.of(),
                null,
                now,
                AnomalyDetectorType.HISTORICAL_MULTI_ENTITY.name(),
                1,
                TestHelpers.randomDetectionDateRange(),
                false
            );

        long startTime = now.minus(10, ChronoUnit.DAYS).toEpochMilli();
        long endTime = now.plus(10, ChronoUnit.DAYS).toEpochMilli();
        AnomalyDetectionException exception = expectThrows(
            AnomalyDetectionException.class,
            () -> ParseUtils.batchFeatureQuery(detector, startTime, endTime, TestHelpers.xContentRegistry())
        );
        assertEquals("No enabled feature configured", exception.getMessage());
    }
}
