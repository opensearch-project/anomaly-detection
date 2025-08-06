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

import static org.opensearch.timeseries.util.ParseUtils.isAdmin;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.util.ParseUtils;

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

    public void testParseAggregatorsWithInvalidAggregationName() throws IOException {
        XContentParser parser = ParseUtils.parser("{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}", TestHelpers.xContentRegistry());
        Exception ex = expectThrows(ParsingException.class, () -> ParseUtils.parseAggregators(parser, 0, "#@?><:{"));
        assertTrue(ex.getMessage().contains("Aggregation names must be alpha-numeric and can only contain '_' and '-'"));
    }

    public void testParseAggregatorsWithTwoAggregationTypes() throws IOException {
        XContentParser parser = ParseUtils
            .parser("{\"test\":{\"avg\":{\"field\":\"value\"},\"sum\":{\"field\":\"value\"}}}", TestHelpers.xContentRegistry());
        Exception ex = expectThrows(ParsingException.class, () -> ParseUtils.parseAggregators(parser, 0, "test"));
        assertTrue(ex.getMessage().contains("Found two aggregation type definitions in"));
    }

    public void testParseAggregatorsWithNullAggregationDefinition() throws IOException {
        String aggName = "test";
        XContentParser parser = ParseUtils.parser("{\"test\":{}}", TestHelpers.xContentRegistry());
        Exception ex = expectThrows(ParsingException.class, () -> ParseUtils.parseAggregators(parser, 0, aggName));
        assertTrue(ex.getMessage().contains("Missing definition for aggregation [" + aggName + "]"));
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

    public void testAddUserRoleFilterWithNullUser() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        ParseUtils.addUserBackendRolesFilter(null, searchSourceBuilder);
        assertEquals("{}", searchSourceBuilder.toString());
    }

    public void testAddUserRoleFilterWithNullUserBackendRole() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        ParseUtils
            .addUserBackendRolesFilter(
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
        ParseUtils
            .addUserBackendRolesFilter(
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
        ParseUtils
            .addUserBackendRolesFilter(
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
            .randomAnomalyDetector(ImmutableList.of(index), ImmutableList.of(feature1, feature2), null, now, 1, false, null);

        long startTime = now.minus(10, ChronoUnit.DAYS).toEpochMilli();
        long endTime = now.plus(10, ChronoUnit.DAYS).toEpochMilli();
        SearchSourceBuilder searchSourceBuilder = ParseUtils
            .batchFeatureQuery(detector, null, startTime, endTime, TestHelpers.xContentRegistry());
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
                + "\":true,\"boost\":1.0}},\"aggregations\":{\"feature_aggs\":{\"composite\":{\"size\":10000,\"sources\":"
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
            .randomAnomalyDetector(ImmutableList.of(index), ImmutableList.of(TestHelpers.randomFeature(false)), null, now, 1, false, null);

        long startTime = now.minus(10, ChronoUnit.DAYS).toEpochMilli();
        long endTime = now.plus(10, ChronoUnit.DAYS).toEpochMilli();

        TimeSeriesException exception = expectThrows(
            TimeSeriesException.class,
            () -> ParseUtils.batchFeatureQuery(detector, null, startTime, endTime, TestHelpers.xContentRegistry())
        );
        assertEquals("No enabled feature configured", exception.getMessage());
    }

    public void testBatchFeatureQueryWithoutFeature() throws IOException {
        String index = randomAlphaOfLength(5);
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(ImmutableList.of(index), ImmutableList.of(), null, now, 1, false, null);

        long startTime = now.minus(10, ChronoUnit.DAYS).toEpochMilli();
        long endTime = now.plus(10, ChronoUnit.DAYS).toEpochMilli();
        TimeSeriesException exception = expectThrows(
            TimeSeriesException.class,
            () -> ParseUtils.batchFeatureQuery(detector, null, startTime, endTime, TestHelpers.xContentRegistry())
        );
        assertEquals("No enabled feature configured", exception.getMessage());
    }

    public void testListEqualsWithoutConsideringOrder() {
        assertTrue(ParseUtils.listEqualsWithoutConsideringOrder(null, null));
        assertTrue(ParseUtils.listEqualsWithoutConsideringOrder(null, ImmutableList.of()));
        assertTrue(ParseUtils.listEqualsWithoutConsideringOrder(ImmutableList.of(), null));
        assertTrue(ParseUtils.listEqualsWithoutConsideringOrder(ImmutableList.of(), ImmutableList.of()));

        assertTrue(ParseUtils.listEqualsWithoutConsideringOrder(ImmutableList.of("a"), ImmutableList.of("a")));
        assertTrue(ParseUtils.listEqualsWithoutConsideringOrder(ImmutableList.of("a", "b"), ImmutableList.of("a", "b")));
        assertTrue(ParseUtils.listEqualsWithoutConsideringOrder(ImmutableList.of("b", "a"), ImmutableList.of("a", "b")));
        assertFalse(ParseUtils.listEqualsWithoutConsideringOrder(ImmutableList.of("a"), ImmutableList.of("a", "b")));
        assertFalse(
            ParseUtils.listEqualsWithoutConsideringOrder(ImmutableList.of(randomAlphaOfLength(5)), ImmutableList.of(randomAlphaOfLength(5)))
        );
    }

    public void testGetFeatureFieldNames() throws IOException {
        Feature feature1 = TestHelpers.randomFeature("feature-name1", "field-name1", "sum", true);
        Feature feature2 = TestHelpers.randomFeature("feature-name2", "field-name2", "sum", true);
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature1, feature2), null, now);
        List<String> fieldNames = ParseUtils.getFeatureFieldNames(detector, TestHelpers.xContentRegistry());
        assertTrue(fieldNames.contains("field-name1"));
        assertTrue(fieldNames.contains("field-name2"));
    }

    public void testIsAdmin() {
        User user1 = new User(
            randomAlphaOfLength(5),
            ImmutableList.of(),
            ImmutableList.of("all_access"),
            ImmutableList.of(randomAlphaOfLength(5))
        );
        assertTrue(isAdmin(user1));
    }

    public void testIsAdminBackendRoleIsAllAccess() {
        String backendRole1 = "all_access";
        User user1 = new User(
            randomAlphaOfLength(5),
            ImmutableList.of(backendRole1),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(randomAlphaOfLength(5))
        );
        assertFalse(isAdmin(user1));
    }

    public void testIsAdminNull() {
        assertFalse(isAdmin(null));
    }
}
