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

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG;
import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_GET_USER_INFO;
import static org.opensearch.ad.constant.CommonErrorMessages.NO_PERMISSION_TO_ACCESS_DETECTOR;
import static org.opensearch.ad.constant.CommonName.DATE_HISTOGRAM;
import static org.opensearch.ad.constant.CommonName.EPOCH_MILLIS_FORMAT;
import static org.opensearch.ad.constant.CommonName.FEATURE_AGGS;
import static org.opensearch.ad.model.AnomalyDetector.QUERY_PARAM_PERIOD_END;
import static org.opensearch.ad.model.AnomalyDetector.QUERY_PARAM_PERIOD_START;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PIECE_SIZE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.search.aggregations.AggregationBuilders.dateRange;
import static org.opensearch.search.aggregations.AggregatorFactories.VALID_AGG_NAME;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.ParsingException;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BaseAggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.carrotsearch.hppc.DoubleArrayList;
import com.google.common.collect.ImmutableList;

/**
 * Parsing utility functions.
 */
public final class ParseUtils {
    private static final Logger logger = LogManager.getLogger(ParseUtils.class);

    private ParseUtils() {}

    /**
     * Parse content parser to {@link java.time.Instant}.
     *
     * @param parser json based content parser
     * @return instance of {@link java.time.Instant}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Instant toInstant(XContentParser parser) throws IOException {
        if (parser.currentToken() == null || parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (parser.currentToken().isValue()) {
            return Instant.ofEpochMilli(parser.longValue());
        }
        return null;
    }

    /**
     * Parse content parser to {@link AggregationBuilder}.
     *
     * @param parser json based content parser
     * @return instance of {@link AggregationBuilder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregationBuilder toAggregationBuilder(XContentParser parser) throws IOException {
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        return parsed.getAggregatorFactories().iterator().next();
    }

    /**
     * Parse json String into {@link XContentParser}.
     *
     * @param content         json string
     * @param contentRegistry ES named content registry
     * @return instance of {@link XContentParser}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static XContentParser parser(String content, NamedXContentRegistry contentRegistry) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(contentRegistry, LoggingDeprecationHandler.INSTANCE, content);
        parser.nextToken();
        return parser;
    }

    /**
     * parse aggregation String into {@link AggregatorFactories.Builder}.
     *
     * @param aggQuery         aggregation query string
     * @param xContentRegistry ES named content registry
     * @param aggName          aggregation name, if set, will use it to replace original aggregation name
     * @return instance of {@link AggregatorFactories.Builder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregatorFactories.Builder parseAggregators(String aggQuery, NamedXContentRegistry xContentRegistry, String aggName)
        throws IOException {
        XContentParser parser = parser(aggQuery, xContentRegistry);
        return parseAggregators(parser, aggName);
    }

    /**
     * Parse content parser to {@link AggregatorFactories.Builder}.
     *
     * @param parser  json based content parser
     * @param aggName aggregation name, if set, will use it to replace original aggregation name
     * @return instance of {@link AggregatorFactories.Builder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregatorFactories.Builder parseAggregators(XContentParser parser, String aggName) throws IOException {
        return parseAggregators(parser, 0, aggName);
    }

    /**
     * Parse content parser to {@link AggregatorFactories.Builder}.
     *
     * @param parser  json based content parser
     * @param level   aggregation level, the top level start from 0
     * @param aggName aggregation name, if set, will use it to replace original aggregation name
     * @return instance of {@link AggregatorFactories.Builder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregatorFactories.Builder parseAggregators(XContentParser parser, int level, String aggName) throws IOException {
        Matcher validAggMatcher = VALID_AGG_NAME.matcher("");
        AggregatorFactories.Builder factories = new AggregatorFactories.Builder();

        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unexpected token " + token + " in [aggs]: aggregations definitions must start with the name of the aggregation."
                );
            }
            final String aggregationName = aggName == null ? parser.currentName() : aggName;
            if (!validAggMatcher.reset(aggregationName).matches()) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Invalid aggregation name ["
                        + aggregationName
                        + "]. Aggregation names must be alpha-numeric and can only contain '_' and '-'"
                );
            }

            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Aggregation definition for ["
                        + aggregationName
                        + " starts with a ["
                        + token
                        + "], expected a ["
                        + XContentParser.Token.START_OBJECT
                        + "]."
                );
            }

            BaseAggregationBuilder aggBuilder = null;
            AggregatorFactories.Builder subFactories = null;

            Map<String, Object> metaData = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Expected ["
                            + XContentParser.Token.FIELD_NAME
                            + "] under a ["
                            + XContentParser.Token.START_OBJECT
                            + "], but got a ["
                            + token
                            + "] in ["
                            + aggregationName
                            + "]",
                        parser.getTokenLocation()
                    );
                }
                final String fieldName = parser.currentName();

                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    switch (fieldName) {
                        case "meta":
                            metaData = parser.map();
                            break;
                        case "aggregations":
                        case "aggs":
                            if (subFactories != null) {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Found two sub aggregation definitions under [" + aggregationName + "]"
                                );
                            }
                            subFactories = parseAggregators(parser, level + 1, null);
                            break;
                        default:
                            if (aggBuilder != null) {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Found two aggregation type definitions in ["
                                        + aggregationName
                                        + "]: ["
                                        + aggBuilder.getType()
                                        + "] and ["
                                        + fieldName
                                        + "]"
                                );
                            }

                            aggBuilder = parser.namedObject(BaseAggregationBuilder.class, fieldName, aggregationName);
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Expected ["
                            + XContentParser.Token.START_OBJECT
                            + "] under ["
                            + fieldName
                            + "], but got a ["
                            + token
                            + "] in ["
                            + aggregationName
                            + "]"
                    );
                }
            }

            if (aggBuilder == null) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Missing definition for aggregation [" + aggregationName + "]",
                    parser.getTokenLocation()
                );
            } else {
                if (metaData != null) {
                    aggBuilder.setMetadata(metaData);
                }

                if (subFactories != null) {
                    aggBuilder.subAggregations(subFactories);
                }

                if (aggBuilder instanceof AggregationBuilder) {
                    factories.addAggregator((AggregationBuilder) aggBuilder);
                } else {
                    factories.addPipelineAggregator((PipelineAggregationBuilder) aggBuilder);
                }
            }
        }

        return factories;
    }

    public static SearchSourceBuilder generateInternalFeatureQuery(
        AnomalyDetector detector,
        long startTime,
        long endTime,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(detector.getTimeField())
            .from(startTime)
            .to(endTime)
            .format("epoch_millis")
            .includeLower(true)
            .includeUpper(false);

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(detector.getFilterQuery());

        SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery);
        if (detector.getFeatureAttributes() != null) {
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                internalSearchSourceBuilder.aggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return internalSearchSourceBuilder;
    }

    public static SearchSourceBuilder generatePreviewQuery(
        AnomalyDetector detector,
        List<Entry<Long, Long>> ranges,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {

        DateRangeAggregationBuilder dateRangeBuilder = dateRange("date_range").field(detector.getTimeField()).format("epoch_millis");
        for (Entry<Long, Long> range : ranges) {
            dateRangeBuilder.addRange(range.getKey(), range.getValue());
        }

        if (detector.getFeatureAttributes() != null) {
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                dateRangeBuilder.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return new SearchSourceBuilder().query(detector.getFilterQuery()).size(0).aggregation(dateRangeBuilder);
    }

    public static String generateInternalFeatureQueryTemplate(AnomalyDetector detector, NamedXContentRegistry xContentRegistry)
        throws IOException {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(detector.getTimeField())
            .from("{{" + QUERY_PARAM_PERIOD_START + "}}")
            .to("{{" + QUERY_PARAM_PERIOD_END + "}}");

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(detector.getFilterQuery());

        SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery);
        if (detector.getFeatureAttributes() != null) {
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                internalSearchSourceBuilder.aggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return internalSearchSourceBuilder.toString();
    }

    public static SearchSourceBuilder generateEntityColdStartQuery(
        AnomalyDetector detector,
        List<Entry<Long, Long>> ranges,
        Entity entity,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(detector.getFilterQuery());

        for (TermQueryBuilder term : entity.getTermQueryBuilders()) {
            internalFilterQuery.filter(term);
        }

        DateRangeAggregationBuilder dateRangeBuilder = dateRange("date_range").field(detector.getTimeField()).format("epoch_millis");
        for (Entry<Long, Long> range : ranges) {
            dateRangeBuilder.addRange(range.getKey(), range.getValue());
        }

        if (detector.getFeatureAttributes() != null) {
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                dateRangeBuilder.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return new SearchSourceBuilder().query(internalFilterQuery).size(0).aggregation(dateRangeBuilder);
    }

    /**
     * Map feature data to its Id and name
     * @param currentFeature Feature data
     * @param detector Detector Config object
     * @return a list of feature data with Id and name
     */
    public static List<FeatureData> getFeatureData(double[] currentFeature, AnomalyDetector detector) {
        List<String> featureIds = detector.getEnabledFeatureIds();
        List<String> featureNames = detector.getEnabledFeatureNames();
        int featureLen = featureIds.size();
        List<FeatureData> featureData = new ArrayList<>();
        for (int i = 0; i < featureLen; i++) {
            featureData.add(new FeatureData(featureIds.get(i), featureNames.get(i), currentFeature[i]));
        }
        return featureData;
    }

    public static SearchSourceBuilder addUserBackendRolesFilter(User user, SearchSourceBuilder searchSourceBuilder) {
        if (user == null) {
            return searchSourceBuilder;
        }
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        String userFieldName = "user";
        String userBackendRoleFieldName = "user.backend_roles.keyword";
        List<String> backendRoles = user.getBackendRoles() != null ? user.getBackendRoles() : ImmutableList.of();
        // For normal case, user should have backend roles.
        TermsQueryBuilder userRolesFilterQuery = QueryBuilders.termsQuery(userBackendRoleFieldName, backendRoles);
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(userFieldName, userRolesFilterQuery, ScoreMode.None);
        boolQueryBuilder.must(nestedQueryBuilder);
        QueryBuilder query = searchSourceBuilder.query();
        if (query == null) {
            searchSourceBuilder.query(boolQueryBuilder);
        } else if (query instanceof BoolQueryBuilder) {
            ((BoolQueryBuilder) query).filter(boolQueryBuilder);
        } else {
            throw new AnomalyDetectionException("Search API does not support queries other than BoolQuery");
        }
        return searchSourceBuilder;
    }

    /**
     * Generates a user string formed by the username, backend roles, roles and requested tenants separated by '|'
     * (e.g., john||own_index,testrole|__user__, no backend role so you see two verticle line after john.).
     * This is the user string format used internally in the OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT and may be
     * parsed using User.parse(string).
     * @param client Client containing user info. A public API request will fill in the user info in the thread context.
     * @return parsed user object
     */
    public static User getUserContext(Client client) {
        String userStr = client.threadPool().getThreadContext().getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        logger.debug("Filtering result by " + userStr);
        return User.parse(userStr);
    }

    public static void resolveUserAndExecute(
        User requestedUser,
        String detectorId,
        boolean filterByEnabled,
        ActionListener listener,
        Consumer<AnomalyDetector> function,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry
    ) {
        try {
            if (requestedUser == null || detectorId == null) {
                // requestedUser == null means security is disabled or user is superadmin. In this case we don't need to
                // check if request user have access to the detector or not.
                function.accept(null);
            } else {
                getDetector(requestedUser, detectorId, listener, function, client, clusterService, xContentRegistry, filterByEnabled);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * If filterByEnabled is true, get detector and check if the user has permissions to access the detector,
     * then execute function; otherwise, get detector and execute function
     * @param requestUser user from request
     * @param detectorId detector id
     * @param listener action listener
     * @param function consumer function
     * @param client client
     * @param clusterService cluster service
     * @param xContentRegistry XContent registry
     * @param filterByBackendRole filter by backend role or not
     */
    public static void getDetector(
        User requestUser,
        String detectorId,
        ActionListener listener,
        Consumer<AnomalyDetector> function,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        boolean filterByBackendRole
    ) {
        if (clusterService.state().metadata().indices().containsKey(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {
            GetRequest request = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
            client
                .get(
                    request,
                    ActionListener
                        .wrap(
                            response -> onGetAdResponse(
                                response,
                                requestUser,
                                detectorId,
                                listener,
                                function,
                                xContentRegistry,
                                filterByBackendRole
                            ),
                            exception -> {
                                logger.error("Failed to get anomaly detector: " + detectorId, exception);
                                listener.onFailure(exception);
                            }
                        )
                );
        } else {
            listener.onFailure(new IndexNotFoundException(AnomalyDetector.ANOMALY_DETECTORS_INDEX));
        }
    }

    public static void onGetAdResponse(
        GetResponse response,
        User requestUser,
        String detectorId,
        ActionListener<GetAnomalyDetectorResponse> listener,
        Consumer<AnomalyDetector> function,
        NamedXContentRegistry xContentRegistry,
        boolean filterByBackendRole
    ) {
        if (response.isExists()) {
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser);
                User resourceUser = detector.getUser();

                if (!filterByBackendRole || checkUserPermissions(requestUser, resourceUser, detectorId)) {
                    function.accept(detector);
                } else {
                    logger.debug("User: " + requestUser.getName() + " does not have permissions to access detector: " + detectorId);
                    listener.onFailure(new AnomalyDetectionException(NO_PERMISSION_TO_ACCESS_DETECTOR + detectorId));
                }
            } catch (Exception e) {
                listener.onFailure(new AnomalyDetectionException(FAIL_TO_GET_USER_INFO + detectorId));
            }
        } else {
            listener.onFailure(new ResourceNotFoundException(detectorId, FAIL_TO_FIND_DETECTOR_MSG + detectorId));
        }
    }

    private static boolean checkUserPermissions(User requestedUser, User resourceUser, String detectorId) throws Exception {
        if (resourceUser.getBackendRoles() == null || requestedUser.getBackendRoles() == null) {
            return false;
        }
        // Check if requested user has backend role required to access the resource
        for (String backendRole : requestedUser.getBackendRoles()) {
            if (resourceUser.getBackendRoles().contains(backendRole)) {
                logger
                    .debug(
                        "User: "
                            + requestedUser.getName()
                            + " has backend role: "
                            + backendRole
                            + " permissions to access detector: "
                            + detectorId
                    );
                return true;
            }
        }
        return false;
    }

    public static boolean checkFilterByBackendRoles(User requestedUser, ActionListener listener) {
        if (requestedUser == null) {
            return false;
        }
        if (requestedUser.getBackendRoles().isEmpty()) {
            listener
                .onFailure(
                    new AnomalyDetectionException(
                        "Filter by backend roles is enabled and User " + requestedUser.getName() + " does not have backend roles configured"
                    )
                );
            return false;
        }
        return true;
    }

    /**
     * Parse max timestamp aggregation named CommonName.AGG_NAME_MAX
     * @param searchResponse Search response
     * @return max timestamp
     */
    public static Optional<Long> getLatestDataTime(SearchResponse searchResponse) {
        return Optional
            .ofNullable(searchResponse)
            .map(SearchResponse::getAggregations)
            .map(aggs -> aggs.asMap())
            .map(map -> (Max) map.get(CommonName.AGG_NAME_MAX_TIME))
            .map(agg -> (long) agg.getValue());
    }

    /**
     * Generate batch query request for feature aggregation on given date range.
     *
     * @param detector anomaly detector
     * @param entity entity
     * @param startTime start time
     * @param endTime end time
     * @param xContentRegistry content registry
     * @return search source builder
     * @throws IOException throw IO exception if fail to parse feature aggregation
     * @throws AnomalyDetectionException throw AD exception if no enabled feature
     */
    public static SearchSourceBuilder batchFeatureQuery(
        AnomalyDetector detector,
        Entity entity,
        long startTime,
        long endTime,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(detector.getTimeField())
            .from(startTime)
            .to(endTime)
            .format(EPOCH_MILLIS_FORMAT)
            .includeLower(true)
            .includeUpper(false);

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(detector.getFilterQuery());

        if (detector.isMultientityDetector() && entity != null && entity.getAttributes().size() > 0) {
            entity
                .getAttributes()
                .entrySet()
                .forEach(attr -> { internalFilterQuery.filter(new TermQueryBuilder(attr.getKey(), attr.getValue())); });
        }

        long intervalSeconds = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().getSeconds();

        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources
            .add(
                new DateHistogramValuesSourceBuilder(DATE_HISTOGRAM)
                    .field(detector.getTimeField())
                    .fixedInterval(DateHistogramInterval.seconds((int) intervalSeconds))
            );

        CompositeAggregationBuilder aggregationBuilder = new CompositeAggregationBuilder(FEATURE_AGGS, sources)
            .size(MAX_BATCH_TASK_PIECE_SIZE);

        if (detector.getEnabledFeatureIds().size() == 0) {
            throw new AnomalyDetectionException("No enabled feature configured").countedInStats(false);
        }

        for (Feature feature : detector.getFeatureAttributes()) {
            if (feature.getEnabled()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                aggregationBuilder.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(aggregationBuilder);
        searchSourceBuilder.query(internalFilterQuery);
        searchSourceBuilder.size(0);

        return searchSourceBuilder;
    }

    public static <T> boolean isNullOrEmpty(Collection<T> collection) {
        return collection == null || collection.size() == 0;
    }

    public static <S, T> boolean isNullOrEmpty(Map<S, T> map) {
        return map == null || map.size() == 0;
    }

    /**
     * Check if two lists of string equals or not without considering the order.
     * If the list is null, will consider it equals to empty list.
     *
     * @param list1 first list
     * @param list2 second list
     * @return true if two list of string equals
     */
    public static boolean listEqualsWithoutConsideringOrder(List<String> list1, List<String> list2) {
        Set<String> set1 = new HashSet<>();
        Set<String> set2 = new HashSet<>();
        if (list1 != null) {
            set1.addAll(list1);
        }
        if (list2 != null) {
            set2.addAll(list2);
        }
        return Objects.equals(set1, set2);
    }

    public static double[] parseDoubleArray(XContentParser parser) throws IOException {
        DoubleArrayList oldValList = new DoubleArrayList();
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            oldValList.add(parser.doubleValue());
        }
        return oldValList.toArray();
    }

    public static List<String> parseAggregationRequest(XContentParser parser) throws IOException {
        List<String> fieldNames = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final String field = parser.currentName();
                switch (field) {
                    case "field":
                        parser.nextToken();
                        fieldNames.add(parser.textOrNull());
                        break;
                    default:
                        parser.skipChildren();
                        break;
                }
            }
        }
        return fieldNames;
    }

    public static List<String> getFeatureFieldNames(AnomalyDetector detector, NamedXContentRegistry xContentRegistry) throws IOException {
        List<String> featureFields = new ArrayList<>();
        for (Feature feature : detector.getFeatureAttributes()) {
            featureFields.add(getFieldNamesForFeature(feature, xContentRegistry).get(0));
        }
        return featureFields;
    }

    public static List<String> getFieldNamesForFeature(Feature feature, NamedXContentRegistry xContentRegistry) throws IOException {
        ParseUtils.parseAggregators(feature.getAggregation().toString(), xContentRegistry, feature.getId());
        XContentParser parser = XContentType.JSON
            .xContent()
            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
        parser.nextToken();
        return ParseUtils.parseAggregationRequest(parser);
    }

}
