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

import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.DATE_HISTOGRAM;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.EPOCH_MILLIS_FORMAT;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.FEATURE_AGGS;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.QUERY_PARAM_PERIOD_END;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.QUERY_PARAM_PERIOD_START;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.search.aggregations.AggregationBuilders.dateRange;
import static org.opensearch.search.aggregations.AggregatorFactories.VALID_AGG_NAME;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.ParsingException;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.rest.RestStatus;
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

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AnomalyDetectorFunction;
import com.amazon.opendistroforelasticsearch.ad.transport.GetAnomalyDetectorResponse;
import com.amazon.opendistroforelasticsearch.commons.ConfigConstants;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
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
        String entityName,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {

        TermQueryBuilder term = new TermQueryBuilder(detector.getCategoryField().get(0), entityName);
        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(detector.getFilterQuery()).filter(term);

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
            throw new OpenSearchException("Search API does not support queries other than BoolQuery");
        }
        return searchSourceBuilder;
    }

    public static User getUserContext(Client client) {
        String userStr = client.threadPool().getThreadContext().getTransient(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT);
        logger.debug("Filtering result by " + userStr);
        return User.parse(userStr);
    }

    /**
     * This method checks requested user has enough permissions to have function executed.
     * Below permissions might be needed for user, depending on various use cases:
     *  - access to data source index specified in inputAnomalyDetector:
     *    skipped if inputAnomalyDetector is null. It is not null only for cases like
     *    create/update/validate/preview detector
     *  - access to existing detector specified by detectorId:
     *    needed only if (checkUserAgainstExistingDetector and filterByEnabled) is true
     *  - access to data source index of existing detector specified by detectorId:
     *    skipped if checkUserAgainstIndicesOfExistingDetector is false.
     *    In case of deleting detector, it is false.
     *
     * If security is disabled, aka requestedUser is null, then none of above permissions are required.
     * @param requestedUser user who sends such request
     * @param detectorId detector id of existing detector
     * @param filterByEnabled if filterBy is enabled or not
     * @param listener listener
     * @param function function to be executed after user permission is checked
     * @param client client
     * @param clusterService cluster Service
     * @param xContentRegistry xContentRegistry
     * @param inputAnomalyDetector detector from request; if request is for existing detector, this should be null
     * @param checkUserAgainstExistingDetector if true, then we need to check user has permission to existing detector
     *                                         if false, no need for such check
     * @param checkUserAgainstIndicesOfExistingDetector if true, we need to check if user has permission to indices of existing detector
     *                                                  if false, no need for such check
     */
    public static void resolveUserAndExecute(
        User requestedUser,
        String detectorId,
        boolean filterByEnabled,
        ActionListener listener,
        AnomalyDetectorFunction function,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        AnomalyDetector inputAnomalyDetector,
        boolean checkUserAgainstExistingDetector,
        boolean checkUserAgainstIndicesOfExistingDetector
    ) {
        if (requestedUser == null) {
            // Security is disabled or user is superadmin
            function.execute();
            return;
        }

        // Security is enabled
        try {
            boolean performUserAgainstExistingDetectorCheck = filterByEnabled && checkUserAgainstExistingDetector;
            AnomalyDetectorFunction getExistingDetectorThenExecute = () -> getDetector(
                requestedUser,
                detectorId,
                listener,
                function,
                client,
                clusterService,
                xContentRegistry,
                performUserAgainstExistingDetectorCheck,
                checkUserAgainstIndicesOfExistingDetector
            );
            if (inputAnomalyDetector != null) {
                AnomalyDetectorFunction functionForNewAD = performUserAgainstExistingDetectorCheck
                    ? getExistingDetectorThenExecute
                    : function;
                checkIndicesAndExecute(inputAnomalyDetector.getIndices(), requestedUser, functionForNewAD, client, listener);
                return;
            }
            // Get existing detector and perform required permission check against user
            getExistingDetectorThenExecute.execute();
        } catch (Exception e) {
            logger.error("Exception caught while checking user permissions", e);
            listener.onFailure(e);
        }
    }

    public static AnomalyDetectorFunction getADFunctionWithIndicesCheck(
        boolean checkUserAgainstIndices,
        List<String> indices,
        User requestedUser,
        AnomalyDetectorFunction function,
        Client client,
        ActionListener listener
    ) {
        if (!checkUserAgainstIndices) {
            return function;
        }
        return () -> checkIndicesAndExecute(indices, requestedUser, function, client, listener);
    }

    public static void checkIndicesAndExecute(
        List<String> indices,
        User requestedUser,
        AnomalyDetectorFunction function,
        Client client,
        ActionListener listener
    ) {
        if (indices == null || indices.isEmpty()) {
            function.execute();
            return;
        }
        SearchRequest searchRequest = new SearchRequest()
            .indices(indices.toArray(new String[0]))
            .source(new SearchSourceBuilder().size(1).query(QueryBuilders.matchAllQuery()));
        client.search(searchRequest, ActionListener.wrap(r -> { function.execute(); }, e -> {
            Exception exceptionThrown = e;
            if (e instanceof OpenSearchSecurityException) {
                String errorMessage = String
                    .format(Locale.ROOT, "User %s has no permission to indices: %s", requestedUser.getName(), indices);
                exceptionThrown = new OpenSearchStatusException(errorMessage, RestStatus.FORBIDDEN, e);
            }
            logger.error(exceptionThrown);
            listener.onFailure(exceptionThrown);
        }));
    }

    public static void getDetector(
        User requestUser,
        String detectorId,
        ActionListener listener,
        AnomalyDetectorFunction function,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        boolean checkUserAgainstDetector,
        boolean checkUserAgainstIndices
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
                                checkUserAgainstDetector,
                                checkUserAgainstIndices,
                                client
                            ),
                            exception -> {
                                logger.error("Failed to get anomaly detector: " + detectorId, exception);
                                listener.onFailure(exception);
                            }
                        )
                );
        } else {
            listener
                .onFailure(
                    new ResourceNotFoundException("Failed to find anomaly detector index: " + AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                );
        }
    }

    public static void onGetAdResponse(
        GetResponse response,
        User requestUser,
        String detectorId,
        ActionListener<GetAnomalyDetectorResponse> listener,
        AnomalyDetectorFunction function,
        NamedXContentRegistry xContentRegistry,
        boolean checkUserAgainstDetector,
        boolean checkUserAgainstIndices,
        Client client
    ) {
        if (response.isExists()) {
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser);
                User resourceUser = detector.getUser();
                AnomalyDetectorFunction functionWithIndicesCheck = getADFunctionWithIndicesCheck(
                    checkUserAgainstIndices,
                    detector.getIndices(),
                    requestUser,
                    function,
                    client,
                    listener
                );

                if (checkUserAgainstDetector) {
                    if (checkUserPermissions(requestUser, resourceUser, detectorId)) {
                        functionWithIndicesCheck.execute();
                    } else {
                        logger.debug("User: " + requestUser.getName() + " does not have permissions to access detector: " + detectorId);
                        listener.onFailure(new OpenSearchException("User does not have permissions to access detector: " + detectorId));
                    }
                } else {
                    functionWithIndicesCheck.execute();
                }
            } catch (Exception e) {
                listener.onFailure(new OpenSearchException("Unable to get user information from detector " + detectorId));
            }
        } else {
            listener.onFailure(new ResourceNotFoundException("Could not find detector " + detectorId));
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
                    new OpenSearchException(
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
     * @param startTime start time
     * @param endTime end time
     * @param xContentRegistry content registry
     * @return search source builder
     * @throws IOException throw IO exception if fail to parse feature aggregation
     * @throws AnomalyDetectionException throw AD exception if no enabled feature
     */
    public static SearchSourceBuilder batchFeatureQuery(
        AnomalyDetector detector,
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

        long intervalSeconds = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().getSeconds();

        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources
            .add(
                new DateHistogramValuesSourceBuilder(DATE_HISTOGRAM)
                    .field(detector.getTimeField())
                    .fixedInterval(DateHistogramInterval.seconds((int) intervalSeconds))
            );

        CompositeAggregationBuilder aggregationBuilder = new CompositeAggregationBuilder(FEATURE_AGGS, sources).size(1000);

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

}
