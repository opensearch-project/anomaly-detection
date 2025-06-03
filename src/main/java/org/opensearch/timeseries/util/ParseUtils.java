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

package org.opensearch.timeseries.util;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.search.aggregations.AggregationBuilders.dateRange;
import static org.opensearch.search.aggregations.AggregatorFactories.VALID_AGG_NAME;
import static org.opensearch.security.spi.resources.FeatureConfigConstants.OPENSEARCH_RESOURCE_SHARING_ENABLED;
import static org.opensearch.security.spi.resources.FeatureConfigConstants.OPENSEARCH_RESOURCE_SHARING_ENABLED_DEFAULT;
import static org.opensearch.timeseries.constant.CommonMessages.FAIL_TO_FIND_CONFIG_MSG;
import static org.opensearch.timeseries.settings.TimeSeriesSettings.MAX_BATCH_TASK_PIECE_SIZE;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
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
import org.opensearch.security.spi.resources.client.ResourceSharingClient;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.resources.ResourceSharingClientAccessor;
import org.opensearch.transport.client.Client;

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
        Config config,
        long startTime,
        long endTime,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(config.getTimeField())
            .from(startTime)
            .to(endTime)
            .format("epoch_millis")
            .includeLower(true)
            .includeUpper(false);

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(config.getFilterQuery());

        SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery);
        if (config.getFeatureAttributes() != null) {
            for (Feature feature : config.getFeatureAttributes()) {
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

    public static SearchSourceBuilder generateRangeQuery(
        Config config,
        List<Entry<Long, Long>> ranges,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {

        DateRangeAggregationBuilder dateRangeBuilder = dateRange("date_range").field(config.getTimeField()).format("epoch_millis");
        for (Entry<Long, Long> range : ranges) {
            dateRangeBuilder.addRange(range.getKey(), range.getValue());
        }

        if (config.getFeatureAttributes() != null) {
            for (Feature feature : config.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                dateRangeBuilder.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return new SearchSourceBuilder().query(config.getFilterQuery()).size(0).aggregation(dateRangeBuilder);
    }

    public static SearchSourceBuilder generateColdStartQuery(
        Config config,
        List<Entry<Long, Long>> ranges,
        Optional<Entity> entity,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(config.getFilterQuery());

        if (entity.isPresent()) {
            for (TermQueryBuilder term : entity.get().getTermQueryForCustomerIndex()) {
                internalFilterQuery.filter(term);
            }
        }

        DateRangeAggregationBuilder dateRangeBuilder = dateRange("date_range").field(config.getTimeField()).format("epoch_millis");
        for (Entry<Long, Long> range : ranges) {
            dateRangeBuilder.addRange(range.getKey(), range.getValue());
        }

        if (config.getFeatureAttributes() != null) {
            for (Feature feature : config.getFeatureAttributes()) {
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

    public static SearchSourceBuilder generateColdStartQueryForSingleFeature(
        Config config,
        List<Entry<Long, Long>> ranges,
        Optional<Entity> entity,
        NamedXContentRegistry xContentRegistry,
        int featureIndex
    ) throws IOException {

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(config.getFilterQuery());

        if (entity.isPresent()) {
            for (TermQueryBuilder term : entity.get().getTermQueryForCustomerIndex()) {
                internalFilterQuery.filter(term);
            }
        }

        DateRangeAggregationBuilder dateRangeBuilder = dateRange("date_range").field(config.getTimeField()).format("epoch_millis");
        for (Entry<Long, Long> range : ranges) {
            dateRangeBuilder.addRange(range.getKey(), range.getValue());
        }

        if (config.getFeatureAttributes() != null) {
            Feature feature = config.getFeatureAttributes().get(featureIndex);
            AggregatorFactories.Builder internalAgg = parseAggregators(
                feature.getAggregation().toString(),
                xContentRegistry,
                feature.getId()
            );
            dateRangeBuilder.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
        } else {
            throw new IllegalArgumentException("empty feature");
        }

        return new SearchSourceBuilder().query(internalFilterQuery).size(0).aggregation(dateRangeBuilder);
    }

    /**
     * Map feature data to its Id and name
     * @param currentFeature Feature data
     * @param config Config object
     * @return a list of feature data with Id and name
     */
    public static List<FeatureData> getFeatureData(double[] currentFeature, Config config) {
        List<String> featureIds = config.getEnabledFeatureIds();
        List<String> featureNames = config.getEnabledFeatureNames();
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
            // e.g., wild card query
            throw new TimeSeriesException("Search API does not support queries other than BoolQuery");
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

    /**
     * run the given function based on given user
     * @param <GetConfigResponseType> Config response type. Can be either GetAnomalyDetectorResponse or GetForecasterResponse
     * @param requestedUser requested user
     * @param configId config Id
     * @param filterByEnabled filter by backend is enabled
     * @param listener listener. We didn't provide the generic type of listener and therefore can return anything using the listener.
     * @param function Function to execute
     * @param client Client to OS.
     * @param clusterService Cluster service of OS.
     * @param xContentRegistry Used to deserialize the get config response.
     * @param configTypeClass the class of the ConfigType, used by the ConfigFactory to parse the correct type of Config
     */
    public static <ConfigType extends Config, GetConfigResponseType extends ActionResponse> void resolveUserAndExecute(
        User requestedUser,
        String configId,
        boolean filterByEnabled,
        ActionListener listener,
        Consumer<ConfigType> function,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Class<ConfigType> configTypeClass
    ) {
        try {
            if (requestedUser == null || configId == null) {
                // requestedUser == null means security is disabled or user is superadmin. In this case we don't need to
                // check if request user have access to the detector or not.
                function.accept(null);
            } else {
                getConfig(
                    requestedUser,
                    configId,
                    listener,
                    function,
                    client,
                    clusterService,
                    xContentRegistry,
                    filterByEnabled,
                    configTypeClass
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * If filterByEnabled is true, get config and check if the user has permissions to access the config,
     * then execute function; otherwise, get config and execute function
     * @param requestUser user from request
     * @param configId config id
     * @param listener action listener
     * @param function consumer function
     * @param client client
     * @param clusterService cluster service
     * @param xContentRegistry XContent registry
     * @param filterByBackendRole filter by backend role or not
     * @param configTypeClass the class of the ConfigType, used by the ConfigFactory to parse the correct type of Config
     */
    public static <ConfigType extends Config, GetConfigResponseType extends ActionResponse> void getConfig(
        User requestUser,
        String configId,
        ActionListener<GetConfigResponseType> listener,
        Consumer<ConfigType> function,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        boolean filterByBackendRole,
        Class<ConfigType> configTypeClass
    ) {
        if (clusterService.state().metadata().indices().containsKey(CommonName.CONFIG_INDEX)) {
            GetRequest request = new GetRequest(CommonName.CONFIG_INDEX).id(configId);
            client
                .get(
                    request,
                    ActionListener
                        .wrap(
                            response -> onGetConfigResponse(
                                response,
                                requestUser,
                                configId,
                                listener,
                                function,
                                xContentRegistry,
                                filterByBackendRole,
                                configTypeClass
                            ),
                            exception -> {
                                logger.error("Failed to get config: " + configId, exception);
                                listener.onFailure(exception);
                            }
                        )
                );
        } else {
            listener.onFailure(new IndexNotFoundException(CommonName.CONFIG_INDEX));
        }
    }

    /**
     * Processes a GetResponse by leveraging the factory method Config.parseConfig to
     * appropriately parse the specified type of Config. The execution of the provided
     * consumer function depends on the state of the 'filterByBackendRole' setting:
     *
     * - If 'filterByBackendRole' is disabled, the consumer function will be invoked
     *   irrespective of the user's permissions.
     *
     * - If 'filterByBackendRole' is enabled, the consumer function will only be invoked
     *   provided the user holds the requisite permissions.
     *
     * @param <ConfigType> The type of Config to be processed in this method, which extends from the Config base type.
     * @param <GetConfigResponseType> The type of ActionResponse to be used, which extends from the ActionResponse base type.
     * @param response The GetResponse from the getConfig request. This contains the information about the config that is to be processed.
     * @param requestUser The User from the request. This user's permissions will be checked to ensure they have access to the config.
     * @param configId The ID of the config. This is used for logging and error messages.
     * @param listener The ActionListener to call if an error occurs. Any errors that occur during the processing of the config will be passed to this listener.
     * @param function The Consumer function to apply to the ConfigType. If the user has permission to access the config, this function will be applied.
     * @param xContentRegistry The XContentRegistry used to create the XContentParser. This is used to parse the response into a ConfigType.
     * @param filterByBackendRole A boolean indicating whether to filter by backend role. If true, the user's backend roles will be checked to ensure they have access to the config.
     * @param configTypeClass The class of the ConfigType, used by the ConfigFactory to parse the correct type of Config.
     */
    public static <ConfigType extends Config, GetConfigResponseType extends ActionResponse> void onGetConfigResponse(
        GetResponse response,
        User requestUser,
        String configId,
        ActionListener<GetConfigResponseType> listener,
        Consumer<ConfigType> function,
        NamedXContentRegistry xContentRegistry,
        boolean filterByBackendRole,
        Class<ConfigType> configTypeClass
    ) {
        if (response.isExists()) {
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                @SuppressWarnings("unchecked")
                ConfigType config = (ConfigType) Config.parseConfig(configTypeClass, parser);

                User resourceUser = config.getUser();

                if (!filterByBackendRole || checkUserPermissions(requestUser, resourceUser, configId) || isAdmin(requestUser)) {
                    function.accept(config);
                } else {
                    logger.debug("User: " + requestUser.getName() + " does not have permissions to access config: " + configId);
                    listener
                        .onFailure(
                            new OpenSearchStatusException(CommonMessages.NO_PERMISSION_TO_ACCESS_CONFIG + configId, RestStatus.FORBIDDEN)
                        );
                }

            } catch (Exception e) {
                logger.error("Fail to parse user out of config", e);
                listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_GET_USER_INFO + configId, RestStatus.BAD_REQUEST));
            }
        } else {
            listener.onFailure(new OpenSearchStatusException(FAIL_TO_FIND_CONFIG_MSG + configId, RestStatus.NOT_FOUND));
        }
    }

    /**
     * 'all_access' role users are treated as admins.
     * @param user of the current role
     * @return boolean if the role is admin
     */
    public static boolean isAdmin(User user) {
        if (user == null) {
            return false;
        }
        return user.getRoles().contains("all_access");
    }

    private static boolean checkUserPermissions(User requestedUser, User resourceUser, String configId) throws Exception {
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
                            + " permissions to access config: "
                            + configId
                    );
                return true;
            }
        }
        return false;
    }

    public static String checkFilterByBackendRoles(User requestedUser) {
        if (requestedUser == null) {
            return "Filter by backend roles is enabled and User is null";
        }
        if (requestedUser.getBackendRoles().isEmpty()) {
            return String
                .format(
                    Locale.ROOT,
                    "Filter by backend roles is enabled and User %s does not have backend roles configured",
                    requestedUser.getName()
                );
        }
        return null;
    }

    /**
     * Checks whether to utilize new ResourAuthz
     * @param settings which is to be checked for the config
     * @return true if the resource-sharing feature and filter-by is enabled, false otherwise.
     */
    public static boolean shouldUseResourceAuthz(Settings settings) {
        boolean filterByEnabled = AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES.get(settings);
        boolean isResourceSharingFeatureEnabled = settings
            .getAsBoolean(OPENSEARCH_RESOURCE_SHARING_ENABLED, OPENSEARCH_RESOURCE_SHARING_ENABLED_DEFAULT);
        return isResourceSharingFeatureEnabled && filterByEnabled;
    }

    /**
     * Verifies whether the user has permission to access the resource.
     * @param requestedUser user from request
     * @param configIndex index where config is stored
     * @param configId config id
     * @param shouldEvaluateWithNewAuthz true only if resource-sharing feature and filter_by_backend role, both are enabled.
     * @param listener action listener
     * @param onSuccess consumer function to execute if user has permission
     * @param successArgs arguments to pass to the consumer function
     * @param fallbackOn501 consumer function to execute if user does not have permission
     * @param fallbackArgs arguments to pass to the consumer function
     */
    public static void verifyResourceAccessAndProcessRequest(
        User requestedUser,
        String configIndex,
        String configId,
        boolean shouldEvaluateWithNewAuthz,
        ActionListener<? extends ActionResponse> listener,
        Consumer<Object[]> onSuccess,
        Object[] successArgs,
        Consumer<Object[]> fallbackOn501,
        Object[] fallbackArgs
    ) {
        // TODO: Remove this feature flag check once feature is GA, as it will be enabled by default
        // detectorId will be null when this is a create request and so we don't need resource authz check
        if (shouldEvaluateWithNewAuthz && !Strings.isNullOrEmpty(configId)) {
            ResourceSharingClient resourceSharingClient = ResourceSharingClientAccessor.getInstance().getResourceSharingClient();
            resourceSharingClient.verifyAccess(configId, configIndex, ActionListener.wrap(isAuthorized -> {
                if (!isAuthorized) {
                    listener
                        .onFailure(
                            new OpenSearchStatusException(
                                "User " + requestedUser.getName() + " is not authorized to access config: " + configId,
                                RestStatus.FORBIDDEN
                            )
                        );
                    return;
                }
                onSuccess.accept(successArgs);
            }, listener::onFailure));
        } else {
            fallbackOn501.accept(fallbackArgs);
        }
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
            // Explanation:
            // - `agg != null`: Ensures the aggregation object is not null.
            // - `agg.getValue() > 0`: Checks that the value is positive, as the aggregation might return a default negative value
            // like -9223372036854775808 if the underlying data is null or invalid. This check prevents converting such invalid
            // values to a timestamp, which would not make sense in the context of time-based data.
            .filter(agg -> agg != null && agg.getValue() > 0) // Check if the value is valid and positive.
            .map(agg -> (long) agg.getValue()); // Cast to long if valid
    }

    /**
     * Generate batch query request for feature aggregation on given date range.
     *
     * @param config config accessor
     * @param entity entity
     * @param startTime start time
     * @param endTime end time
     * @param xContentRegistry content registry
     * @return search source builder
     * @throws IOException throw IO exception if fail to parse feature aggregation
     * @throws TimeSeriesException throw AD exception if no enabled feature
     */
    public static SearchSourceBuilder batchFeatureQuery(
        Config config,
        Entity entity,
        long startTime,
        long endTime,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(config.getTimeField())
            .from(startTime)
            .to(endTime)
            .format(CommonName.EPOCH_MILLIS_FORMAT)
            .includeLower(true)
            .includeUpper(false);

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(config.getFilterQuery());

        if (config.isHighCardinality() && entity != null && entity.getAttributes().size() > 0) {
            entity
                .getAttributes()
                .entrySet()
                .forEach(attr -> { internalFilterQuery.filter(new TermQueryBuilder(attr.getKey(), attr.getValue())); });
        }

        long intervalSeconds = ((IntervalTimeConfiguration) config.getInterval()).toDuration().getSeconds();

        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources
            .add(
                new DateHistogramValuesSourceBuilder(CommonName.DATE_HISTOGRAM)
                    .field(config.getTimeField())
                    .fixedInterval(DateHistogramInterval.seconds((int) intervalSeconds))
            );

        CompositeAggregationBuilder aggregationBuilder = new CompositeAggregationBuilder(CommonName.FEATURE_AGGS, sources)
            .size(MAX_BATCH_TASK_PIECE_SIZE);

        if (config.getEnabledFeatureIds().size() == 0) {
            throw new TimeSeriesException("No enabled feature configured").countedInStats(false);
        }

        for (Feature feature : config.getFeatureAttributes()) {
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
        final List<Double> oldValList = new ArrayList<>();
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            oldValList.add(parser.doubleValue());
        }
        return oldValList.stream().mapToDouble(Double::doubleValue).toArray();
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

    public static List<String> getFeatureFieldNames(Config config, NamedXContentRegistry xContentRegistry) throws IOException {
        List<String> featureFields = new ArrayList<>();
        for (Feature feature : config.getFeatureAttributes()) {
            featureFields.add(getFieldNamesForFeature(feature, xContentRegistry).get(0));
        }
        return featureFields;
    }

    /**
     * This works only when the query is a simple aggregation. It won't work for aggregation with a filter.
     * @param feature Feature in AD
     * @param xContentRegistry used to parse xcontent
     * @return parsed field name
     * @throws IOException when parsing fails
     */
    public static List<String> getFieldNamesForFeature(Feature feature, NamedXContentRegistry xContentRegistry) throws IOException {
        ParseUtils.parseAggregators(feature.getAggregation().toString(), xContentRegistry, feature.getId());
        XContentParser parser = XContentType.JSON
            .xContent()
            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, feature.getAggregation().toString());
        parser.nextToken();
        return ParseUtils.parseAggregationRequest(parser);
    }

}
