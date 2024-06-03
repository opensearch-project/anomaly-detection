/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.CONFIG_BUCKET_MINIMUM_SUCCESS_RATE;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.MergeableList;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;

/**
 * <p>This class executes all validation checks that are not blocking on the 'model' level.
 * This mostly involves checking if the data is generally dense enough to complete model training
 * which is based on if enough buckets in the last x intervals have at least 1 document present.</p>
 * <p>Initially different bucket aggregations are executed with with every configuration applied and with
 * different varying intervals in order to find the best interval for the data. If no interval is found with all
 * configuration applied then each configuration is tested sequentially for sparsity</p>
 */
// TODO: Add more UT and IT
public class ModelValidationActionHandler {

    protected final Config config;
    protected final ClusterService clusterService;
    protected final Logger logger = LogManager.getLogger(ModelValidationActionHandler.class);
    protected final TimeValue requestTimeout;
    protected final Client client;
    protected final SecurityClientUtil clientUtil;
    protected final NamedXContentRegistry xContentRegistry;
    protected final ActionListener<ValidateConfigResponse> listener;
    protected final Clock clock;
    protected final String validationType;
    protected final Settings settings;
    protected final User user;
    protected final AnalysisType context;
    private final HistogramAggregationHelper histogramAggHelper;
    private final IntervalCalculation intervalCalculation;
    // time range bounds to verify configured interval makes sense or not
    private LongBounds timeRangeToSearchForConfiguredInterval;
    private final LatestTimeRetriever latestTimeRetriever;

    /**
     * Constructor function.
     *
     * @param clusterService                  ClusterService
     * @param client                          OS node client that executes actions on the local node
     * @param clientUtil                      client util
     * @param listener                        OS channel used to construct bytes / builder based outputs, and send responses
     * @param config                          config instance
     * @param requestTimeout                  request time out configuration
     * @param xContentRegistry                Registry which is used for XContentParser
     * @param searchFeatureDao                Search feature DAO
     * @param validationType                  Specified type for validation
     * @param clock                           clock object to know when to timeout
     * @param settings                        Node settings
     * @param user                            User info
     * @param context                         Analysis type
     */
    public ModelValidationActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        ActionListener<ValidateConfigResponse> listener,
        Config config,
        TimeValue requestTimeout,
        NamedXContentRegistry xContentRegistry,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        Clock clock,
        Settings settings,
        User user,
        AnalysisType context
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.clientUtil = clientUtil;
        this.listener = listener;
        this.config = config;
        this.requestTimeout = requestTimeout;
        this.xContentRegistry = xContentRegistry;
        this.validationType = validationType;
        this.clock = clock;
        this.settings = settings;
        this.user = user;
        this.context = context;
        this.histogramAggHelper = new HistogramAggregationHelper(config, requestTimeout);
        this.intervalCalculation = new IntervalCalculation(config, requestTimeout, client, clientUtil, user, context, clock);
        // calculate the bounds in a lazy manner
        this.timeRangeToSearchForConfiguredInterval = null;
        this.latestTimeRetriever = new LatestTimeRetriever(config, requestTimeout, clientUtil, client, user, context, searchFeatureDao);
    }

    public void start() {
        ActionListener<Pair<Optional<Long>, Map<String, Object>>> latestTimeListener = ActionListener
            .wrap(
                latestEntityAttributes -> getSampleRangesForValidationChecks(
                    latestEntityAttributes.getLeft(),
                    config,
                    listener,
                    latestEntityAttributes.getRight()
                ),
                exception -> {
                    listener.onFailure(exception);
                    logger.error("Failed to create search request for last data point", exception);
                }
            );
        latestTimeRetriever.checkIfHC(latestTimeListener);
    }

    private void getSampleRangesForValidationChecks(
        Optional<Long> latestTime,
        Config config,
        ActionListener<ValidateConfigResponse> listener,
        Map<String, Object> topEntity
    ) {
        if (!latestTime.isPresent() || latestTime.get() <= 0) {
            listener
                .onFailure(
                    new ValidationException(
                        CommonMessages.TIME_FIELD_NOT_ENOUGH_HISTORICAL_DATA,
                        ValidationIssueType.TIMEFIELD_FIELD,
                        ValidationAspect.MODEL
                    )
                );
            return;
        }
        long timeRangeEnd = Math.min(Instant.now().toEpochMilli(), latestTime.get());
        intervalCalculation
            .findInterval(
                timeRangeEnd,
                topEntity,
                ActionListener.wrap(interval -> processIntervalRecommendation(interval, latestTime.get()), listener::onFailure)
            );
    }

    private void processIntervalRecommendation(IntervalTimeConfiguration interval, long latestTime) {
        // if interval suggestion is null that means no interval could be found with all the configurations
        // applied, our next step then is to check density just with the raw data and then add each configuration
        // one at a time to try and find root cause of low density
        if (interval == null) {
            checkRawDataSparsity(latestTime);
        } else {
            if (((IntervalTimeConfiguration) config.getInterval()).gte(interval)) {
                logger.info("Using the current interval there is enough dense data ");
                // Check if there is a window delay recommendation if everything else is successful and send exception
                if (Instant.now().toEpochMilli() - latestTime > histogramAggHelper.timeConfigToMilliSec(config.getWindowDelay())) {
                    sendWindowDelayRec(latestTime);
                    return;
                }
                // The rate of buckets with at least 1 doc with given interval is above the success rate
                listener.onResponse(null);
                return;
            }
            // return response with interval recommendation
            listener
                .onFailure(
                    new ValidationException(
                        CommonMessages.INTERVAL_REC + interval.getInterval(),
                        ValidationIssueType.DETECTION_INTERVAL,
                        ValidationAspect.MODEL,
                        interval
                    )
                );
        }
    }

    public AggregationBuilder getBucketAggregation(long latestTime) {
        IntervalTimeConfiguration interval = (IntervalTimeConfiguration) config.getInterval();
        long intervalInMinutes = IntervalTimeConfiguration.getIntervalInMinute(interval);
        if (timeRangeToSearchForConfiguredInterval == null) {
            timeRangeToSearchForConfiguredInterval = histogramAggHelper.getTimeRangeBounds(latestTime, intervalInMinutes * 60000);
        }

        return histogramAggHelper.getBucketAggregation((int) intervalInMinutes, timeRangeToSearchForConfiguredInterval);
    }

    private void checkRawDataSparsity(long latestTime) {
        AggregationBuilder aggregation = getBucketAggregation(latestTime);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().aggregation(aggregation).size(0).timeout(requestTimeout);
        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        final ActionListener<SearchResponse> searchResponseListener = ActionListener
            .wrap(response -> processRawDataResults(response, latestTime), listener::onFailure);
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                user,
                client,
                context,
                searchResponseListener
            );
    }

    public double processBucketAggregationResults(Histogram buckets, long latestTime) {
        long intervalInMillis = config.getIntervalInMilliseconds();
        return histogramAggHelper.processBucketAggregationResults(buckets, intervalInMillis, config);
    }

    private void processRawDataResults(SearchResponse response, long latestTime) {
        Histogram aggregate = null;
        try {
            aggregate = histogramAggHelper.checkBucketResultErrors(response);
        } catch (ValidationException e) {
            listener.onFailure(e);
        }

        if (aggregate == null) {
            return;
        }
        double fullBucketRate = processBucketAggregationResults(aggregate, latestTime);
        if (fullBucketRate < TimeSeriesSettings.INTERVAL_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ValidationException(CommonMessages.RAW_DATA_TOO_SPARSE, ValidationIssueType.INDICES, ValidationAspect.MODEL)
                );
        } else {
            checkDataFilterSparsity(latestTime);
        }
    }

    private void checkDataFilterSparsity(long latestTime) {
        AggregationBuilder aggregation = getBucketAggregation(latestTime);
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(config.getFilterQuery());
        SearchSourceBuilder searchSourceBuilder = histogramAggHelper.getSearchSourceBuilder(query, aggregation);
        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        final ActionListener<SearchResponse> searchResponseListener = ActionListener
            .wrap(response -> processDataFilterResults(response, latestTime), listener::onFailure);
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                user,
                client,
                context,
                searchResponseListener
            );
    }

    private void processDataFilterResults(SearchResponse response, long latestTime) {
        Histogram aggregate = null;
        try {
            aggregate = histogramAggHelper.checkBucketResultErrors(response);
        } catch (ValidationException e) {
            listener.onFailure(e);
        }

        if (aggregate == null) {
            return;
        }
        double fullBucketRate = processBucketAggregationResults(aggregate, latestTime);
        if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ValidationException(
                        CommonMessages.FILTER_QUERY_TOO_SPARSE,
                        ValidationIssueType.FILTER_QUERY,
                        ValidationAspect.MODEL
                    )
                );
            // blocks below are executed if data is dense enough with filter query applied.
            // If HCAD then category fields will be added to bucket aggregation to see if they
            // are the root cause of the issues and if not the feature queries will be checked for sparsity
        } else if (config.isHighCardinality()) {
            getTopEntityForCategoryField(latestTime);
        } else {
            try {
                checkFeatureQueryDelegate(latestTime);
            } catch (Exception ex) {
                logger.error(ex);
                listener.onFailure(ex);
            }
        }
    }

    private void getTopEntityForCategoryField(long latestTime) {
        ActionListener<Map<String, Object>> getTopEntityListener = ActionListener
            .wrap(topEntity -> checkCategoryFieldSparsity(topEntity, latestTime), exception -> {
                listener.onFailure(exception);
                logger.error("Failed to get top entity for categorical field", exception);
                return;
            });
        latestTimeRetriever.getTopEntity(getTopEntityListener);
    }

    private void checkCategoryFieldSparsity(Map<String, Object> topEntity, long latestTime) {
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(config.getFilterQuery());
        for (Map.Entry<String, Object> entry : topEntity.entrySet()) {
            query.filter(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
        }
        AggregationBuilder aggregation = getBucketAggregation(latestTime);
        SearchSourceBuilder searchSourceBuilder = histogramAggHelper.getSearchSourceBuilder(query, aggregation);
        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        final ActionListener<SearchResponse> searchResponseListener = ActionListener
            .wrap(response -> processTopEntityResults(response, latestTime), listener::onFailure);
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                user,
                client,
                context,
                searchResponseListener
            );
    }

    private void processTopEntityResults(SearchResponse response, long latestTime) {
        Histogram aggregate = null;
        try {
            aggregate = histogramAggHelper.checkBucketResultErrors(response);
        } catch (ValidationException e) {
            listener.onFailure(e);
        }

        if (aggregate == null) {
            return;
        }
        double fullBucketRate = processBucketAggregationResults(aggregate, latestTime);
        if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
            listener
                .onFailure(
                    new ValidationException(CommonMessages.CATEGORY_FIELD_TOO_SPARSE, ValidationIssueType.CATEGORY, ValidationAspect.MODEL)
                );
        } else {
            try {
                checkFeatureQueryDelegate(latestTime);
            } catch (Exception ex) {
                logger.error(ex);
                listener.onFailure(ex);
            }
        }
    }

    private void checkFeatureQueryDelegate(long latestTime) throws IOException {
        ActionListener<MergeableList<double[]>> validateFeatureQueriesListener = ActionListener.wrap(response -> {
            windowDelayRecommendation(latestTime);
        }, exception -> {
            listener
                .onFailure(new ValidationException(exception.getMessage(), ValidationIssueType.FEATURE_ATTRIBUTES, ValidationAspect.MODEL));
        });
        MultiResponsesDelegateActionListener<MergeableList<double[]>> multiFeatureQueriesResponseListener =
            new MultiResponsesDelegateActionListener<>(
                validateFeatureQueriesListener,
                config.getFeatureAttributes().size(),
                CommonMessages.FEATURE_QUERY_TOO_SPARSE,
                false
            );

        for (Feature feature : config.getFeatureAttributes()) {
            AggregationBuilder aggregation = getBucketAggregation(latestTime);
            BoolQueryBuilder query = QueryBuilders.boolQuery().filter(config.getFilterQuery());
            List<String> featureFields = ParseUtils.getFieldNamesForFeature(feature, xContentRegistry);
            for (String featureField : featureFields) {
                query.filter(QueryBuilders.existsQuery(featureField));
            }
            SearchSourceBuilder searchSourceBuilder = histogramAggHelper.getSearchSourceBuilder(query, aggregation);
            SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
            final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(response -> {
                try {
                    Histogram aggregate = histogramAggHelper.checkBucketResultErrors(response);
                    if (aggregate == null) {
                        return;
                    }
                    double fullBucketRate = processBucketAggregationResults(aggregate, latestTime);
                    if (fullBucketRate < CONFIG_BUCKET_MINIMUM_SUCCESS_RATE) {
                        multiFeatureQueriesResponseListener
                            .onFailure(
                                new ValidationException(
                                    CommonMessages.FEATURE_QUERY_TOO_SPARSE,
                                    ValidationIssueType.FEATURE_ATTRIBUTES,
                                    ValidationAspect.MODEL
                                )
                            );
                    } else {
                        multiFeatureQueriesResponseListener
                            .onResponse(new MergeableList<>(new ArrayList<>(Collections.singletonList(new double[] { fullBucketRate }))));
                    }
                } catch (ValidationException e) {
                    listener.onFailure(e);
                }

            }, e -> {
                logger.error(e);
                multiFeatureQueriesResponseListener
                    .onFailure(new OpenSearchStatusException(CommonMessages.FEATURE_QUERY_TOO_SPARSE, RestStatus.BAD_REQUEST, e));
            });
            // using the original context in listener as user roles have no permissions for internal operations like fetching a
            // checkpoint
            clientUtil
                .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                    searchRequest,
                    client::search,
                    user,
                    client,
                    context,
                    searchResponseListener
                );
        }
    }

    private void sendWindowDelayRec(long latestTimeInMillis) {
        long minutesSinceLastStamp = (long) Math.ceil((Instant.now().toEpochMilli() - latestTimeInMillis) / 60000.0);
        listener
            .onFailure(
                new ValidationException(
                    String.format(Locale.ROOT, CommonMessages.WINDOW_DELAY_REC, minutesSinceLastStamp, minutesSinceLastStamp),
                    ValidationIssueType.WINDOW_DELAY,
                    ValidationAspect.MODEL,
                    new IntervalTimeConfiguration(minutesSinceLastStamp, ChronoUnit.MINUTES)
                )
            );
    }

    private void windowDelayRecommendation(long latestTime) {
        // Check if there is a better window-delay to recommend and if one was recommended
        // then send exception and return, otherwise continue to let user know data is too sparse as explained below
        if (Instant.now().toEpochMilli() - latestTime > histogramAggHelper.timeConfigToMilliSec(config.getWindowDelay())) {
            sendWindowDelayRec(latestTime);
            return;
        }
        // This case has been reached if following conditions are met:
        // 1. no interval recommendation was found that leads to a bucket success rate of >= 0.75
        // 2. bucket success rate with the given interval and just raw data is also below 0.75.
        // 3. no single configuration during the following checks reduced the bucket success rate below 0.25
        // This means the rate with all configs applied or just raw data was below 0.75 but the rate when checking each configuration at
        // a time was always above 0.25 meaning the best suggestion is to simply ingest more data or change interval since
        // we have no more insight regarding the root cause of the lower density.
        listener
            .onFailure(new ValidationException(CommonMessages.RAW_DATA_TOO_SPARSE, ValidationIssueType.INDICES, ValidationAspect.MODEL));
    }

}
