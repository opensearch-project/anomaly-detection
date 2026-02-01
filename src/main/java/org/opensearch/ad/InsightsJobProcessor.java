/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.correlation.Anomaly;
import org.opensearch.ad.correlation.AnomalyCorrelation;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.InsightsGenerator;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorMetadata;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.InjectSecurity;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.JobProcessor;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.ResultRequest;
import org.opensearch.timeseries.util.PluginClient;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.transport.client.Client;

/**
 * InsightsJobProcessor processes the global Insights job which analyzes all detectors.
 * 
 * Unlike regular AD jobs that run detection on a single detector, the Insights job:
 * 1. Runs at a configured frequency (e.g., every 5 minutes, every 24 hours)
 * 2. Queries detectors created by LLM
 * 3. Retrieves anomaly results from the past execution interval
 * 4. Runs anomaly correlation to group related anomalies
 * 5. Generates insights and writes them to the insights-results index
 * 
 */
public class InsightsJobProcessor extends
    JobProcessor<ADIndex, ADIndexManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult, ADProfileAction, ExecuteADResultResponseRecorder, ADIndexJobActionHandler> {

    private static final Logger log = LogManager.getLogger(InsightsJobProcessor.class);
    private static final int LOG_PREVIEW_LIMIT = 2000;
    private static final String RESULT_INDEX_AGG_NAME = "result_index";

    private static volatile InsightsJobProcessor INSTANCE;
    private NamedXContentRegistry xContentRegistry;
    private Settings settings;

    private Client localClient;
    private ThreadPool localThreadPool;
    private String localThreadPoolName;
    private PluginClient pluginClient;

    public static InsightsJobProcessor getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (InsightsJobProcessor.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new InsightsJobProcessor();
            return INSTANCE;
        }
    }

    private InsightsJobProcessor() {
        super(AnalysisType.AD, TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME, AnomalyResultAction.INSTANCE);
        this.localThreadPoolName = TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME;
    }

    public void registerSettings(Settings settings) {
        super.registerSettings(settings, AnomalyDetectorSettings.AD_MAX_RETRY_FOR_END_RUN_EXCEPTION);
        this.settings = settings;
    }

    public void setXContentRegistry(NamedXContentRegistry xContentRegistry) {
        this.xContentRegistry = xContentRegistry;
    }

    public void setPluginClient(PluginClient pluginClient) {
        this.pluginClient = pluginClient;
    }

    @Override
    public void setClient(Client client) {
        super.setClient(client);
        this.localClient = client;
    }

    @Override
    public void setThreadPool(ThreadPool threadPool) {
        super.setThreadPool(threadPool);
        this.localThreadPool = threadPool;
    }

    /**
     * Process the Insights job.
     * Overrides the default process method to implement Insights-specific logic.
     */
    @Override
    public void process(Job jobParameter, JobExecutionContext context) {
        String jobName = jobParameter.getName();
        log.info("Starting Insights job execution: {}", jobName);

        // Calculate analysis time window based on job schedule interval
        Instant executionEndTime = Instant.now();
        Instant executionStartTime;

        // Extract interval from schedule
        if (jobParameter.getSchedule() instanceof IntervalSchedule) {
            IntervalSchedule intervalSchedule = (IntervalSchedule) jobParameter.getSchedule();
            long intervalAmount = intervalSchedule.getInterval();
            ChronoUnit intervalUnit = intervalSchedule.getUnit();
            executionStartTime = executionEndTime.minus(intervalAmount, intervalUnit);
            log
                .info(
                    "Insights job analyzing data from {} to {} (interval: {} {})",
                    executionStartTime,
                    executionEndTime,
                    intervalAmount,
                    intervalUnit
                );
        } else {
            // Fallback to 24 hours if schedule type is unexpected
            log.warn("Unexpected schedule type for Insights job: {}, defaulting to 24 hours", jobParameter.getSchedule().getClass());
            executionStartTime = executionEndTime.minus(24, ChronoUnit.HOURS);
            log.info("Insights job analyzing data from {} to {} (default 24h window)", executionStartTime, executionEndTime);
        }

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            try {
                if (jobParameter.getLockDurationSeconds() != null) {
                    // Acquire lock to prevent concurrent execution
                    lockService
                        .acquireLock(
                            jobParameter,
                            context,
                            ActionListener
                                .wrap(
                                    lock -> runInsightsJob(jobParameter, lockService, lock, executionStartTime, executionEndTime),
                                    exception -> {
                                        log
                                            .error(
                                                new ParameterizedMessage("Failed to acquire lock for Insights job {}", jobName),
                                                exception
                                            );
                                        // No lock to release on acquisition failure
                                    }
                                )
                        );
                } else {
                    log.warn("No lock duration configured for Insights job: {}", jobName);
                }
            } catch (Exception e) {
                log.error(new ParameterizedMessage("Error executing Insights job {}", jobName), e);
            }
        };

        localThreadPool.executor(localThreadPoolName).submit(runnable);
    }

    /**
     * Run the Insights job once, 5 minutes after the job is enabled
     * to pick up anomalies generated by auto created detectors.
     * 
     * @param jobParameter The insights job
     */
    public void runOnce(Job jobParameter) {
        String jobName = jobParameter.getName();
        log.info("Starting one-time Insights job execution (manual trigger) for {}", jobName);

        Instant executionEndTime = Instant.now();
        Instant executionStartTime;

        if (jobParameter.getSchedule() instanceof IntervalSchedule) {
            IntervalSchedule intervalSchedule = (IntervalSchedule) jobParameter.getSchedule();
            long intervalAmount = intervalSchedule.getInterval();
            ChronoUnit intervalUnit = intervalSchedule.getUnit();
            executionStartTime = executionEndTime.minus(intervalAmount, intervalUnit);
            log
                .info(
                    "One-time Insights job analyzing data from {} to {} (interval: {} {})",
                    executionStartTime,
                    executionEndTime,
                    intervalAmount,
                    intervalUnit
                );
        } else {
            log
                .warn(
                    "Unexpected schedule type for Insights job {} in one-time run: {}, defaulting to 24 hours",
                    jobName,
                    jobParameter.getSchedule() != null ? jobParameter.getSchedule().getClass() : "null"
                );
            executionStartTime = executionEndTime.minus(24, ChronoUnit.HOURS);
            log.info("One-time Insights job analyzing data from {} to {} (default 24h window)", executionStartTime, executionEndTime);
        }

        ActionListener<List<AnomalyResult>> anomaliesListener = ActionListener.wrap(anomalies -> {
            if (anomalies == null || anomalies.isEmpty()) {
                log.info("No anomalies found in one-time run, skipping correlation");
                return;
            }

            ActionListener<Void> completion = ActionListener.wrap(r -> {}, e -> {
                log.error(new ParameterizedMessage("One-time Insights job {} failed", jobName), e);
            });

            fetchDetectorMetadataAndProceed(anomalies, jobParameter, executionStartTime, executionEndTime, completion);
        }, e -> { log.error(new ParameterizedMessage("Failed to query anomaly results for one-time Insights job {}", jobName), e); });

        queryCustomResultIndex(jobParameter, executionStartTime, executionEndTime, anomaliesListener);
    }

    /**
     * Release lock for job.
     */
    private void releaseLock(Job jobParameter, LockService lockService, LockModel lock) {
        if (lockService == null || lock == null) {
            log.debug("No lock to release for Insights job {}", jobParameter.getName());
            return;
        }
        lockService
            .release(
                lock,
                ActionListener.wrap(released -> { log.info("Released lock for Insights job {}", jobParameter.getName()); }, exception -> {
                    log.error(new ParameterizedMessage("Failed to release lock for Insights job {}", jobParameter.getName()), exception);
                })
            );
    }

    /**
     * Execute the main Insights job logic.
     * 
     * @param jobParameter The insights job
     * @param lockService Lock service for releasing lock
     * @param lock The acquired lock
     * @param executionStartTime Start of analysis window
     * @param executionEndTime End of analysis window
     */
    private void runInsightsJob(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime
    ) {
        String jobName = jobParameter.getName();
        if (lock == null) {
            log.warn("Can't run Insights job due to null lock for {}", jobName);
            return;
        }

        log.info("Running Insights job for time window: {} to {}", executionStartTime, executionEndTime);

        // Guarded listener that ensures the lock is released exactly once regardless of success/failure path
        ActionListener<Void> lockReleasing = guardedLockReleasingListener(jobParameter, lockService, lock);

        ActionListener<List<AnomalyResult>> anomaliesListener = ActionListener.wrap(anomalies -> {
            if (anomalies == null || anomalies.isEmpty()) {
                log.info("No anomalies found in time window, skipping correlation");
                lockReleasing.onResponse(null);
                return;
            }
            fetchDetectorMetadataAndProceed(anomalies, jobParameter, executionStartTime, executionEndTime, lockReleasing);
        }, lockReleasing::onFailure);

        queryCustomResultIndex(jobParameter, executionStartTime, executionEndTime, anomaliesListener);
    }

    /**
     * a lock-releasing listener that guarantees the lock is released at most once.
     */
    private ActionListener<Void> guardedLockReleasingListener(Job jobParameter, LockService lockService, LockModel lock) {
        AtomicBoolean done = new AtomicBoolean(false);

        return ActionListener.wrap(r -> {
            if (done.compareAndSet(false, true)) {
                releaseLock(jobParameter, lockService, lock);
            } else {
                log.warn("Lock already released for Insights job {}", jobParameter.getName());
            }
        }, e -> {
            if (done.compareAndSet(false, true)) {
                log.error(new ParameterizedMessage("Insights job {} failed", jobParameter.getName()), e);
                releaseLock(jobParameter, lockService, lock);
            } else {
                log.warn("Lock already released for Insights job {} (got extra failure)", jobParameter.getName(), e);
            }
        });
    }

    /**
     * Query all anomalies from custom result indices for the given time window.
     *
     * @param jobParameter The insights job
     * @param executionStartTime Start of analysis window
     * @param executionEndTime End of analysis window
     */
    private void queryCustomResultIndex(
        Job jobParameter,
        Instant executionStartTime,
        Instant executionEndTime,
        ActionListener<List<AnomalyResult>> listener
    ) {
        log.info("Querying anomaly results from {} to {}", executionStartTime, executionEndTime);

        resolveCustomResultIndexPatterns(jobParameter, ActionListener.wrap(indexPatterns -> {
            if (indexPatterns == null || indexPatterns.isEmpty()) {
                log.info("No custom result indices found; skipping anomaly query");
                listener.onResponse(new ArrayList<>());
                return;
            }

            List<AnomalyResult> allAnomalies = new ArrayList<>();

            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

            boolQuery
                .filter(
                    QueryBuilders
                        .rangeQuery("execution_start_time")
                        .gte(executionStartTime.toEpochMilli())
                        .lte(executionEndTime.toEpochMilli())
                        .format("epoch_millis")
                );

            boolQuery.filter(QueryBuilders.rangeQuery("anomaly_grade").gt(0));

            final int pageSize = 10000;
            final TimeValue scrollKeepAlive = TimeValue.timeValueMinutes(5);

            SearchSourceBuilder baseSource = new SearchSourceBuilder()
                .query(boolQuery)
                .size(pageSize)
                .fetchSource(
                    new String[] { "detector_id", "model_id", "entity", "data_start_time", "data_end_time", "anomaly_grade" },
                    null
                )
                .sort("_doc", SortOrder.ASC);

            SearchRequest searchRequest = new SearchRequest(indexPatterns.toArray(new String[0]))
                .source(baseSource)
                .scroll(scrollKeepAlive);
            logAnomalyResultsQueryPreview(searchRequest, baseSource, pageSize, scrollKeepAlive, executionStartTime, executionEndTime);

            User userInfo = SecurityUtil.getUserFromJob(jobParameter, settings);
            String user = userInfo.getName();
            List<String> roles = userInfo.getRoles();
            InjectSecurity injectSecurity = new InjectSecurity(
                jobParameter.getName(),
                settings,
                localClient.threadPool().getThreadContext()
            );
            try {
                // anomaly results are stored in custom result indices; use job user credentials to search
                injectSecurity.inject(user, roles);
                localClient.search(searchRequest, ActionListener.runBefore(ActionListener.wrap(searchResponse -> {
                    String scrollId = searchResponse.getScrollId();
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    try {
                        parseAnomalyHits(hits, allAnomalies);
                    } catch (Exception parseException) {
                        // Best effort cleanup: if parsing unexpectedly fails, clear the scroll id we currently hold.
                        clearScroll(jobParameter, scrollId);
                        listener.onFailure(parseException);
                        return;
                    }

                    if (hits.length == 0 || hits.length < pageSize) {
                        log
                            .info(
                                "Successfully parsed {} anomalies in time window {} to {}",
                                allAnomalies.size(),
                                executionStartTime,
                                executionEndTime
                            );
                        clearScroll(jobParameter, scrollId);
                        listener.onResponse(allAnomalies);
                        return;
                    }

                    fetchScrolledAnomalies(
                        jobParameter,
                        scrollId,
                        scrollKeepAlive,
                        pageSize,
                        allAnomalies,
                        executionStartTime,
                        executionEndTime,
                        listener
                    );
                }, e -> {
                    if (e.getMessage() != null
                        && (e.getMessage().contains("no such index") || e.getMessage().contains("index_not_found"))) {
                        log.info("Anomaly results index does not exist yet (no anomalies recorded)");
                    } else {
                        log.error("Failed to query anomaly results", e);
                    }
                    listener.onFailure(e);
                }), injectSecurity::close));
            } catch (Exception e) {
                injectSecurity.close();
                log.error("Failed to query anomaly results for Insights job {}", jobParameter.getName(), e);
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    /**
     * Resolve custom result index patterns (alias*) used by detectors.
     * We rely on the detector config's `result_index` field (stored as an alias since 2.15) and query all history indices via wildcard.
     */
    private void resolveCustomResultIndexPatterns(Job jobParameter, ActionListener<List<String>> listener) {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .aggregation(new TermsAggregationBuilder(RESULT_INDEX_AGG_NAME).field(Config.RESULT_INDEX_FIELD).size(10000))
            .size(0);

        SearchRequest request = new SearchRequest(ADCommonName.CONFIG_INDEX).source(source);
        ThreadContext threadContext = localClient.threadPool().getThreadContext();
        // detector configs are stored in system index; use stashed context
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            Client systemClient = pluginClient != null ? pluginClient : localClient;
            try {
                systemClient.search(request, ActionListener.wrap(response -> {
                    List<String> patterns = new ArrayList<>();
                    Aggregations aggregations = response.getAggregations();
                    if (aggregations == null) {
                        listener.onResponse(patterns);
                        return;
                    }

                    // Iterate instead of using Aggregations#get(...) (final method in core; harder to mock in unit tests).
                    StringTerms resultIndicesAgg = null;
                    for (Aggregation agg : aggregations) {
                        if (agg instanceof StringTerms && RESULT_INDEX_AGG_NAME.equals(agg.getName())) {
                            resultIndicesAgg = (StringTerms) agg;
                            break;
                        }
                    }
                    if (resultIndicesAgg == null || resultIndicesAgg.getBuckets() == null) {
                        listener.onResponse(patterns);
                        return;
                    }

                    for (StringTerms.Bucket bucket : resultIndicesAgg.getBuckets()) {
                        String alias = bucket.getKeyAsString();
                        if (alias == null || alias.isEmpty()) {
                            continue;
                        }
                        patterns.add(IndexManagement.getAllCustomResultIndexPattern(alias));
                    }

                    listener.onResponse(patterns);
                }, listener::onFailure));
            } catch (IllegalStateException e) {
                // PluginClient present but subject not initialized; fall back to stashed regular client.
                localClient.search(request, ActionListener.wrap(response -> {
                    List<String> patterns = new ArrayList<>();
                    Aggregations aggregations = response.getAggregations();
                    if (aggregations == null) {
                        listener.onResponse(patterns);
                        return;
                    }
                    StringTerms resultIndicesAgg = null;
                    for (Aggregation agg : aggregations) {
                        if (agg instanceof StringTerms && RESULT_INDEX_AGG_NAME.equals(agg.getName())) {
                            resultIndicesAgg = (StringTerms) agg;
                            break;
                        }
                    }
                    if (resultIndicesAgg == null || resultIndicesAgg.getBuckets() == null) {
                        listener.onResponse(patterns);
                        return;
                    }
                    for (StringTerms.Bucket bucket : resultIndicesAgg.getBuckets()) {
                        String alias = bucket.getKeyAsString();
                        if (alias == null || alias.isEmpty()) {
                            continue;
                        }
                        patterns.add(IndexManagement.getAllCustomResultIndexPattern(alias));
                    }
                    listener.onResponse(patterns);
                }, listener::onFailure));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Fetch anomalies with pagination
     */
    private void fetchScrolledAnomalies(
        Job jobParameter,
        String scrollId,
        TimeValue scrollKeepAlive,
        int pageSize,
        List<AnomalyResult> allAnomalies,
        Instant executionStartTime,
        Instant executionEndTime,
        ActionListener<List<AnomalyResult>> listener
    ) {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scrollKeepAlive);
        User userInfo = SecurityUtil.getUserFromJob(jobParameter, settings);
        String user = userInfo.getName();
        List<String> roles = userInfo.getRoles();
        InjectSecurity injectSecurity = new InjectSecurity(jobParameter.getName(), settings, localClient.threadPool().getThreadContext());
        try {
            injectSecurity.inject(user, roles);
            localClient.searchScroll(scrollRequest, ActionListener.runBefore(ActionListener.wrap(searchResponse -> {
                String nextScrollId = searchResponse.getScrollId();
                SearchHit[] hits = searchResponse.getHits().getHits();
                try {
                    parseAnomalyHits(hits, allAnomalies);
                } catch (Exception parseException) {
                    // If parsing fails after we have a newer scroll id, clear that (not the previous one).
                    clearScroll(jobParameter, nextScrollId != null ? nextScrollId : scrollId);
                    listener.onFailure(parseException);
                    return;
                }

                if (hits.length == 0 || hits.length < pageSize) {
                    log
                        .info(
                            "Successfully parsed {} anomalies in time window {} to {}",
                            allAnomalies.size(),
                            executionStartTime,
                            executionEndTime
                        );
                    clearScroll(jobParameter, nextScrollId);
                    listener.onResponse(allAnomalies);
                    return;
                }

                fetchScrolledAnomalies(
                    jobParameter,
                    nextScrollId,
                    scrollKeepAlive,
                    pageSize,
                    allAnomalies,
                    executionStartTime,
                    executionEndTime,
                    listener
                );
            }, e -> {
                clearScroll(jobParameter, scrollId);
                if (e.getMessage() != null && (e.getMessage().contains("no such index") || e.getMessage().contains("index_not_found"))) {
                    log.info("Anomaly results index does not exist yet (no anomalies recorded)");
                } else {
                    log.error("Failed to query anomaly results", e);
                }
                listener.onFailure(e);
            }), injectSecurity::close));
        } catch (Exception e) {
            injectSecurity.close();
            listener.onFailure(e);
        }
    }

    private void parseAnomalyHits(SearchHit[] hits, List<AnomalyResult> allAnomalies) {
        for (SearchHit hit : hits) {
            try {
                XContentParser parser = org.opensearch.timeseries.util.RestHandlerUtils
                    .createXContentParserFromRegistry(xContentRegistry, hit.getSourceRef());
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyResult anomaly = AnomalyResult.parse(parser);
                allAnomalies.add(anomaly);
            } catch (Exception e) {
                log.warn("Failed to parse anomaly from {} (document may be incomplete)", hit.getId(), e);
            }
        }
    }

    private void clearScroll(Job jobParameter, String scrollId) {
        if (scrollId == null || scrollId.isEmpty()) {
            return;
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        User userInfo = SecurityUtil.getUserFromJob(jobParameter, settings);
        String user = userInfo.getName();
        List<String> roles = userInfo.getRoles();
        InjectSecurity injectSecurity = new InjectSecurity(jobParameter.getName(), settings, localClient.threadPool().getThreadContext());
        try {
            injectSecurity.inject(user, roles);
            localClient
                .clearScroll(clearScrollRequest, ActionListener.runBefore(ActionListener.wrap(ClearScrollResponse::isSucceeded, e -> {
                    log.warn("Failed to clear scroll {}", scrollId, e);
                }), injectSecurity::close));
        } catch (Exception e) {
            injectSecurity.close();
            log.warn("Failed to clear scroll {}", scrollId, e);
        }
    }

    /**
     * Process anomalies with anomaly correlation.
     *
     * @param jobParameter The insights job
     * @param anomalies All collected anomalies
     * @param detectorMetadataMap Detector metadata for insights generation
     * @param executionStartTime Start of analysis window
     * @param executionEndTime End of analysis window
     */
    private void processAnomaliesWithCorrelation(
        Job jobParameter,
        List<AnomalyResult> anomalies,
        Map<String, DetectorMetadata> detectorMetadataMap,
        List<AnomalyDetector> detectors,
        Instant executionStartTime,
        Instant executionEndTime,
        ActionListener<Void> completionListener
    ) {
        if (detectors == null || detectors.isEmpty()) {
            log.warn("No detector configs available for correlation; skipping insights generation");
            completionListener.onResponse(null);
            return;
        }

        try {
            CorrelationPayload payload = buildCorrelationPayload(anomalies);
            if (payload.anomalies.isEmpty()) {
                completionListener.onResponse(null);
                return;
            }

            log.info("AnomalyCorrelation input: {} anomalies, {} detectors", payload.anomalies.size(), detectors.size());
            logCorrelationInputPreview(payload.anomalies);
            logCorrelationDetectorsPreview(detectors);
            List<AnomalyCorrelation.Cluster> clusters = AnomalyCorrelation.clusterWithEventWindows(payload.anomalies, detectors, false);
            logCorrelationClustersPreview(clusters);
            log.info("Anomaly correlation completed, found {} event clusters", clusters.size());

            java.util.Optional<XContentBuilder> insightsDoc = InsightsGenerator
                .generateInsightsFromClusters(
                    clusters,
                    payload.anomalyResultByAnomaly,
                    detectorMetadataMap,
                    executionStartTime,
                    executionEndTime
                );
            if (insightsDoc.isEmpty()) {
                log.info("No insights document generated (clusters empty); skipping write to insights index");
                completionListener.onResponse(null);
                return;
            }
            writeInsightsToIndex(jobParameter, insightsDoc.get(), completionListener);
        } catch (Exception e) {
            log.error("Anomaly correlation failed", e);
            completionListener.onFailure(e);
        }
    }

    private static final class CorrelationPayload {
        private final List<Anomaly> anomalies;
        private final java.util.IdentityHashMap<Anomaly, AnomalyResult> anomalyResultByAnomaly;

        private CorrelationPayload(List<Anomaly> anomalies, java.util.IdentityHashMap<Anomaly, AnomalyResult> anomalyResultByAnomaly) {
            this.anomalies = anomalies;
            this.anomalyResultByAnomaly = anomalyResultByAnomaly;
        }
    }

    private CorrelationPayload buildCorrelationPayload(List<AnomalyResult> anomalies) {
        List<Anomaly> correlationAnomalies = new ArrayList<>();
        java.util.IdentityHashMap<Anomaly, AnomalyResult> anomalyResultByAnomaly = new java.util.IdentityHashMap<>();

        for (AnomalyResult anomaly : anomalies) {
            Instant start = anomaly.getDataStartTime();
            Instant end = anomaly.getDataEndTime();
            if (start == null || end == null || !end.isAfter(start)) {
                continue;
            }

            String modelId = anomaly.getModelId();
            if (modelId == null) {
                modelId = org.opensearch.timeseries.model.IndexableResult.getEntityId(anomaly.getEntity(), anomaly.getConfigId());
            }
            if (modelId == null) {
                modelId = anomaly.getConfigId();
            }

            String configId = anomaly.getConfigId();
            if (configId == null) {
                continue;
            }

            Anomaly correlationAnomaly = new Anomaly(modelId, configId, start, end);
            correlationAnomalies.add(correlationAnomaly);
            anomalyResultByAnomaly.put(correlationAnomaly, anomaly);
        }

        return new CorrelationPayload(correlationAnomalies, anomalyResultByAnomaly);
    }

    /**
     * Write insights document to insights-results index.
     * 
     * @param jobParameter The insights job
     * @param lockService Lock service for releasing lock
     * @param insightsDoc Generated insights document
     */
    private void writeInsightsToIndex(Job jobParameter, XContentBuilder insightsDoc, ActionListener<Void> completionListener) {
        log.info("Writing insights to index: {}", ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS);
        logInsightsDocPreview(insightsDoc);

        User userInfo = SecurityUtil.getUserFromJob(jobParameter, settings);
        String user = userInfo.getName();
        List<String> roles = userInfo.getRoles();

        IndexRequest indexRequest = new IndexRequest(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS).source(insightsDoc);

        // Before writing, validate that the insights result index mapping has not been modified.
        indexManagement.validateInsightsResultIndexMapping(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS, ActionListener.wrap(valid -> {
            if (!valid) {
                log.error("Insights result index mapping is not correct; skipping insights write and ending job run");
                completionListener.onFailure(new IllegalStateException("Insights result index mapping is not correct"));
                return;
            }

            InjectSecurity injectSecurity = new InjectSecurity(
                jobParameter.getName(),
                settings,
                localClient.threadPool().getThreadContext()
            );
            try {
                injectSecurity.inject(user, roles);

                localClient
                    .index(
                        indexRequest,
                        ActionListener.runBefore(ActionListener.wrap(response -> { completionListener.onResponse(null); }, error -> {
                            log.error("Failed to write insights to index", error);
                            completionListener.onFailure(error);
                        }), () -> injectSecurity.close())
                    );
            } catch (Exception e) {
                injectSecurity.close();
                log.error("Failed to inject security context for insights write", e);
                completionListener.onFailure(e);
            }
        }, e -> {
            log.error("Failed to validate insights result index mapping", e);
            completionListener.onFailure(e);
        }));
    }

    @Override
    protected ResultRequest createResultRequest(String configId, long start, long end) {
        // TO-DO: we will make all auto-created detectors use custom result index in the future, so this method will be used.
        throw new UnsupportedOperationException("InsightsJobProcessor does not use createResultRequest");
    }

    @Override
    protected void validateResultIndexAndRunJob(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime,
        String configId,
        String user,
        List<String> roles,
        ExecuteADResultResponseRecorder recorder,
        Config detector
    ) {
        // TO-DO: we will make all auto-created detectors use custom result index in the future, so this method will be used.
        throw new UnsupportedOperationException(
            "InsightsJobProcessor does not use validateResultIndexAndRunJob - it overrides process() entirely"
        );
    }

    /**
     * Fetch detector configs for the detectors present in anomalies and proceed to correlation.
     */
    private void fetchDetectorMetadataAndProceed(
        List<AnomalyResult> anomalies,
        Job jobParameter,
        Instant executionStartTime,
        Instant executionEndTime,
        ActionListener<Void> completionListener
    ) {
        Set<String> detectorIds = new HashSet<>();
        for (AnomalyResult anomaly : anomalies) {
            if (anomaly.getDetectorId() != null) {
                detectorIds.add(anomaly.getDetectorId());
            }
        }

        if (detectorIds.isEmpty()) {
            log.warn("No detector IDs present in anomalies, skipping correlation");
            completionListener.onResponse(null);
            return;
        }

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termsQuery("_id", detectorIds)).size(detectorIds.size());

        SearchRequest request = new SearchRequest(ADCommonName.CONFIG_INDEX).source(source);

        ThreadContext threadContext = localClient.threadPool().getThreadContext();
        // detector configs are stored in system index; use stashed context
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            Client systemClient = pluginClient != null ? pluginClient : localClient;
            try {
                systemClient.search(request, ActionListener.wrap(response -> {
                    Map<String, DetectorMetadata> metadataMap = new HashMap<>();
                    List<AnomalyDetector> detectors = new ArrayList<>();

                    for (SearchHit hit : response.getHits().getHits()) {
                        String id = hit.getId();
                        try (
                            XContentParser parser = org.opensearch.timeseries.util.RestHandlerUtils
                                .createXContentParserFromRegistry(xContentRegistry, hit.getSourceRef())
                        ) {
                            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                            AnomalyDetector detector = AnomalyDetector.parse(parser, id, hit.getVersion());
                            detectors.add(detector);
                            metadataMap.put(id, new DetectorMetadata(id, detector.getName(), detector.getIndices()));
                        } catch (Exception e) {
                            log.warn("Failed to parse detector config {}", id, e);
                            Map<String, Object> src = hit.getSourceAsMap();
                            String name = src != null ? (String) src.get("name") : null;
                            @SuppressWarnings("unchecked")
                            List<String> indices = src != null ? (List<String>) src.get("indices") : new ArrayList<>();
                            metadataMap.put(id, new DetectorMetadata(id, name, indices));
                        }
                    }

                    processAnomaliesWithCorrelation(
                        jobParameter,
                        anomalies,
                        metadataMap,
                        detectors,
                        executionStartTime,
                        executionEndTime,
                        completionListener
                    );
                }, e -> {
                    log.error("Failed to fetch detector configs for metadata enrichment, proceeding with minimal metadata", e);
                    Map<String, DetectorMetadata> fallback = buildDetectorMetadataFromAnomalies(anomalies);
                    processAnomaliesWithCorrelation(
                        jobParameter,
                        anomalies,
                        fallback,
                        new ArrayList<>(),
                        executionStartTime,
                        executionEndTime,
                        completionListener
                    );
                }));
            } catch (IllegalStateException e) {
                // PluginClient present but subject not initialized; fall back to stashed regular client.
                localClient.search(request, ActionListener.wrap(response -> {
                    Map<String, DetectorMetadata> metadataMap = new HashMap<>();
                    List<AnomalyDetector> detectors = new ArrayList<>();

                    for (SearchHit hit : response.getHits().getHits()) {
                        String id = hit.getId();
                        try (
                            XContentParser parser = org.opensearch.timeseries.util.RestHandlerUtils
                                .createXContentParserFromRegistry(xContentRegistry, hit.getSourceRef())
                        ) {
                            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                            AnomalyDetector detector = AnomalyDetector.parse(parser, id, hit.getVersion());
                            detectors.add(detector);
                            metadataMap.put(id, new DetectorMetadata(id, detector.getName(), detector.getIndices()));
                        } catch (Exception ex) {
                            log.warn("Failed to parse detector config {}", id, ex);
                            Map<String, Object> src = hit.getSourceAsMap();
                            String name = src != null ? (String) src.get("name") : null;
                            @SuppressWarnings("unchecked")
                            List<String> indices = src != null ? (List<String>) src.get("indices") : new ArrayList<>();
                            metadataMap.put(id, new DetectorMetadata(id, name, indices));
                        }
                    }

                    processAnomaliesWithCorrelation(
                        jobParameter,
                        anomalies,
                        metadataMap,
                        detectors,
                        executionStartTime,
                        executionEndTime,
                        completionListener
                    );
                }, e2 -> {
                    log.error("Failed to fetch detector configs for metadata enrichment, proceeding with minimal metadata", e2);
                    Map<String, DetectorMetadata> fallback = buildDetectorMetadataFromAnomalies(anomalies);
                    processAnomaliesWithCorrelation(
                        jobParameter,
                        anomalies,
                        fallback,
                        new ArrayList<>(),
                        executionStartTime,
                        executionEndTime,
                        completionListener
                    );
                }));
            }
        } catch (Exception e) {
            completionListener.onFailure(e);
        }
    }

    private Map<String, DetectorMetadata> buildDetectorMetadataFromAnomalies(List<AnomalyResult> anomalies) {
        Map<String, DetectorMetadata> metadataMap = new HashMap<>();

        for (AnomalyResult anomaly : anomalies) {
            String detectorId = anomaly.getDetectorId();

            if (!metadataMap.containsKey(detectorId)) {
                metadataMap.put(detectorId, new DetectorMetadata(detectorId, null, new ArrayList<>()));
            }
        }

        log.info("Built detector metadata from {} anomalies, found {} unique detectors", anomalies.size(), metadataMap.size());
        return metadataMap;
    }

    private void logCorrelationInputPreview(List<Anomaly> anomalies) {
        if (!log.isInfoEnabled()) {
            return;
        }
        if (anomalies == null || anomalies.isEmpty()) {
            return;
        }
        int previewCount = Math.min(3, anomalies.size());
        if (previewCount == 0) {
            return;
        }
        StringBuilder preview = new StringBuilder();
        preview.append("[");
        for (int i = 0; i < previewCount; i++) {
            Anomaly anomaly = anomalies.get(i);
            if (i > 0) {
                preview.append(", ");
            }
            preview
                .append("{modelId=")
                .append(anomaly.getModelId())
                .append(", detectorId=")
                .append(anomaly.getConfigId())
                .append(", start=")
                .append(anomaly.getDataStartTime())
                .append(", end=")
                .append(anomaly.getDataEndTime())
                .append("}");
        }
        preview.append("]");
        log.info("AnomalyCorrelation input preview: {}", preview);
    }

    private void logInsightsDocPreview(XContentBuilder insightsDoc) {
        if (!log.isInfoEnabled()) {
            return;
        }
        try {
            log.info("Insights document preview: {}", truncate(insightsDoc.toString()));
        } catch (Exception e) {
            log.warn("Failed to serialize insights document for logging", e);
        }
    }

    private void logAnomalyResultsQueryPreview(
        SearchRequest request,
        SearchSourceBuilder source,
        int pageSize,
        TimeValue scrollKeepAlive,
        Instant executionStartTime,
        Instant executionEndTime
    ) {
        if (!log.isInfoEnabled()) {
            return;
        }
        String index = request != null && request.indices() != null ? String.join(",", request.indices()) : "(none)";
        log
            .info(
                "Anomaly results query: index={}, window=[{}, {}], pageSize={}, scrollKeepAlive={}",
                index,
                executionStartTime,
                executionEndTime,
                pageSize,
                scrollKeepAlive
            );
        if (source != null) {
            log.info("Anomaly results SearchSource: {}", truncate(source.toString()));
        }
    }

    private void logCorrelationDetectorsPreview(List<AnomalyDetector> detectors) {
        if (!log.isInfoEnabled() || detectors == null || detectors.isEmpty()) {
            return;
        }
        int previewCount = Math.min(3, detectors.size());
        StringBuilder preview = new StringBuilder();
        preview.append("[");
        for (int i = 0; i < previewCount; i++) {
            AnomalyDetector d = detectors.get(i);
            if (i > 0) {
                preview.append(", ");
            }
            if (d == null) {
                preview.append("{null}");
                continue;
            }
            preview
                .append("{id=")
                .append(d.getId())
                .append(", name=")
                .append(d.getName())
                .append(", interval=")
                .append(d.getInterval())
                .append("}");
        }
        preview.append("]");
        log.info("AnomalyCorrelation detectors preview: total={}, sample={}", detectors.size(), preview);
    }

    private void logCorrelationClustersPreview(List<AnomalyCorrelation.Cluster> clusters) {
        if (!log.isInfoEnabled() || clusters == null || clusters.isEmpty()) {
            return;
        }
        int previewCount = Math.min(3, clusters.size());
        StringBuilder preview = new StringBuilder();
        preview.append("[");
        for (int i = 0; i < previewCount; i++) {
            AnomalyCorrelation.Cluster c = clusters.get(i);
            if (i > 0) {
                preview.append(", ");
            }
            if (c == null) {
                preview.append("{null}");
                continue;
            }
            preview
                .append("{eventWindow=")
                .append(c.getEventWindow())
                .append(", anomalyCount=")
                .append(c.getAnomalies() != null ? c.getAnomalies().size() : 0)
                .append("}");
        }
        preview.append("]");
        log.info("AnomalyCorrelation clusters preview: total={}, sample={}", clusters.size(), preview);
    }

    private String truncate(String value) {
        if (value == null || value.length() <= LOG_PREVIEW_LIMIT) {
            return value;
        }
        return value.substring(0, LOG_PREVIEW_LIMIT) + "...(truncated)";
    }
}
