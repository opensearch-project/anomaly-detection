/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.InsightsGenerator;
import org.opensearch.ad.ml.MLCommonsClient;
import org.opensearch.ad.ml.MLMetricsCorrelationInputBuilder;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorMetadata;
import org.opensearch.ad.model.MLMetricsCorrelationInput;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.common.settings.Settings;
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
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.JobProcessor;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.ResultRequest;
import org.opensearch.timeseries.util.SecurityUtil;
import org.opensearch.transport.client.Client;

/**
 * InsightsJobProcessor processes the global Insights job which analyzes all detectors.
 * 
 * Unlike regular AD jobs that run detection on a single detector, the Insights job:
 * 1. Runs at a configured frequency (e.g., every 5 minutes, every 24 hours)
 * 2. Queries detectors created by LLM
 * 3. Retrieves anomaly results from the past execution interval
 * 4. Calls ML Commons metrics correlation algorithm
 * 5. Generates insights and writes them to the insights-results index
 * 
 */
public class InsightsJobProcessor extends
    JobProcessor<ADIndex, ADIndexManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult, ADProfileAction, ExecuteADResultResponseRecorder, ADIndexJobActionHandler> {

    private static final Logger log = LogManager.getLogger(InsightsJobProcessor.class);

    private static InsightsJobProcessor INSTANCE;
    private NamedXContentRegistry xContentRegistry;
    private Settings settings;

    // Local references to parent's private fields (needed for direct access)
    private Client localClient;
    private ThreadPool localThreadPool;
    private String localThreadPoolName;
    private volatile MLCommonsClient mlCommonsClient;

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

    @Override
    public void setClient(org.opensearch.transport.client.Client client) {
        super.setClient(client);
        this.localClient = client;
    }

    @Override
    public void setThreadPool(ThreadPool threadPool) {
        super.setThreadPool(threadPool);
        this.localThreadPool = threadPool;
    }

    private synchronized void initMlCommonsClient() {
        if (this.mlCommonsClient == null && this.localClient != null && this.xContentRegistry != null) {
            try {
                this.mlCommonsClient = new MLCommonsClient(this.localClient, this.xContentRegistry);
            } catch (NoClassDefFoundError e) {
                log.warn("ML Commons classes not found; Insights correlation will be skipped", e);
                this.mlCommonsClient = null;
            } catch (Throwable t) {
                log.warn("Failed to initialize ML Commons client; Insights correlation will be skipped", t);
                this.mlCommonsClient = null;
            }
        }
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
            executionStartTime = executionEndTime.minus(24, ChronoUnit.HOURS);
            // executionStartTime = executionEndTime.minus(intervalAmount, intervalUnit);
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
     * Release lock for job.
     */
    private void releaseLock(Job jobParameter, LockService lockService, LockModel lock) {
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

        querySystemResultIndex(jobParameter, lockService, lock, executionStartTime, executionEndTime);
    }

    /**
     * Query all anomalies from system result index for the given time window.
     * 
     * @param jobParameter The insights job
     * @param lockService Lock service for releasing lock
     * @param lock The acquired lock
     * @param executionStartTime Start of analysis window
     * @param executionEndTime End of analysis window
     */
    private void querySystemResultIndex(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime
    ) {
        log.info("Querying all anomaly results from {} to {}", executionStartTime, executionEndTime);

        User userInfo = SecurityUtil.getUserFromJob(jobParameter, settings);
        String user = userInfo.getName();
        List<String> roles = userInfo.getRoles();

        List<AnomalyResult> allAnomalies = new ArrayList<>();

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        boolQuery
            .filter(
                QueryBuilders
                    .rangeQuery("data_start_time")
                    .gte(executionStartTime.toEpochMilli())
                    .lte(executionEndTime.toEpochMilli())
                    .format("epoch_millis")
            );

        boolQuery.filter(QueryBuilders.rangeQuery("anomaly_grade").gt(0));

        final int pageSize = 10000;

        SearchSourceBuilder baseSource = new SearchSourceBuilder()
            .query(boolQuery)
            .size(pageSize)
            .fetchSource(
                new String[] { "detector_id", "entity", "data_start_time", "data_end_time", "anomaly_grade", "anomaly_score" },
                null
            )
            .sort("data_start_time", SortOrder.ASC)
            .sort("_id", SortOrder.ASC);

        InjectSecurity injectSecurity = new InjectSecurity(jobParameter.getName(), settings, localClient.threadPool().getThreadContext());
        try {
            injectSecurity.inject(user, roles);

            fetchPagedAnomalies(
                baseSource,
                null,
                allAnomalies,
                injectSecurity,
                jobParameter,
                lockService,
                lock,
                executionStartTime,
                executionEndTime
            );
        } catch (Exception e) {
            injectSecurity.close();
            log.error("Failed to inject security context for anomaly query", e);
            releaseLock(jobParameter, lockService, lock);
        }
    }

    /**
     * Fetch anomalies with pagination
     */
    private void fetchPagedAnomalies(
        SearchSourceBuilder baseSource,
        Object[] searchAfter,
        List<AnomalyResult> allAnomalies,
        InjectSecurity injectSecurity,
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime
    ) {
        SearchSourceBuilder pageSource = new SearchSourceBuilder()
            .query(baseSource.query())
            .size(baseSource.size())
            .fetchSource(baseSource.fetchSource())
            .sort("data_start_time", SortOrder.ASC)
            .sort("_id", SortOrder.ASC);

        if (searchAfter != null) {
            pageSource.searchAfter(searchAfter);
        }

        SearchRequest pageRequest = new SearchRequest(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS).source(pageSource);

        localClient.search(pageRequest, ActionListener.wrap(searchResponse -> {
            SearchHit[] hits = searchResponse.getHits().getHits();

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

            // when search results is less than one page
            if (hits.length == 0 || hits.length < baseSource.size()) {
                log
                    .info(
                        "Successfully parsed {} anomalies in time window {} to {}",
                        allAnomalies.size(),
                        executionStartTime,
                        executionEndTime
                    );

                if (!allAnomalies.isEmpty()) {
                    // Enrich detector metadata (names, indices) before correlation
                    fetchDetectorMetadataAndProceed(allAnomalies, jobParameter, lockService, lock, executionStartTime, executionEndTime);
                } else {
                    log.info("No anomalies found in time window, skipping ML correlation");
                    releaseLock(jobParameter, lockService, lock);
                }

                injectSecurity.close();
                return;
            }

            // continue to next page
            Object[] next = hits[hits.length - 1].getSortValues();
            fetchPagedAnomalies(
                baseSource,
                next,
                allAnomalies,
                injectSecurity,
                jobParameter,
                lockService,
                lock,
                executionStartTime,
                executionEndTime
            );
        }, e -> {
            if (e.getMessage() != null && (e.getMessage().contains("no such index") || e.getMessage().contains("index_not_found"))) {
                log.info("Anomaly results index does not exist yet (no anomalies recorded)");
            } else {
                log.error("Failed to query anomaly results", e);
            }
            injectSecurity.close();
            releaseLock(jobParameter, lockService, lock);
        }));
    }

    /**
     * Process anomalies with ML Commons metrics correlation.
     * 
     * @param jobParameter The insights job
     * @param lockService Lock service for releasing lock
     * @param lock The acquired lock
     * @param anomalies All collected anomalies
     * @param detectorMetadataMap Detector metadata for insights generation
     * @param executionStartTime Start of analysis window
     * @param executionEndTime End of analysis window
     */
    private void processAnomaliesWithMLCommons(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        List<AnomalyResult> anomalies,
        Map<String, DetectorMetadata> detectorMetadataMap,
        Instant executionStartTime,
        Instant executionEndTime
    ) {
        MLMetricsCorrelationInput input = MLMetricsCorrelationInputBuilder
            .buildInput(anomalies, detectorMetadataMap, executionStartTime, executionEndTime);

        log.info("Built correlation input: {} metrics × {} buckets", input.getNumMetrics(), input.getNumBuckets());

        log.info("Matrix contents: {}", input.getMatrix());

        if (input.getNumMetrics() == 0) {
            releaseLock(jobParameter, lockService, lock);
            return;
        }

        initMlCommonsClient();
        if (mlCommonsClient == null) {
            log.info("Skipping ML correlation because ML Commons is not available");
            releaseLock(jobParameter, lockService, lock);
            return;
        }
        mlCommonsClient.executeMetricsCorrelation(input, ActionListener.wrap(mlOutput -> {
            log.info("ML Commons correlation completed, found {} event clusters", mlOutput.getInferenceResults().size());

            try {
                XContentBuilder insightsDoc = InsightsGenerator.generateInsights(mlOutput, input);
                writeInsightsToIndex(jobParameter, lockService, lock, insightsDoc);

            } catch (IOException e) {
                log.error("Failed to generate insights document", e);
                releaseLock(jobParameter, lockService, lock);
            }
        }, error -> {
            log.error("ML Commons correlation failed", error);
            releaseLock(jobParameter, lockService, lock);
        }));
    }

    /**
     * Write insights document to insights-results index.
     * 
     * @param jobParameter The insights job
     * @param lockService Lock service for releasing lock
     * @param lock The acquired lock
     * @param insightsDoc Generated insights document
     */
    private void writeInsightsToIndex(Job jobParameter, LockService lockService, LockModel lock, XContentBuilder insightsDoc) {
        log.info("Writing insights to index: {}", ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS);

        User userInfo = SecurityUtil.getUserFromJob(jobParameter, settings);
        String user = userInfo.getName();
        List<String> roles = userInfo.getRoles();

        IndexRequest indexRequest = new IndexRequest(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS).source(insightsDoc);

        InjectSecurity injectSecurity = new InjectSecurity(jobParameter.getName(), settings, localClient.threadPool().getThreadContext());
        try {
            injectSecurity.inject(user, roles);

            localClient
                .index(
                    indexRequest,
                    ActionListener.runBefore(ActionListener.wrap(response -> { releaseLock(jobParameter, lockService, lock); }, error -> {
                        log.error("Failed to write insights to index", error);
                        releaseLock(jobParameter, lockService, lock);
                    }), () -> injectSecurity.close())
                );
        } catch (Exception e) {
            injectSecurity.close();
            log.error("Failed to inject security context for insights write", e);
            releaseLock(jobParameter, lockService, lock);
        }
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
     * Fetch detector configs for the detectors present in anomalies and proceed to ML correlation.
     */
    private void fetchDetectorMetadataAndProceed(
        List<AnomalyResult> anomalies,
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime
    ) {
        Set<String> detectorIds = new HashSet<>();
        for (AnomalyResult anomaly : anomalies) {
            if (anomaly.getDetectorId() != null) {
                detectorIds.add(anomaly.getDetectorId());
            }
        }

        if (detectorIds.isEmpty()) {
            log.warn("No detector IDs present in anomalies, skipping ML correlation");
            releaseLock(jobParameter, lockService, lock);
            return;
        }

        SearchSourceBuilder source = new SearchSourceBuilder()
            .query(QueryBuilders.termsQuery("_id", detectorIds))
            .size(detectorIds.size())
            .fetchSource(new String[] { "name", "indices" }, null);

        SearchRequest request = new SearchRequest(ADCommonName.CONFIG_INDEX).source(source);

        User userInfo = SecurityUtil.getUserFromJob(jobParameter, settings);
        String user = userInfo.getName();
        List<String> roles = userInfo.getRoles();
        InjectSecurity injectSecurity = new InjectSecurity(jobParameter.getName(), settings, localClient.threadPool().getThreadContext());
        try {
            injectSecurity.inject(user, roles);

            localClient.search(request, ActionListener.runBefore(ActionListener.wrap(response -> {
                Map<String, DetectorMetadata> metadataMap = new HashMap<>();

                for (SearchHit hit : response.getHits().getHits()) {
                    try {
                        String id = hit.getId();
                        Map<String, Object> src = hit.getSourceAsMap();
                        String name = src != null ? (String) src.get("name") : null;
                        @SuppressWarnings("unchecked")
                        List<String> indices = src != null ? (List<String>) src.get("indices") : new ArrayList<>();
                        metadataMap.put(id, new DetectorMetadata(id, name, indices));
                    } catch (Exception e) {
                        log.warn("Failed to extract detector metadata from {}", hit.getId(), e);
                    }
                }

                processAnomaliesWithMLCommons(
                    jobParameter,
                    lockService,
                    lock,
                    anomalies,
                    metadataMap,
                    executionStartTime,
                    executionEndTime
                );
            }, e -> {
                log.error("Failed to fetch detector configs for metadata enrichment, proceeding with minimal metadata", e);
                Map<String, DetectorMetadata> fallback = buildDetectorMetadataFromAnomalies(anomalies);
                processAnomaliesWithMLCommons(jobParameter, lockService, lock, anomalies, fallback, executionStartTime, executionEndTime);
            }), () -> injectSecurity.close()));
        } catch (Exception e) {
            injectSecurity.close();
            log.error("Failed to inject security context for detector metadata fetch", e);
            Map<String, DetectorMetadata> fallback = buildDetectorMetadataFromAnomalies(anomalies);
            processAnomaliesWithMLCommons(jobParameter, lockService, lock, anomalies, fallback, executionStartTime, executionEndTime);
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
}
