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

package org.opensearch.ad.task;

import static org.opensearch.ad.AnomalyDetectorPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static org.opensearch.ad.constant.CommonName.AGG_NAME_MAX_TIME;
import static org.opensearch.ad.constant.CommonName.AGG_NAME_MIN_TIME;
import static org.opensearch.ad.model.ADTask.CURRENT_PIECE_FIELD;
import static org.opensearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static org.opensearch.ad.model.ADTask.INIT_PROGRESS_FIELD;
import static org.opensearch.ad.model.ADTask.STATE_FIELD;
import static org.opensearch.ad.model.ADTask.TASK_PROGRESS_FIELD;
import static org.opensearch.ad.model.ADTask.WORKER_NODE_FIELD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_TOP_ENTITIES_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_TOP_ENTITIES_LIMIT_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static org.opensearch.ad.stats.StatNames.AD_EXECUTING_BATCH_TASK_COUNT;
import static org.opensearch.ad.util.ParseUtils.isNullOrEmpty;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.PriorityTracker;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.ADTaskCancelledException;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.feature.SinglePointFeatures;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.ad.transport.ADBatchAnomalyResultRequest;
import org.opensearch.ad.transport.ADBatchAnomalyResultResponse;
import org.opensearch.ad.transport.ADBatchTaskRemoteExecutionAction;
import org.opensearch.ad.transport.handler.AnomalyResultBulkIndexHandler;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ParsedMax;
import org.opensearch.search.aggregations.metrics.ParsedMin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ADBatchTaskRunner {
    private final Logger logger = LogManager.getLogger(ADBatchTaskRunner.class);

    private Settings settings;
    private final ThreadPool threadPool;
    private final SDKRestClient client;
    private final ADStats adStats;
    private final SDKClusterService clusterService;
    private final FeatureManager featureManager;
    private final ADCircuitBreakerService adCircuitBreakerService;
    private final ADTaskManager adTaskManager;
    private final AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final SearchFeatureDao searchFeatureDao;

    private final ADTaskCacheManager adTaskCacheManager;
    private final TransportRequestOptions option;
    private final HashRing hashRing;
    private final ModelManager modelManager;

    private volatile Integer maxAdBatchTaskPerNode;
    private volatile Integer pieceSize;
    private volatile Integer pieceIntervalSeconds;
    private volatile Integer maxTopEntitiesPerHcDetector;
    private volatile Integer maxRunningEntitiesPerDetector;

    private static final int MAX_TOP_ENTITY_SEARCH_BUCKETS = 1000;
    private static final int SLEEP_TIME_FOR_NEXT_ENTITY_TASK_IN_MILLIS = 2000;

    public ADBatchTaskRunner(
        Settings settings,
        ThreadPool threadPool,
        SDKClusterService clusterService,
        SDKRestClient client,
        ADCircuitBreakerService adCircuitBreakerService,
        FeatureManager featureManager,
        ADTaskManager adTaskManager,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ADStats adStats,
        AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler,
        ADTaskCacheManager adTaskCacheManager,
        SearchFeatureDao searchFeatureDao,
        HashRing hashRing,
        ModelManager modelManager
    ) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.anomalyResultBulkIndexHandler = anomalyResultBulkIndexHandler;
        this.adStats = adStats;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.adTaskManager = adTaskManager;
        this.featureManager = featureManager;
        this.anomalyDetectionIndices = anomalyDetectionIndices;

        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();

        this.adTaskCacheManager = adTaskCacheManager;
        this.searchFeatureDao = searchFeatureDao;
        this.hashRing = hashRing;
        this.modelManager = modelManager;

        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);

        this.pieceSize = BATCH_TASK_PIECE_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_SIZE, it -> pieceSize = it);

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);

        this.maxTopEntitiesPerHcDetector = MAX_TOP_ENTITIES_FOR_HISTORICAL_ANALYSIS.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_TOP_ENTITIES_FOR_HISTORICAL_ANALYSIS, it -> maxTopEntitiesPerHcDetector = it);

        this.maxRunningEntitiesPerDetector = MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS, it -> maxRunningEntitiesPerDetector = it);
    }

    /**
     * Run AD task.
     * 1. For HC detector, will get top entities first(initialize top entities). If top
     *    entities already initialized, will execute AD task directly.
     * 2. For single entity detector, execute AD task directly.
     * @param adTask single entity or HC detector task
     * @param transportService transport service
     * @param listener action listener
     */
    public void run(ADTask adTask, TransportService transportService, ActionListener<ADBatchAnomalyResultResponse> listener) {
        boolean isHCDetector = adTask.getDetector().isMultientityDetector();
        if (isHCDetector && !adTaskCacheManager.topEntityInited(adTask.getDetectorId())) {
            // Initialize top entities for HC detector
            threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                ActionListener<ADBatchAnomalyResultResponse> hcDelegatedListener = getInternalHCDelegatedListener(adTask);
                ActionListener<String> topEntitiesListener = getTopEntitiesListener(adTask, transportService, hcDelegatedListener);
                try {
                    getTopEntities(adTask, topEntitiesListener);
                } catch (Exception e) {
                    topEntitiesListener.onFailure(e);
                }
            });
            listener.onResponse(new ADBatchAnomalyResultResponse(clusterService.localNode().getId(), false));
        } else {
            // Execute AD task for single entity detector or HC detector which top entities initialized
            forwardOrExecuteADTask(adTask, transportService, listener);
        }
    }

    private ActionListener<ADBatchAnomalyResultResponse> getInternalHCDelegatedListener(ADTask adTask) {
        return ActionListener
            .wrap(
                r -> logger.debug("[InternalHCDelegatedListener]: running task {} on nodeId {}", adTask.getTaskId(), r.getNodeId()),
                e -> logger.error("[InternalHCDelegatedListener]: failed to run task", e)
            );
    }

    /**
     * Create internal action listener for HC task. The action listener will be used in
     * {@link ADBatchTaskRunner#getTopEntities(ADTask, ActionListener)}. Will call
     * listener's onResponse method when get top entities done.
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param listener action listener
     * @return action listener
     */
    private ActionListener<String> getTopEntitiesListener(
        ADTask adTask,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        String taskId = adTask.getTaskId();
        String detectorId = adTask.getDetectorId();
        ActionListener<String> actionListener = ActionListener.wrap(response -> {
            adTaskCacheManager.setTopEntityInited(detectorId);
            int totalEntities = adTaskCacheManager.getPendingEntityCount(detectorId);
            logger.info("Total top entities: {} for detector {}, task {}", totalEntities, detectorId, taskId);

            /* @anomaly-detection commented until we have support for the hashring, dataNodes will change once we have multi node support : https://github.com/opensearch-project/opensearch-sdk-java/issues/200  
             * 
             *  hashRing.getNodesWithSameLocalAdVersion(dataNodes -> {
            */

            DiscoveryNode[] dataNodes = { clusterService.localNode() };
            int numberOfEligibleDataNodes = dataNodes.length;
            // maxAdBatchTaskPerNode means how many task can run on per data node, which is hard limitation per node.
            // maxRunningEntitiesPerDetector means how many entities can run per detector on whole cluster, which is
            // soft limit to control how many entities to run in parallel per HC detector.
            int maxRunningEntitiesLimit = Math
                .min(totalEntities, Math.min(numberOfEligibleDataNodes * maxAdBatchTaskPerNode, maxRunningEntitiesPerDetector));
            adTaskCacheManager.setDetectorTaskLaneLimit(detectorId, maxRunningEntitiesLimit);
            // scale down HC detector task slots in case there is less top entities in detection date range
            int maxRunningEntities = Math.min(maxRunningEntitiesLimit, adTaskCacheManager.getDetectorTaskSlots(detectorId));
            logger
                .debug(
                    "Calculate task lane for detector {}: totalEntities: {}, numberOfEligibleDataNodes: {}, maxAdBatchTaskPerNode: {}, "
                        + "maxRunningEntitiesPerDetector: {}, maxRunningEntities: {}, detectorTaskSlots: {}",
                    detectorId,
                    totalEntities,
                    numberOfEligibleDataNodes,
                    maxAdBatchTaskPerNode,
                    maxRunningEntitiesPerDetector,
                    maxRunningEntities,
                    adTaskCacheManager.getDetectorTaskSlots(detectorId)
                );
            forwardOrExecuteADTask(adTask, transportService, listener);
            // As we have started one entity task, need to minus 1 for max allowed running entities.
            adTaskCacheManager.setAllowedRunningEntities(detectorId, maxRunningEntities - 1);
        }, e -> {
            logger.debug("Failed to run task " + taskId, e);
            if (adTask.getTaskType().equals(ADTaskType.HISTORICAL_HC_DETECTOR.name())) {
                adTaskManager.entityTaskDone(adTask, e, transportService);
            }
            listener.onFailure(e);
        });
        ThreadedActionListener<String> threadedActionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            actionListener,
            false
        );
        return threadedActionListener;
    }

    /**
     * Get top entities for HC detector. Will use similar logic of realtime detector,
     * but split the whole historical detection date range into limited number of
     * buckets (1000 buckets by default). Will get top entities for each bucket, then
     * put each bucket's top entities into {@link PriorityTracker} to track top
     * entities dynamically. Once all buckets done, we can get finalized top entities
     * in {@link PriorityTracker}.
     *
     * @param adTask AD task
     * @param internalHCListener internal HC listener
     */
    public void getTopEntities(ADTask adTask, ActionListener<String> internalHCListener) {
        getDateRangeOfSourceData(adTask, (dataStartTime, dataEndTime) -> {
            PriorityTracker priorityTracker = new PriorityTracker(
                Clock.systemUTC(),
                adTask.getDetector().getDetectorIntervalInSeconds(),
                adTask.getDetectionDateRange().getStartTime().toEpochMilli(),
                MAX_TOP_ENTITIES_LIMIT_FOR_HISTORICAL_ANALYSIS
            );
            long detectorInterval = adTask.getDetector().getDetectorIntervalInMilliseconds();
            logger
                .debug(
                    "start to search top entities at {}, data start time: {}, data end time: {}, interval: {}",
                    System.currentTimeMillis(),
                    dataStartTime,
                    dataEndTime,
                    detectorInterval
                );
            if (adTask.getDetector().isMultiCategoryDetector()) {
                searchTopEntitiesForMultiCategoryHC(
                    adTask,
                    priorityTracker,
                    dataEndTime,
                    Math.max((dataEndTime - dataStartTime) / MAX_TOP_ENTITY_SEARCH_BUCKETS, detectorInterval),
                    dataStartTime,
                    dataStartTime + detectorInterval,
                    internalHCListener
                );
            } else {
                searchTopEntitiesForSingleCategoryHC(
                    adTask,
                    priorityTracker,
                    dataEndTime,
                    Math.max((dataEndTime - dataStartTime) / MAX_TOP_ENTITY_SEARCH_BUCKETS, detectorInterval),
                    dataStartTime,
                    dataStartTime + detectorInterval,
                    internalHCListener
                );
            }
        }, internalHCListener);
    }

    private void searchTopEntitiesForMultiCategoryHC(
        ADTask adTask,
        PriorityTracker priorityTracker,
        long detectionEndTime,
        long bucketInterval,
        long dataStartTime,
        long dataEndTime,
        ActionListener<String> internalHCListener
    ) {
        checkIfADTaskCancelledAndCleanupCache(adTask);
        ActionListener<List<Entity>> topEntitiesListener = ActionListener.wrap(topEntities -> {
            topEntities
                .forEach(entity -> priorityTracker.updatePriority(adTaskManager.convertEntityToString(entity, adTask.getDetector())));

            if (dataEndTime < detectionEndTime) {
                searchTopEntitiesForMultiCategoryHC(
                    adTask,
                    priorityTracker,
                    detectionEndTime,
                    bucketInterval,
                    dataEndTime,
                    dataEndTime + bucketInterval,
                    internalHCListener
                );
            } else {
                logger.debug("finish searching top entities at " + System.currentTimeMillis());
                List<String> topNEntities = priorityTracker.getTopNEntities(maxTopEntitiesPerHcDetector);
                if (topNEntities.size() == 0) {
                    logger.error("There is no entity found for detector " + adTask.getDetectorId());
                    internalHCListener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "No entity found"));
                    return;
                }
                adTaskCacheManager.addPendingEntities(adTask.getDetectorId(), topNEntities);
                adTaskCacheManager.setTopEntityCount(adTask.getDetectorId(), topNEntities.size());
                internalHCListener.onResponse("Get top entities done");
            }
        }, e -> {
            logger.error("Failed to get top entities for detector " + adTask.getDetectorId(), e);
            internalHCListener.onFailure(e);
        });
        int minimumDocCount = Math.max((int) (bucketInterval / adTask.getDetector().getDetectorIntervalInMilliseconds()) / 2, 1);
        searchFeatureDao
            .getHighestCountEntities(
                adTask.getDetector(),
                dataStartTime,
                dataEndTime,
                MAX_TOP_ENTITIES_LIMIT_FOR_HISTORICAL_ANALYSIS,
                minimumDocCount,
                MAX_TOP_ENTITIES_LIMIT_FOR_HISTORICAL_ANALYSIS,
                topEntitiesListener
            );
    }

    private void searchTopEntitiesForSingleCategoryHC(
        ADTask adTask,
        PriorityTracker priorityTracker,
        long detectionEndTime,
        long interval,
        long dataStartTime,
        long dataEndTime,
        ActionListener<String> internalHCListener
    ) {
        checkIfADTaskCancelledAndCleanupCache(adTask);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(adTask.getDetector().getTimeField())
            .gte(dataStartTime)
            .lte(dataEndTime)
            .format("epoch_millis");
        boolQueryBuilder.filter(rangeQueryBuilder);
        boolQueryBuilder.filter(adTask.getDetector().getFilterQuery());
        sourceBuilder.query(boolQueryBuilder);

        String topEntitiesAgg = "topEntities";
        AggregationBuilder aggregation = new TermsAggregationBuilder(topEntitiesAgg)
            .field(adTask.getDetector().getCategoryField().get(0))
            .size(MAX_TOP_ENTITIES_LIMIT_FOR_HISTORICAL_ANALYSIS);
        sourceBuilder.aggregation(aggregation).size(0);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(adTask.getDetector().getIndices().toArray(new String[0]));
        client.search(searchRequest, ActionListener.wrap(r -> {
            ParsedStringTerms stringTerms = r.getAggregations().get(topEntitiesAgg);
            List<ParsedStringTerms.ParsedBucket> buckets = (List<ParsedStringTerms.ParsedBucket>) stringTerms.getBuckets();
            List<String> topEntities = new ArrayList<>();
            for (ParsedStringTerms.ParsedBucket bucket : buckets) {
                String key = bucket.getKeyAsString();
                topEntities.add(key);
            }

            topEntities.forEach(e -> priorityTracker.updatePriority(e));
            if (dataEndTime < detectionEndTime) {
                searchTopEntitiesForSingleCategoryHC(
                    adTask,
                    priorityTracker,
                    detectionEndTime,
                    interval,
                    dataEndTime,
                    dataEndTime + interval,
                    internalHCListener
                );
            } else {
                logger.debug("finish searching top entities at " + System.currentTimeMillis());
                List<String> topNEntities = priorityTracker.getTopNEntities(maxTopEntitiesPerHcDetector);
                if (topNEntities.size() == 0) {
                    logger.error("There is no entity found for detector " + adTask.getDetectorId());
                    internalHCListener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "No entity found"));
                    return;
                }
                adTaskCacheManager.addPendingEntities(adTask.getDetectorId(), topNEntities);
                adTaskCacheManager.setTopEntityCount(adTask.getDetectorId(), topNEntities.size());
                internalHCListener.onResponse("Get top entities done");
            }
        }, e -> {
            logger.error("Failed to get top entities for detector " + adTask.getDetectorId(), e);
            internalHCListener.onFailure(e);
        }));
    }

    /**
     * Forward AD task to work node.
     * 1. For HC detector, return directly if no more pending entity. Otherwise check if
     *    there is AD task created for this entity. If yes, just forward the entity task
     *    to worker node; otherwise, create entity task first, then forward.
     * 2. For single entity detector, set task as INIT state and forward task to worker
     *    node.
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param listener action listener
     */
    public void forwardOrExecuteADTask(
        ADTask adTask,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        try {
            checkIfADTaskCancelledAndCleanupCache(adTask);
            String detectorId = adTask.getDetectorId();
            AnomalyDetector detector = adTask.getDetector();
            boolean isHCDetector = detector.isMultientityDetector();
            if (isHCDetector) {
                String entityString = adTaskCacheManager.pollEntity(detectorId);
                logger.debug("Start to run entity: {} of detector {}", entityString, detectorId);
                if (entityString == null) {
                    listener.onResponse(new ADBatchAnomalyResultResponse(clusterService.localNode().getId(), false));
                    return;
                }
                ActionListener<Object> wrappedListener = ActionListener.wrap(r -> logger.debug("Entity task created successfully"), e -> {
                    logger.error("Failed to start entity task for detector: {}, entity: {}", detectorId, entityString);
                    // If fail, move the entity into pending task queue
                    adTaskCacheManager.addPendingEntity(detectorId, entityString);
                });
                // This is to handle retry case. To retry entity, we need to get the old entity task created before.
                Entity entity = adTaskManager.parseEntityFromString(entityString, adTask);
                String parentTaskId = adTask.getTaskType().equals(ADTaskType.HISTORICAL_HC_ENTITY.name())
                    ? adTask.getParentTaskId() // For HISTORICAL_HC_ENTITY task, return its parent task id
                    : adTask.getTaskId(); // For HISTORICAL_HC_DETECTOR task, its task id is parent task id
                adTaskManager
                    .getAndExecuteOnLatestADTask(
                        detectorId,
                        parentTaskId,
                        entity,
                        ImmutableList.of(ADTaskType.HISTORICAL_HC_ENTITY),
                        existingEntityTask -> {
                            if (existingEntityTask.isPresent()) { // retry failed entity caused by limit exceed exception
                                // TODO: if task failed due to limit exceed exception in half way, resume from the break point or just clear
                                // the
                                // old AD tasks and rerun it? Currently we just support rerunning task failed due to limit exceed exception
                                // before starting.
                                ADTask adEntityTask = existingEntityTask.get();
                                logger
                                    .debug(
                                        "Rerun entity task for task id: {}, error of last run: {}",
                                        adEntityTask.getTaskId(),
                                        adEntityTask.getError()
                                    );
                                ActionListener<ADBatchAnomalyResultResponse> workerNodeResponseListener = workerNodeResponseListener(
                                    adEntityTask,
                                    transportService,
                                    listener
                                );
                                forwardOrExecuteEntityTask(adEntityTask, transportService, workerNodeResponseListener);
                            } else {
                                logger.info("Create entity task for entity:{}", entityString);
                                Instant now = Instant.now();
                                ADTask adEntityTask = new ADTask.Builder()
                                    .detectorId(adTask.getDetectorId())
                                    .detector(detector)
                                    .isLatest(true)
                                    .taskType(ADTaskType.HISTORICAL_HC_ENTITY.name())
                                    .executionStartTime(now)
                                    .taskProgress(0.0f)
                                    .initProgress(0.0f)
                                    .state(ADTaskState.INIT.name())
                                    .initProgress(0.0f)
                                    .lastUpdateTime(now)
                                    .startedBy(adTask.getStartedBy())
                                    .coordinatingNode(clusterService.localNode().getId())
                                    .detectionDateRange(adTask.getDetectionDateRange())
                                    .user(adTask.getUser())
                                    .entity(entity)
                                    .parentTaskId(parentTaskId)
                                    .build();
                                adTaskManager.createADTaskDirectly(adEntityTask, r -> {
                                    adEntityTask.setTaskId(r.getId());
                                    ActionListener<ADBatchAnomalyResultResponse> workerNodeResponseListener = workerNodeResponseListener(
                                        adEntityTask,
                                        transportService,
                                        listener
                                    );
                                    forwardOrExecuteEntityTask(adEntityTask, transportService, workerNodeResponseListener);
                                }, wrappedListener);
                            }
                        },
                        transportService,
                        false,
                        wrappedListener
                    );
            } else {
                Map<String, Object> updatedFields = new HashMap<>();
                updatedFields.put(STATE_FIELD, ADTaskState.INIT.name());
                updatedFields.put(INIT_PROGRESS_FIELD, 0.0f);
                ActionListener<ADBatchAnomalyResultResponse> workerNodeResponseListener = workerNodeResponseListener(
                    adTask,
                    transportService,
                    listener
                );
                adTaskManager
                    .updateADTask(
                        adTask.getTaskId(),
                        updatedFields,
                        ActionListener
                            .wrap(
                                r -> forwardOrExecuteEntityTask(adTask, transportService, workerNodeResponseListener),
                                e -> { workerNodeResponseListener.onFailure(e); }
                            )
                    );
            }
        } catch (Exception e) {
            logger.error("Failed to forward or execute AD task " + adTask.getTaskId(), e);
            listener.onFailure(e);
        }
    }

    /**
     * Return delegated listener to listen to task execution response. After task
     * dispatched to worker node, this listener will listen to response from
     * worker node.
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param listener action listener
     * @return action listener
     */
    private ActionListener<ADBatchAnomalyResultResponse> workerNodeResponseListener(
        ADTask adTask,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        ActionListener<ADBatchAnomalyResultResponse> actionListener = ActionListener.wrap(r -> {
            listener.onResponse(r);
            if (adTask.isEntityTask()) {
                // When reach this line, the entity task already been put into worker node's cache.
                // Then it's safe to move entity from temp entities queue to running entities queue.
                adTaskCacheManager.moveToRunningEntity(adTask.getDetectorId(), adTaskManager.convertEntityToString(adTask));
            }
            startNewEntityTaskLane(adTask, transportService);
        }, e -> {
            logger.error("Failed to dispatch task to worker node, task id: " + adTask.getTaskId(), e);
            listener.onFailure(e);
            handleException(adTask, e);

            if (adTask.getDetector().isMultientityDetector()) {
                // Entity task done on worker node. Send entity task done message to coordinating node to poll next entity.
                adTaskManager.entityTaskDone(adTask, e, transportService);
                if (adTaskCacheManager.getAvailableNewEntityTaskLanes(adTask.getDetectorId()) > 0) {
                    // When reach this line, it means entity task failed to start on worker node
                    // Sleep some time before starting new task lane.
                    threadPool
                        .schedule(
                            () -> startNewEntityTaskLane(adTask, transportService),
                            TimeValue.timeValueSeconds(SLEEP_TIME_FOR_NEXT_ENTITY_TASK_IN_MILLIS),
                            AD_BATCH_TASK_THREAD_POOL_NAME
                        );
                }
            }
        });

        ThreadedActionListener<ADBatchAnomalyResultResponse> threadedActionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            actionListener,
            false
        );
        return threadedActionListener;
    }

    private void forwardOrExecuteEntityTask(
        ADTask adTask,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> workerNodeResponseListener
    ) {
        checkIfADTaskCancelledAndCleanupCache(adTask);
        dispatchTask(adTask, ActionListener.wrap(node -> {
            if (clusterService.localNode().getId().equals(node.getId())) {
                // Execute batch task locally
                startADBatchTaskOnWorkerNode(adTask, false, transportService, workerNodeResponseListener);
            } else {
                // For extensions this will never happen as all requests are directed to extension node (clusterService.localNode())
                // Execute batch task remotely
                transportService
                    .sendRequest(
                        node,
                        ADBatchTaskRemoteExecutionAction.NAME,
                        new ADBatchAnomalyResultRequest(adTask),
                        option,
                        new ActionListenerResponseHandler<>(workerNodeResponseListener, ADBatchAnomalyResultResponse::new)
                    );
            }
        }, e -> workerNodeResponseListener.onFailure(e)));
    }

    // start new entity task lane
    private synchronized void startNewEntityTaskLane(ADTask adTask, TransportService transportService) {
        if (adTask.getDetector().isMultientityDetector() && adTaskCacheManager.getAndDecreaseEntityTaskLanes(adTask.getDetectorId()) > 0) {
            logger.debug("start new task lane for detector {}", adTask.getDetectorId());
            forwardOrExecuteADTask(adTask, transportService, getInternalHCDelegatedListener(adTask));
        }
    }

    private void dispatchTask(ADTask adTask, ActionListener<DiscoveryNode> listener) {
        listener.onResponse(clusterService.localNode());
        /* @anomaly.detection commented until we have support for the hashring : https://github.com/opensearch-project/opensearch-sdk-java/issues/200 , not necessary to query for node stats as all tasks are dispatched to extension node 
        hashRing.getNodesWithSameLocalAdVersion(dataNodes -> {
            ADStatsRequest adStatsRequest = new ADStatsRequest(dataNodes);
            adStatsRequest.addAll(ImmutableSet.of(AD_EXECUTING_BATCH_TASK_COUNT.getName(), JVM_HEAP_USAGE.getName()));
        
            client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
                List<ADStatsNodeResponse> candidateNodeResponse = adStatsResponse
                    .getNodes()
                    .stream()
                    .filter(stat -> (long) stat.getStatsMap().get(JVM_HEAP_USAGE.getName()) < DEFAULT_JVM_HEAP_USAGE_THRESHOLD)
                    .collect(Collectors.toList());
        
                if (candidateNodeResponse.size() == 0) {
                    StringBuilder errorMessageBuilder = new StringBuilder("All nodes' memory usage exceeds limitation ")
                        .append(DEFAULT_JVM_HEAP_USAGE_THRESHOLD)
                        .append("%. ")
                        .append(NO_ELIGIBLE_NODE_TO_RUN_DETECTOR)
                        .append(adTask.getDetectorId());
                    String errorMessage = errorMessageBuilder.toString();
                    logger.warn(errorMessage + ", task id " + adTask.getTaskId() + ", " + adTask.getTaskType());
                    listener.onFailure(new LimitExceededException(adTask.getDetectorId(), errorMessage));
                    return;
                }
                candidateNodeResponse = candidateNodeResponse
                    .stream()
                    .filter(stat -> (Long) stat.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName()) < maxAdBatchTaskPerNode)
                    .collect(Collectors.toList());
                if (candidateNodeResponse.size() == 0) {
                    StringBuilder errorMessageBuilder = new StringBuilder("All nodes' executing batch tasks exceeds limitation ")
                        .append(NO_ELIGIBLE_NODE_TO_RUN_DETECTOR)
                        .append(adTask.getDetectorId());
                    String errorMessage = errorMessageBuilder.toString();
                    logger.warn(errorMessage + ", task id " + adTask.getTaskId() + ", " + adTask.getTaskType());
                    listener.onFailure(new LimitExceededException(adTask.getDetectorId(), errorMessage));
                    return;
                }
                Optional<ADStatsNodeResponse> targetNode = candidateNodeResponse
                    .stream()
                    .sorted((ADStatsNodeResponse r1, ADStatsNodeResponse r2) -> {
                        int result = ((Long) r1.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName()))
                            .compareTo((Long) r2.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName()));
                        if (result == 0) {
                            // if multiple nodes have same running task count, choose the one with least
                            // JVM heap usage.
                            return ((Long) r1.getStatsMap().get(JVM_HEAP_USAGE.getName()))
                                .compareTo((Long) r2.getStatsMap().get(JVM_HEAP_USAGE.getName()));
                        }
                        return result;
                    })
                    .findFirst();
                listener.onResponse(targetNode.get().getNode());
            }, exception -> {
                logger.error("Failed to get node's task stats", exception);
                listener.onFailure(exception);
            }));
        }, listener);
        */
    }

    /**
     * Start AD task in dedicated batch task thread pool on worker node.
     *
     * @param adTask ad task
     * @param runTaskRemotely run task remotely or not
     * @param transportService transport service
     * @param delegatedListener action listener
     */
    public void startADBatchTaskOnWorkerNode(
        ADTask adTask,
        boolean runTaskRemotely,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> delegatedListener
    ) {
        try {
            // check if cluster is eligible to run AD currently, if not eligible like
            // circuit breaker open, will throw exception.
            checkClusterState(adTask);
            threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                ActionListener<String> internalListener = internalBatchTaskListener(adTask, transportService);
                try {
                    executeADBatchTaskOnWorkerNode(adTask, internalListener);
                } catch (Exception e) {
                    internalListener.onFailure(e);
                }
            });
            delegatedListener.onResponse(new ADBatchAnomalyResultResponse(clusterService.localNode().getId(), runTaskRemotely));
        } catch (Exception e) {
            logger.error("Fail to start AD batch task " + adTask.getTaskId(), e);
            delegatedListener.onFailure(e);
        }
    }

    private ActionListener<String> internalBatchTaskListener(ADTask adTask, TransportService transportService) {
        String taskId = adTask.getTaskId();
        String detectorTaskId = adTask.getDetectorLevelTaskId();
        String detectorId = adTask.getDetectorId();
        ActionListener<String> listener = ActionListener.wrap(response -> {
            // If batch task finished normally, remove task from cache and decrease executing task count by 1.
            adTaskCacheManager.remove(taskId, detectorId, detectorTaskId);
            adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
            if (!adTask.getDetector().isMultientityDetector()) {
                // Set single-entity detector task as FINISHED here
                adTaskManager
                    .cleanDetectorCache(
                        adTask,
                        transportService,
                        () -> adTaskManager.updateADTask(taskId, ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name()))
                    );
            } else {
                // Set entity task as FINISHED here
                adTaskManager.updateADTask(adTask.getTaskId(), ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name()));
                adTaskManager.entityTaskDone(adTask, null, transportService);
            }
        }, e -> {
            // If batch task failed, remove task from cache and decrease executing task count by 1.
            adTaskCacheManager.remove(taskId, detectorId, detectorTaskId);
            adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
            if (!adTask.getDetector().isMultientityDetector()) {
                adTaskManager.cleanDetectorCache(adTask, transportService, () -> handleException(adTask, e));
            } else {
                adTaskManager.entityTaskDone(adTask, e, transportService);
                handleException(adTask, e);
            }
        });
        ThreadedActionListener<String> threadedActionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            listener,
            false
        );
        return threadedActionListener;
    }

    private void handleException(ADTask adTask, Exception e) {
        // Check if batch task was cancelled or not by exception type.
        // If it's cancelled, then increase cancelled task count by 1, otherwise increase failure count by 1.
        if (e instanceof ADTaskCancelledException) {
            adStats.getStat(StatNames.AD_CANCELED_BATCH_TASK_COUNT.getName()).increment();
        } else if (ExceptionUtil.countInStats(e)) {
            adStats.getStat(StatNames.AD_BATCH_TASK_FAILURE_COUNT.getName()).increment();
        }
        // Handle AD task exception
        adTaskManager.handleADTaskException(adTask, e);
    }

    private void executeADBatchTaskOnWorkerNode(ADTask adTask, ActionListener<String> internalListener) {
        // track AD executing batch task and total batch task execution count
        adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).increment();
        adStats.getStat(StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT.getName()).increment();

        // put AD task into cache
        adTaskCacheManager.add(adTask);

        // start to run first piece
        Instant executeStartTime = Instant.now();
        // TODO: refactor to make the workflow more clear
        runFirstPiece(adTask, executeStartTime, internalListener);
    }

    private void checkClusterState(ADTask adTask) {
        // check if AD plugin is enabled
        checkADPluginEnabled(adTask.getDetectorId());

        // check if circuit breaker is open
        checkCircuitBreaker(adTask);
    }

    private void checkADPluginEnabled(String detectorId) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new EndRunException(detectorId, CommonErrorMessages.DISABLED_ERR_MSG, true).countedInStats(false);
        }
    }

    private void checkCircuitBreaker(ADTask adTask) {
        String taskId = adTask.getTaskId();
        if (adCircuitBreakerService.isOpen()) {
            String error = "Circuit breaker is open";
            logger.error("AD task: {}, {}", taskId, error);
            throw new LimitExceededException(adTask.getDetectorId(), error, true);
        }
    }

    private void runFirstPiece(ADTask adTask, Instant executeStartTime, ActionListener<String> internalListener) {
        try {
            adTaskManager
                .updateADTask(
                    adTask.getTaskId(),
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            ADTaskState.INIT.name(),
                            CURRENT_PIECE_FIELD,
                            adTask.getDetectionDateRange().getStartTime().toEpochMilli(),
                            TASK_PROGRESS_FIELD,
                            0.0f,
                            INIT_PROGRESS_FIELD,
                            0.0f,
                            WORKER_NODE_FIELD,
                            clusterService.localNode().getId()
                        ),
                    ActionListener.wrap(r -> {
                        try {
                            checkIfADTaskCancelledAndCleanupCache(adTask);
                            getDateRangeOfSourceData(adTask, (dataStartTime, dataEndTime) -> {
                                long interval = ((IntervalTimeConfiguration) adTask.getDetector().getDetectionInterval())
                                    .toDuration()
                                    .toMillis();
                                long expectedPieceEndTime = dataStartTime + pieceSize * interval;
                                long firstPieceEndTime = Math.min(expectedPieceEndTime, dataEndTime);
                                logger
                                    .debug(
                                        "start first piece from {} to {}, interval {}, dataStartTime {}, dataEndTime {},"
                                            + " detectorId {}, taskId {}",
                                        dataStartTime,
                                        firstPieceEndTime,
                                        interval,
                                        dataStartTime,
                                        dataEndTime,
                                        adTask.getDetectorId(),
                                        adTask.getTaskId()
                                    );
                                getFeatureData(
                                    adTask,
                                    dataStartTime, // first piece start time
                                    firstPieceEndTime, // first piece end time
                                    dataStartTime,
                                    dataEndTime,
                                    interval,
                                    executeStartTime,
                                    internalListener
                                );
                            }, internalListener);
                        } catch (Exception e) {
                            internalListener.onFailure(e);
                        }
                    }, internalListener::onFailure)
                );
        } catch (Exception exception) {
            internalListener.onFailure(exception);
        }
    }

    private void getDateRangeOfSourceData(ADTask adTask, BiConsumer<Long, Long> consumer, ActionListener<String> internalListener) {
        String taskId = adTask.getTaskId();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.min(AGG_NAME_MIN_TIME).field(adTask.getDetector().getTimeField()))
            .aggregation(AggregationBuilders.max(AGG_NAME_MAX_TIME).field(adTask.getDetector().getTimeField()))
            .size(0);
        if (adTask.getEntity() != null && adTask.getEntity().getAttributes().size() > 0) {
            BoolQueryBuilder query = new BoolQueryBuilder();
            adTask
                .getEntity()
                .getAttributes()
                .entrySet()
                .forEach(entity -> query.filter(new TermQueryBuilder(entity.getKey(), entity.getValue())));
            searchSourceBuilder.query(query);
        }

        SearchRequest request = new SearchRequest()
            .indices(adTask.getDetector().getIndices().toArray(new String[0]))
            .source(searchSourceBuilder);

        client.search(request, ActionListener.wrap(r -> {
            ParsedMin minAgg = r.getAggregations().get(AGG_NAME_MIN_TIME);
            ParsedMax maxAgg = r.getAggregations().get(AGG_NAME_MAX_TIME);
            double minValue = minAgg.getValue();
            double maxValue = maxAgg.getValue();
            // If time field not exist or there is no value, will return infinity value
            if (minValue == Double.POSITIVE_INFINITY) {
                internalListener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "There is no data in the time field"));
                return;
            }
            long interval = ((IntervalTimeConfiguration) adTask.getDetector().getDetectionInterval()).toDuration().toMillis();

            DetectionDateRange detectionDateRange = adTask.getDetectionDateRange();
            long dataStartTime = detectionDateRange.getStartTime().toEpochMilli();
            long dataEndTime = detectionDateRange.getEndTime().toEpochMilli();
            long minDate = (long) minValue;
            long maxDate = (long) maxValue;

            if (minDate >= dataEndTime || maxDate <= dataStartTime) {
                internalListener
                    .onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "There is no data in the detection date range"));
                return;
            }
            if (minDate > dataStartTime) {
                dataStartTime = minDate;
            }
            if (maxDate < dataEndTime) {
                dataEndTime = maxDate;
            }

            // normalize start/end time to make it consistent with feature data agg result
            dataStartTime = dataStartTime - dataStartTime % interval;
            dataEndTime = dataEndTime - dataEndTime % interval;
            logger.debug("adjusted date range: start: {}, end: {}, taskId: {}", dataStartTime, dataEndTime, taskId);
            if ((dataEndTime - dataStartTime) < NUM_MIN_SAMPLES * interval) {
                internalListener.onFailure(new AnomalyDetectionException("There is not enough data to train model").countedInStats(false));
                return;
            }
            consumer.accept(dataStartTime, dataEndTime);
        }, e -> { internalListener.onFailure(e); }));
    }

    private void getFeatureData(
        ADTask adTask,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime,
        ActionListener<String> internalListener
    ) {
        ActionListener<Map<Long, Optional<double[]>>> actionListener = ActionListener.wrap(dataPoints -> {
            try {
                if (dataPoints.size() == 0) {
                    logger.debug("No data in current piece with end time: " + pieceEndTime);
                    // Current piece end time is the next piece's start time
                    runNextPiece(adTask, pieceEndTime, dataStartTime, dataEndTime, interval, internalListener);
                } else {
                    detectAnomaly(
                        adTask,
                        dataPoints,
                        pieceStartTime,
                        pieceEndTime,
                        dataStartTime,
                        dataEndTime,
                        interval,
                        executeStartTime,
                        internalListener
                    );
                }
            } catch (Exception e) {
                internalListener.onFailure(e);
            }
        }, exception -> {
            logger.debug("Fail to get feature data by batch for this piece with end time: " + pieceEndTime);
            // TODO: Exception may be caused by wrong feature query or some bad data. Differentiate these
            // and skip current piece if error caused by bad data.
            internalListener.onFailure(exception);
        });
        ThreadedActionListener<Map<Long, Optional<double[]>>> threadedActionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            actionListener,
            false
        );

        featureManager
            .getFeatureDataPointsByBatch(adTask.getDetector(), adTask.getEntity(), pieceStartTime, pieceEndTime, threadedActionListener);
    }

    private void detectAnomaly(
        ADTask adTask,
        Map<Long, Optional<double[]>> dataPoints,
        long pieceStartTime,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        Instant executeStartTime,
        ActionListener<String> internalListener
    ) {
        String taskId = adTask.getTaskId();
        ThresholdedRandomCutForest trcf = adTaskCacheManager.getTRcfModel(taskId);
        Deque<Map.Entry<Long, Optional<double[]>>> shingle = adTaskCacheManager.getShingle(taskId);

        List<AnomalyResult> anomalyResults = new ArrayList<>();

        long intervalEndTime = pieceStartTime;
        for (int i = 0; i < pieceSize && intervalEndTime < dataEndTime; i++) {
            Optional<double[]> dataPoint = dataPoints.containsKey(intervalEndTime) ? dataPoints.get(intervalEndTime) : Optional.empty();
            intervalEndTime = intervalEndTime + interval;
            SinglePointFeatures feature = featureManager
                .getShingledFeatureForHistoricalAnalysis(adTask.getDetector(), shingle, dataPoint, intervalEndTime);
            List<FeatureData> featureData = null;
            if (feature.getUnprocessedFeatures().isPresent()) {
                featureData = ParseUtils.getFeatureData(feature.getUnprocessedFeatures().get(), adTask.getDetector());
            }
            if (!feature.getProcessedFeatures().isPresent()) {
                String error = feature.getUnprocessedFeatures().isPresent()
                    ? "No full shingle in current detection window"
                    : "No data in current detection window";
                AnomalyResult anomalyResult = new AnomalyResult(
                    adTask.getDetectorId(),
                    adTask.getDetectorLevelTaskId(),
                    featureData,
                    Instant.ofEpochMilli(intervalEndTime - interval),
                    Instant.ofEpochMilli(intervalEndTime),
                    executeStartTime,
                    Instant.now(),
                    error,
                    adTask.getEntity(),
                    adTask.getDetector().getUser(),
                    anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT),
                    adTask.getEntityModelId()
                );
                anomalyResults.add(anomalyResult);
            } else {
                double[] point = feature.getProcessedFeatures().get();
                // 0 is placeholder for timestamp. In the future, we will add
                // data time stamp there.
                AnomalyDescriptor descriptor = trcf.process(point, 0);
                double score = descriptor.getRCFScore();
                if (!adTaskCacheManager.isThresholdModelTrained(taskId) && score > 0) {
                    adTaskCacheManager.setThresholdModelTrained(taskId, true);
                }

                AnomalyResult anomalyResult = AnomalyResult
                    .fromRawTRCFResult(
                        adTask.getDetectorId(),
                        adTask.getDetector().getDetectorIntervalInMilliseconds(),
                        adTask.getDetectorLevelTaskId(),
                        score,
                        descriptor.getAnomalyGrade(),
                        descriptor.getDataConfidence(),
                        featureData,
                        Instant.ofEpochMilli(intervalEndTime - interval),
                        Instant.ofEpochMilli(intervalEndTime),
                        executeStartTime,
                        Instant.now(),
                        null,
                        adTask.getEntity(),
                        adTask.getDetector().getUser(),
                        anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT),
                        adTask.getEntityModelId(),
                        modelManager.normalizeAttribution(trcf.getForest(), descriptor.getRelevantAttribution()),
                        descriptor.getRelativeIndex(),
                        descriptor.getPastValues(),
                        descriptor.getExpectedValuesList(),
                        descriptor.getLikelihoodOfValues(),
                        descriptor.getThreshold()
                    );
                anomalyResults.add(anomalyResult);
            }
        }

        String user;
        List<String> roles;
        if (adTask.getUser() == null) {
            // It's possible that user create domain with security disabled, then enable security
            // after upgrading. This is for BWC, for old detectors which created when security
            // disabled, the user will be null.
            user = "";
            roles = settings.getAsList("", ImmutableList.of("all_access", "AmazonES_all_access"));
        } else {
            user = adTask.getUser().getName();
            roles = adTask.getUser().getRoles();
        }
        String resultIndex = adTask.getDetector().getResultIndex();

        if (resultIndex == null) {
            // if result index is null, store anomaly result directly
            storeAnomalyResultAndRunNextPiece(
                adTask,
                pieceEndTime,
                dataStartTime,
                dataEndTime,
                interval,
                internalListener,
                anomalyResults,
                resultIndex,
                null
            );
            return;
        }

        try {
            storeAnomalyResultAndRunNextPiece(
                adTask,
                pieceEndTime,
                dataStartTime,
                dataEndTime,
                interval,
                internalListener,
                anomalyResults,
                resultIndex,
                null
            );
        } catch (Exception exception) {
            logger.error("Failed to inject user roles", exception);
            internalListener.onFailure(exception);
        }
    }

    private void storeAnomalyResultAndRunNextPiece(
        ADTask adTask,
        long pieceEndTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        ActionListener<String> internalListener,
        List<AnomalyResult> anomalyResults,
        String resultIndex,
        CheckedRunnable<?> runBefore
    ) {
        ActionListener actionListener = new ThreadedActionListener<>(
            logger,
            threadPool,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            ActionListener.wrap(r -> {
                try {
                    runNextPiece(adTask, pieceEndTime, dataStartTime, dataEndTime, interval, internalListener);
                } catch (Exception e) {
                    internalListener.onFailure(e);
                }
            }, e -> {
                logger.error("Fail to bulk index anomaly result", e);
                internalListener.onFailure(e);
            }),
            false
        );
        anomalyResultBulkIndexHandler
            .bulkIndexAnomalyResult(
                resultIndex,
                anomalyResults,
                runBefore == null ? actionListener : ActionListener.runBefore(actionListener, runBefore)
            );
    }

    private void runNextPiece(
        ADTask adTask,
        long pieceStartTime,
        long dataStartTime,
        long dataEndTime,
        long interval,
        ActionListener<String> internalListener
    ) {
        String taskId = adTask.getTaskId();
        String detectorId = adTask.getDetectorId();
        String detectorTaskId = adTask.getDetectorLevelTaskId();
        float initProgress = calculateInitProgress(taskId);
        String taskState = initProgress >= 1.0f ? ADTaskState.RUNNING.name() : ADTaskState.INIT.name();
        logger.debug("Init progress: {}, taskState:{}, task id: {}", initProgress, taskState, taskId);

        if (initProgress >= 1.0f && adTask.isEntityTask()) {
            updateDetectorLevelTaskState(detectorId, adTask.getParentTaskId(), ADTaskState.RUNNING.name());
        }

        if (pieceStartTime < dataEndTime) {
            checkIfADTaskCancelledAndCleanupCache(adTask);
            threadPool.schedule(() -> {
                checkClusterState(adTask);
                long expectedPieceEndTime = pieceStartTime + pieceSize * interval;
                long pieceEndTime = expectedPieceEndTime > dataEndTime ? dataEndTime : expectedPieceEndTime;
                logger
                    .debug(
                        "task id: {}, start next piece start from {} to {}, interval {}",
                        adTask.getTaskId(),
                        pieceStartTime,
                        pieceEndTime,
                        interval
                    );
                float taskProgress = (float) (pieceStartTime - dataStartTime) / (dataEndTime - dataStartTime);
                logger.debug("Task progress: {}, task id:{}, detector id:{}", taskProgress, taskId, detectorId);
                adTaskManager
                    .updateADTask(
                        taskId,
                        ImmutableMap
                            .of(
                                STATE_FIELD,
                                taskState,
                                CURRENT_PIECE_FIELD,
                                pieceStartTime,
                                TASK_PROGRESS_FIELD,
                                taskProgress,
                                INIT_PROGRESS_FIELD,
                                initProgress
                            ),
                        ActionListener
                            .wrap(
                                r -> getFeatureData(
                                    adTask,
                                    pieceStartTime,
                                    pieceEndTime,
                                    dataStartTime,
                                    dataEndTime,
                                    interval,
                                    Instant.now(),
                                    internalListener
                                ),
                                e -> internalListener.onFailure(e)
                            )
                    );
            }, TimeValue.timeValueSeconds(pieceIntervalSeconds), AD_BATCH_TASK_THREAD_POOL_NAME);
        } else {
            logger.info("AD task finished for detector {}, task id: {}", detectorId, taskId);
            adTaskCacheManager.remove(taskId, detectorId, detectorTaskId);
            adTaskManager
                .updateADTask(
                    taskId,
                    ImmutableMap
                        .of(
                            CURRENT_PIECE_FIELD,
                            dataEndTime,
                            TASK_PROGRESS_FIELD,
                            1.0f,
                            EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli(),
                            INIT_PROGRESS_FIELD,
                            initProgress,
                            STATE_FIELD,
                            ADTaskState.FINISHED
                        ),
                    ActionListener.wrap(r -> internalListener.onResponse("task execution done"), e -> internalListener.onFailure(e))
                );
        }
    }

    private void updateDetectorLevelTaskState(String detectorId, String detectorTaskId, String newState) {
        AnomalyDetectorFunction function = () -> adTaskManager
            .updateADTask(detectorTaskId, ImmutableMap.of(STATE_FIELD, newState), ActionListener.wrap(r -> {
                logger.info("Updated HC detector task: {} state as: {} for detector: {}", detectorTaskId, newState, detectorId);
                adTaskCacheManager.updateDetectorTaskState(detectorId, detectorTaskId, newState);
            }, e -> { logger.error("Failed to update HC detector task: {} for detector: {}", detectorTaskId, detectorId); }));
        if (adTaskCacheManager.detectorTaskStateExists(detectorId, detectorTaskId)) {
            if (!Objects.equals(adTaskCacheManager.getDetectorTaskState(detectorId, detectorTaskId), newState)) {
                function.execute();
            }
        } else if (!adTaskCacheManager.isHistoricalAnalysisCancelledForHC(detectorId, detectorTaskId)) {
            adTaskManager.getADTask(detectorTaskId, ActionListener.wrap(task -> {
                if (task.isPresent()) {
                    if (!Objects.equals(task.get().getState(), newState)) {
                        function.execute();
                    }
                }
            }, exception -> { logger.error("failed to get detector level task " + detectorTaskId, exception); }));
        }
    }

    private float calculateInitProgress(String taskId) {
        RandomCutForest rcf = adTaskCacheManager.getTRcfModel(taskId).getForest();
        if (rcf == null) {
            return 0.0f;
        }
        float initProgress = (float) rcf.getTotalUpdates() / NUM_MIN_SAMPLES;
        logger.debug("RCF total updates {} for task {}", rcf.getTotalUpdates(), taskId);
        return initProgress > 1.0f ? 1.0f : initProgress;
    }

    private void checkIfADTaskCancelledAndCleanupCache(ADTask adTask) {
        String taskId = adTask.getTaskId();
        String detectorId = adTask.getDetectorId();
        String detectorTaskId = adTask.getDetectorLevelTaskId();
        // refresh latest HC task run time
        adTaskCacheManager.refreshLatestHCTaskRunTime(detectorId);
        if (adTask.getDetector().isMultientityDetector()
            && adTaskCacheManager.isHCTaskCoordinatingNode(detectorId)
            && adTaskCacheManager.isHistoricalAnalysisCancelledForHC(detectorId, detectorTaskId)) {
            // clean up pending and running entity on coordinating node
            adTaskCacheManager.clearPendingEntities(detectorId);
            adTaskCacheManager.removeRunningEntity(detectorId, adTaskManager.convertEntityToString(adTask));
            throw new ADTaskCancelledException(
                adTaskCacheManager.getCancelReasonForHC(detectorId, detectorTaskId),
                adTaskCacheManager.getCancelledByForHC(detectorId, detectorTaskId)
            );
        }

        if (adTaskCacheManager.contains(taskId) && adTaskCacheManager.isCancelled(taskId)) {
            logger.info("AD task cancelled, stop running task {}", taskId);
            String cancelReason = adTaskCacheManager.getCancelReason(taskId);
            String cancelledBy = adTaskCacheManager.getCancelledBy(taskId);
            adTaskCacheManager.remove(taskId, detectorId, detectorTaskId);
            if (!adTaskCacheManager.isHCTaskCoordinatingNode(detectorId)
                && isNullOrEmpty(adTaskCacheManager.getTasksOfDetector(detectorId))) {
                // Clean up historical task cache for HC detector on worker node if no running entity task.
                logger.info("All AD task cancelled, cleanup historical task cache for detector {}", detectorId);
                adTaskCacheManager.removeHistoricalTaskCache(detectorId);
            }

            throw new ADTaskCancelledException(cancelReason, cancelledBy);
        }
    }

}
