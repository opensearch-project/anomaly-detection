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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.task;

import static com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.ad.breaker.MemoryCircuitBreaker.DEFAULT_JVM_HEAP_USAGE_THRESHOLD;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.AGG_NAME_MAX_TIME;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.AGG_NAME_MIN_TIME;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.CURRENT_PIECE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.INIT_PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.TASK_PROGRESS_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.WORKER_NODE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.stats.InternalStatNames.JVM_HEAP_USAGE;
import static com.amazon.opendistroforelasticsearch.ad.stats.StatNames.AD_EXECUTING_BATCH_TASK_COUNT;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ADTaskCancelledException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SinglePointFeatures;
import com.amazon.opendistroforelasticsearch.ad.indices.ADIndex;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchTaskRemoteExecutionAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodesAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultBulkIndexHandler;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.amazon.randomcutforest.RandomCutForest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

public class ADBatchTaskRunner {
    private final Logger logger = LogManager.getLogger(ADBatchTaskRunner.class);

    private final RateLimiter rateLimiter = RateLimiter.create(1);

    private final ThreadPool threadPool;
    private final Client client;
    private final ADStats adStats;
    private final DiscoveryNodeFilterer nodeFilter;
    private final ClusterService clusterService;
    private final FeatureManager featureManager;
    private final ADCircuitBreakerService adCircuitBreakerService;
    private final ADTaskManager adTaskManager;
    private final AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private AnomalyDetectionIndices anomalyDetectionIndices;

    private final ADTaskCacheManager adTaskCacheManager;
    private final TransportRequestOptions option;

    private volatile Integer maxAdBatchTaskPerNode;
    private volatile Integer pieceSize;
    private volatile Integer pieceIntervalSeconds;

    public ADBatchTaskRunner(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        DiscoveryNodeFilterer nodeFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ADCircuitBreakerService adCircuitBreakerService,
        FeatureManager featureManager,
        ADTaskManager adTaskManager,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ADStats adStats,
        AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler,
        ADTaskCacheManager adTaskCacheManager
    ) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.anomalyResultBulkIndexHandler = anomalyResultBulkIndexHandler;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nodeFilter = nodeFilter;
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

        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);

        this.pieceSize = BATCH_TASK_PIECE_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_SIZE, it -> pieceSize = it);

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);
    }

    /**
     * Run AD task.
     * 1. Set AD task state as {@link ADTaskState#INIT}
     * 2. Gather node stats and find node with least load to run AD task.
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param listener action listener
     */
    public void run(ADTask adTask, TransportService transportService, ActionListener<ADBatchAnomalyResultResponse> listener) {
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(STATE_FIELD, ADTaskState.INIT.name());
        updatedFields.put(INIT_PROGRESS_FIELD, 0.0f);

        ActionListener<ADBatchAnomalyResultResponse> delegatedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
            listener.onFailure(e);
            handleException(adTask, e);
        });

        adTaskManager
            .updateADTask(adTask.getTaskId(), updatedFields, ActionListener.wrap(r -> dispatchTask(adTask, ActionListener.wrap(node -> {
                if (clusterService.localNode().getId().equals(node.getId())) {
                    // Execute batch task locally
                    logger
                        .info(
                            "execute AD task {} locally on node {} for detector {}",
                            adTask.getTaskId(),
                            node.getId(),
                            adTask.getDetectorId()
                        );
                    startADBatchTask(adTask, false, transportService, delegatedListener);
                } else {
                    // Execute batch task remotely
                    logger
                        .info(
                            "execute AD task {} remotely on node {} for detector {}",
                            adTask.getTaskId(),
                            node.getId(),
                            adTask.getDetectorId()
                        );
                    transportService
                        .sendRequest(
                            node,
                            ADBatchTaskRemoteExecutionAction.NAME,
                            new ADBatchAnomalyResultRequest(adTask),
                            option,
                            new ActionListenerResponseHandler<>(delegatedListener, ADBatchAnomalyResultResponse::new)
                        );
                }
            }, e -> delegatedListener.onFailure(e))), e -> delegatedListener.onFailure(e)));
    }

    private void dispatchTask(ADTask adTask, ActionListener<DiscoveryNode> listener) {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ADStatsRequest adStatsRequest = new ADStatsRequest(dataNodes);
        adStatsRequest.addAll(ImmutableSet.of(AD_EXECUTING_BATCH_TASK_COUNT.getName(), JVM_HEAP_USAGE.getName()));

        client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
            List<ADStatsNodeResponse> candidateNodeResponse = adStatsResponse
                .getNodes()
                .stream()
                .filter(stat -> (long) stat.getStatsMap().get(JVM_HEAP_USAGE.getName()) < DEFAULT_JVM_HEAP_USAGE_THRESHOLD)
                .collect(Collectors.toList());

            if (candidateNodeResponse.size() == 0) {
                String errorMessage = "All nodes' memory usage exceeds limitation"
                    + DEFAULT_JVM_HEAP_USAGE_THRESHOLD
                    + ". No eligible node to run detector "
                    + adTask.getDetectorId();
                logger.warn(errorMessage);
                listener.onFailure(new LimitExceededException(adTask.getDetectorId(), errorMessage));
                return;
            }
            candidateNodeResponse = candidateNodeResponse
                .stream()
                .filter(stat -> (Long) stat.getStatsMap().get(AD_EXECUTING_BATCH_TASK_COUNT.getName()) < maxAdBatchTaskPerNode)
                .collect(Collectors.toList());
            if (candidateNodeResponse.size() == 0) {
                String errorMessage = "All nodes' executing historical detector count exceeds limitation. No eligible node to run detector "
                    + adTask.getDetectorId();
                logger.warn(errorMessage);
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
    }

    /**
     * Start AD task in dedicated batch task thread pool on worker node.
     *
     * @param adTask ad task
     * @param runTaskRemotely run task remotely or not
     * @param transportService transport service
     * @param listener action listener
     */
    public void startADBatchTask(
        ADTask adTask,
        boolean runTaskRemotely,
        TransportService transportService,
        ActionListener<ADBatchAnomalyResultResponse> listener
    ) {
        try {
            // check if cluster is eligible to run AD currently, if not eligible like
            // circuit breaker open, will throw exception.
            checkClusterState(adTask);
            threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                ActionListener<String> internalListener = internalBatchTaskListener(adTask, transportService);
                try {
                    executeADBatchTask(adTask, internalListener);
                } catch (Exception e) {
                    internalListener.onFailure(e);
                }
            });
            listener.onResponse(new ADBatchAnomalyResultResponse(clusterService.localNode().getId(), runTaskRemotely));
        } catch (Exception e) {
            logger.error("Fail to start AD batch task " + adTask.getTaskId(), e);
            listener.onFailure(e);
        }
    }

    private ActionListener<String> internalBatchTaskListener(ADTask adTask, TransportService transportService) {
        String taskId = adTask.getTaskId();
        ActionListener<String> listener = ActionListener.wrap(response -> {
            // If batch task finished normally, remove task from cache and decrease executing task count by 1.
            adTaskCacheManager.remove(taskId);
            adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();

            adTaskManager
                .cleanDetectorCache(
                    adTask,
                    transportService,
                    () -> adTaskManager.updateADTask(taskId, ImmutableMap.of(STATE_FIELD, ADTaskState.FINISHED.name()))
                );
        }, e -> {
            // If batch task failed, remove task from cache and decrease executing task count by 1.
            adTaskCacheManager.remove(taskId);
            adStats.getStat(AD_EXECUTING_BATCH_TASK_COUNT.getName()).decrement();
            adTaskManager.cleanDetectorCache(adTask, transportService, () -> handleException(adTask, e));
        });
        return listener;
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

    private void executeADBatchTask(ADTask adTask, ActionListener<String> internalListener) {
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
                            adTask.getDetector().getDetectionDateRange().getStartTime().toEpochMilli(),
                            TASK_PROGRESS_FIELD,
                            0.0f,
                            INIT_PROGRESS_FIELD,
                            0.0f,
                            WORKER_NODE_FIELD,
                            clusterService.localNode().getId()
                        ),
                    ActionListener.wrap(r -> {
                        try {
                            checkIfADTaskCancelled(adTask.getTaskId());
                            getDateRangeOfSourceData(adTask, (minDate, maxDate) -> {
                                long interval = ((IntervalTimeConfiguration) adTask.getDetector().getDetectionInterval())
                                    .toDuration()
                                    .toMillis();

                                DetectionDateRange detectionDateRange = adTask.getDetector().getDetectionDateRange();
                                long dataStartTime = detectionDateRange.getStartTime().toEpochMilli();
                                long dataEndTime = detectionDateRange.getEndTime().toEpochMilli();

                                if (minDate >= dataEndTime || maxDate <= dataStartTime) {
                                    internalListener
                                        .onFailure(
                                            new ResourceNotFoundException(
                                                adTask.getDetectorId(),
                                                "There is no data in the detection date range"
                                            )
                                        );
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
                                if ((dataEndTime - dataStartTime) < THRESHOLD_MODEL_TRAINING_SIZE * interval) {
                                    internalListener
                                        .onFailure(
                                            new AnomalyDetectionException("There is no enough data to train model").countedInStats(false)
                                        );
                                    return;
                                }
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

    private void getDateRangeOfSourceData(ADTask adTask, BiConsumer<Long, Long> consumer, ActionListener listener) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.min(AGG_NAME_MIN_TIME).field(adTask.getDetector().getTimeField()))
            .aggregation(AggregationBuilders.max(AGG_NAME_MAX_TIME).field(adTask.getDetector().getTimeField()))
            .size(0);
        SearchRequest request = new SearchRequest()
            .indices(adTask.getDetector().getIndices().toArray(new String[0]))
            .source(searchSourceBuilder);

        client.search(request, ActionListener.wrap(r -> {
            InternalMin minAgg = r.getAggregations().get(AGG_NAME_MIN_TIME);
            InternalMax maxAgg = r.getAggregations().get(AGG_NAME_MAX_TIME);
            double minValue = minAgg.getValue();
            double maxValue = maxAgg.getValue();
            // If time field not exist or there is no value, will return infinity value
            if (minValue == Double.POSITIVE_INFINITY) {
                listener.onFailure(new ResourceNotFoundException(adTask.getDetectorId(), "There is no data in the time field"));
                return;
            }
            consumer.accept((long) minValue, (long) maxValue);
        }, e -> { listener.onFailure(e); }));
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

        featureManager.getFeatureDataPointsByBatch(adTask.getDetector(), pieceStartTime, pieceEndTime, threadedActionListener);
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
        RandomCutForest rcf = adTaskCacheManager.getRcfModel(taskId);
        ThresholdingModel threshold = adTaskCacheManager.getThresholdModel(taskId);
        Deque<Map.Entry<Long, Optional<double[]>>> shingle = adTaskCacheManager.getShingle(taskId);

        List<AnomalyResult> anomalyResults = new ArrayList<>();

        long intervalEndTime = pieceStartTime;
        for (int i = 0; i < pieceSize && intervalEndTime < dataEndTime; i++) {
            Optional<double[]> dataPoint = dataPoints.containsKey(intervalEndTime) ? dataPoints.get(intervalEndTime) : Optional.empty();
            intervalEndTime = intervalEndTime + interval;
            SinglePointFeatures feature = featureManager
                .getShingledFeatureForHistoricalDetector(adTask.getDetector(), shingle, dataPoint, intervalEndTime);
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
                    taskId,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    featureData,
                    Instant.ofEpochMilli(intervalEndTime - interval),
                    Instant.ofEpochMilli(intervalEndTime),
                    executeStartTime,
                    Instant.now(),
                    error,
                    null,
                    adTask.getDetector().getUser(),
                    anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT)
                );
                anomalyResults.add(anomalyResult);
            } else {
                double[] point = feature.getProcessedFeatures().get();
                double score = rcf.getAnomalyScore(point);
                rcf.update(point);
                double grade = 0d;
                double confidence = 0d;
                if (!adTaskCacheManager.isThresholdModelTrained(taskId)) {
                    if (adTaskCacheManager.getThresholdModelTrainingDataSize(taskId) < THRESHOLD_MODEL_TRAINING_SIZE) {
                        if (score > 0) {
                            adTaskCacheManager.addThresholdModelTrainingData(taskId, score);
                        }
                    } else {
                        logger.debug("training threshold model");
                        threshold.train(adTaskCacheManager.getThresholdModelTrainingData(taskId));
                        adTaskCacheManager.setThresholdModelTrained(taskId, true);
                    }
                } else {
                    grade = threshold.grade(score);
                    confidence = threshold.confidence();
                    if (score > 0) {
                        threshold.update(score);
                    }
                }

                AnomalyResult anomalyResult = new AnomalyResult(
                    adTask.getDetectorId(),
                    taskId,
                    score,
                    grade,
                    confidence,
                    featureData,
                    Instant.ofEpochMilli(intervalEndTime - interval),
                    Instant.ofEpochMilli(intervalEndTime),
                    executeStartTime,
                    Instant.now(),
                    null,
                    null,
                    adTask.getDetector().getUser(),
                    anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT)
                );
                anomalyResults.add(anomalyResult);
            }
        }

        anomalyResultBulkIndexHandler
            .bulkIndexAnomalyResult(
                anomalyResults,
                new ThreadedActionListener<>(logger, threadPool, AD_BATCH_TASK_THREAD_POOL_NAME, ActionListener.wrap(r -> {
                    try {
                        runNextPiece(adTask, pieceEndTime, dataStartTime, dataEndTime, interval, internalListener);
                    } catch (Exception e) {
                        internalListener.onFailure(e);
                    }
                }, e -> {
                    logger.error("Fail to bulk index anomaly result", e);
                    internalListener.onFailure(e);
                }), false)
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
        float initProgress = calculateInitProgress(taskId);
        String taskState = initProgress >= 1.0f ? ADTaskState.RUNNING.name() : ADTaskState.INIT.name();

        if (pieceStartTime < dataEndTime) {
            checkClusterState(adTask);
            long expectedPieceEndTime = pieceStartTime + pieceSize * interval;
            long pieceEndTime = expectedPieceEndTime > dataEndTime ? dataEndTime : expectedPieceEndTime;
            int i = 0;
            while (i < pieceIntervalSeconds) {
                // check if task cancelled every second, so frontend can get STOPPED state
                // in 1 second once task cancelled.
                checkIfADTaskCancelled(taskId);
                rateLimiter.acquire(1);
                i++;
            }
            logger.debug("start next piece start from {} to {}, interval {}", pieceStartTime, pieceEndTime, interval);
            float taskProgress = (float) (pieceStartTime - dataStartTime) / (dataEndTime - dataStartTime);
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
        } else {
            logger.info("AD task finished for detector {}, task id: {}", adTask.getDetectorId(), taskId);
            adTaskCacheManager.remove(taskId);
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
                            initProgress
                        ),
                    ActionListener.wrap(r -> internalListener.onResponse("task execution done"), e -> internalListener.onFailure(e))
                );
        }
    }

    private float calculateInitProgress(String taskId) {
        RandomCutForest rcf = adTaskCacheManager.getRcfModel(taskId);
        if (rcf == null) {
            return 0.0f;
        }
        float initProgress = (float) rcf.getTotalUpdates() / NUM_MIN_SAMPLES;
        return initProgress > 1.0f ? 1.0f : initProgress;
    }

    private void checkIfADTaskCancelled(String taskId) {
        if (adTaskCacheManager.contains(taskId) && adTaskCacheManager.isCancelled(taskId)) {
            throw new ADTaskCancelledException(adTaskCacheManager.getCancelReason(taskId), adTaskCacheManager.getCancelledBy(taskId));
        }
    }

}
