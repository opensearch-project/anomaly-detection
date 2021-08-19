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

package org.opensearch.ad.task;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.ad.AnomalyDetectorPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static org.opensearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static org.opensearch.ad.constant.CommonErrorMessages.EXCEED_HISTORICAL_ANALYSIS_LIMIT;
import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG;
import static org.opensearch.ad.constant.CommonErrorMessages.NO_ELIGIBLE_NODE_TO_RUN_DETECTOR;
import static org.opensearch.ad.constant.CommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.model.ADTask.COORDINATING_NODE_FIELD;
import static org.opensearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static org.opensearch.ad.model.ADTask.ERROR_FIELD;
import static org.opensearch.ad.model.ADTask.ESTIMATED_MINUTES_LEFT_FIELD;
import static org.opensearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static org.opensearch.ad.model.ADTask.EXECUTION_START_TIME_FIELD;
import static org.opensearch.ad.model.ADTask.INIT_PROGRESS_FIELD;
import static org.opensearch.ad.model.ADTask.IS_LATEST_FIELD;
import static org.opensearch.ad.model.ADTask.LAST_UPDATE_TIME_FIELD;
import static org.opensearch.ad.model.ADTask.PARENT_TASK_ID_FIELD;
import static org.opensearch.ad.model.ADTask.STATE_FIELD;
import static org.opensearch.ad.model.ADTask.STOPPED_BY_FIELD;
import static org.opensearch.ad.model.ADTask.TASK_PROGRESS_FIELD;
import static org.opensearch.ad.model.ADTask.TASK_TYPE_FIELD;
import static org.opensearch.ad.model.ADTaskState.NOT_ENDED_STATES;
import static org.opensearch.ad.model.ADTaskType.ALL_HISTORICAL_TASK_TYPES;
import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;
import static org.opensearch.ad.model.ADTaskType.REALTIME_TASK_TYPES;
import static org.opensearch.ad.model.ADTaskType.taskTypeToString;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static org.opensearch.ad.model.AnomalyResult.TASK_ID_FIELD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static org.opensearch.ad.stats.InternalStatNames.AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT;
import static org.opensearch.ad.stats.InternalStatNames.AD_USED_BATCH_TASK_SLOT_COUNT;
import static org.opensearch.ad.util.ExceptionUtil.getErrorMessage;
import static org.opensearch.ad.util.ExceptionUtil.getShardsFailure;
import static org.opensearch.ad.util.ParseUtils.isNullOrEmpty;
import static org.opensearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.opensearch.ad.util.RestHandlerUtils.createXContentParserFromRegistry;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.cluster.ADDataMigrator;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.ADTaskCancelledException;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.DuplicateTaskException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADEntityTaskProfile;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import org.opensearch.ad.transport.ADBatchAnomalyResultAction;
import org.opensearch.ad.transport.ADBatchAnomalyResultRequest;
import org.opensearch.ad.transport.ADCancelTaskAction;
import org.opensearch.ad.transport.ADCancelTaskRequest;
import org.opensearch.ad.transport.ADStatsNodeResponse;
import org.opensearch.ad.transport.ADStatsNodesAction;
import org.opensearch.ad.transport.ADStatsRequest;
import org.opensearch.ad.transport.ADTaskProfileAction;
import org.opensearch.ad.transport.ADTaskProfileNodeResponse;
import org.opensearch.ad.transport.ADTaskProfileRequest;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
import org.opensearch.ad.transport.ForwardADTaskAction;
import org.opensearch.ad.transport.ForwardADTaskRequest;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.Script;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Manage AD task.
 */
public class ADTaskManager {
    public static final String AD_TASK_LEAD_NODE_MODEL_ID = "ad_task_lead_node_model_id";
    private final Logger logger = LogManager.getLogger(this.getClass());
    static final String STATE_INDEX_NOT_EXIST_MSG = "State index does not exist.";
    private final Set<String> retryableErrors = ImmutableSet.of(EXCEED_HISTORICAL_ANALYSIS_LIMIT, NO_ELIGIBLE_NODE_TO_RUN_DETECTOR);
    private final Client client;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionIndices detectionIndices;
    private final DiscoveryNodeFilterer nodeFilter;
    private final ADTaskCacheManager adTaskCacheManager;
    private final ADDataMigrator dataMigrator;

    private final HashRing hashRing;
    private volatile Integer maxOldAdTaskDocsPerDetector;
    private volatile Integer pieceIntervalSeconds;
    private volatile boolean deleteADResultWhenDeleteDetector;
    private volatile TransportRequestOptions transportRequestOptions;
    private final ThreadPool threadPool;
    private static int DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS = 5;
    private final SearchFeatureDao searchFeatureDao;
    private final Semaphore checkingTaskSlot;

    private volatile Integer maxAdBatchTaskPerNode;
    private volatile Integer maxRunningEntitiesPerDetector;

    private final Semaphore scaleEntityTaskLane;
    private static final int SCALE_ENTITY_TASK_LANE_INTERVAL_IN_MILLIS = 10_000; // 10 seconds

    public ADTaskManager(
        Settings settings,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        AnomalyDetectionIndices detectionIndices,
        DiscoveryNodeFilterer nodeFilter,
        HashRing hashRing,
        ADTaskCacheManager adTaskCacheManager,
        ThreadPool threadPool,
        ADDataMigrator dataMigrator,
        SearchFeatureDao searchFeatureDao
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.detectionIndices = detectionIndices;
        this.nodeFilter = nodeFilter;
        this.clusterService = clusterService;
        this.adTaskCacheManager = adTaskCacheManager;
        this.hashRing = hashRing;
        this.dataMigrator = dataMigrator;
        this.searchFeatureDao = searchFeatureDao;

        this.maxOldAdTaskDocsPerDetector = MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR, it -> maxOldAdTaskDocsPerDetector = it);

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);

        this.deleteADResultWhenDeleteDetector = DELETE_AD_RESULT_WHEN_DELETE_DETECTOR.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(DELETE_AD_RESULT_WHEN_DELETE_DETECTOR, it -> deleteADResultWhenDeleteDetector = it);

        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);

        this.maxRunningEntitiesPerDetector = MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS, it -> maxRunningEntitiesPerDetector = it);

        transportRequestOptions = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(REQUEST_TIMEOUT.get(settings))
            .build();
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(
                REQUEST_TIMEOUT,
                it -> {
                    transportRequestOptions = TransportRequestOptions
                        .builder()
                        .withType(TransportRequestOptions.Type.REG)
                        .withTimeout(it)
                        .build();
                }
            );
        this.threadPool = threadPool;
        this.checkingTaskSlot = new Semaphore(1);
        this.scaleEntityTaskLane = new Semaphore(1);
    }

    /**
     * Start detector. Will create schedule job for realtime detector,
     * and start AD task for historical detector.
     *
     * @param detectorId detector id
     * @param detectionDateRange historical analysis date range
     * @param handler anomaly detector job action handler
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startDetector(
        String detectorId,
        DetectionDateRange detectionDateRange,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(detectorId, (detector) -> {
            if (validateDetector(detector, listener)) { // validate if detector is ready to start
                if (detectionDateRange == null) {
                    // start realtime job
                    handler.startAnomalyDetectorJob(detector);
                } else {
                    // start historical analysis task
                    forwardApplyForTaskSlotsRequestToLeadNode(detector, detectionDateRange, user, transportService, listener);
                }
            }
        }, listener);
    }

    /**
     * When AD receives start historical analysis request for a detector, will
     * 1. Forward to lead node to check available task slots first.
     * 2. If available task slots exit, will forward request to coordinating node
     *    to gather information like top entities.
     * 3. Then coordinating node will choose one data node with least load as work
     *    node and dispatch historical analysis to it.
     *
     * @param detector detector
     * @param detectionDateRange detection date range
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    protected void forwardApplyForTaskSlotsRequestToLeadNode(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        ForwardADTaskRequest forwardADTaskRequest = new ForwardADTaskRequest(
            detector,
            detectionDateRange,
            user,
            ADTaskAction.APPLY_FOR_TASK_SLOTS,
            hashRing.getAdVersion(clusterService.localNode().getId())
        );
        forwardRequestToLeadNode(forwardADTaskRequest, transportService, listener);
    }

    public void forwardScaleTaskSlotRequestToLeadNode(
        ADTask adTask,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        forwardRequestToLeadNode(new ForwardADTaskRequest(adTask, ADTaskAction.CHECK_AVAILABLE_TASK_SLOTS), transportService, listener);
    }

    public void forwardRequestToLeadNode(
        ForwardADTaskRequest forwardADTaskRequest,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        hashRing.getOwningNodeWithSameLocalAdVersion(AD_TASK_LEAD_NODE_MODEL_ID, node -> {
            if (!node.isPresent()) {
                listener.onFailure(new ResourceNotFoundException("Can't find AD task lead node"));
                return;
            }
            transportService
                .sendRequest(
                    node.get(),
                    ForwardADTaskAction.NAME,
                    forwardADTaskRequest,
                    transportRequestOptions,
                    new ActionListenerResponseHandler<>(listener, AnomalyDetectorJobResponse::new)
                );
        }, listener);
    }

    /**
     * Forward historical analysis task to coordinating node.
     *
     * @param detector anomaly detector
     * @param detectionDateRange historical analysis date range
     * @param user user
     * @param availableTaskSlots available task slots
     * @param transportService transport service
     * @param listener action listener
     */
    public void startHistoricalAnalysis(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        int availableTaskSlots,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String detectorId = detector.getDetectorId();
        hashRing.getOwningNodeWithSameLocalAdVersion(detectorId, owningNode -> {
            if (!owningNode.isPresent()) {
                logger.debug("Can't find eligible node to run as AD task's coordinating node");
                listener.onFailure(new OpenSearchStatusException("No eligible node to run detector", RestStatus.INTERNAL_SERVER_ERROR));
                return;
            }
            logger.debug("coordinating node is : {} for detector: {}", owningNode.get().getId(), detectorId);
            forwardDetectRequestToCoordinatingNode(
                detector,
                detectionDateRange,
                user,
                availableTaskSlots,
                ADTaskAction.START,
                transportService,
                owningNode.get(),
                listener
            );
        }, listener);

    }

    /**
     * We have three types of nodes in AD task process.
     *
     * 1.Forwarding node which receives external request. The request will \
     *   be sent to coordinating node first.
     * 2.Coordinating node which maintains running historical detector set.\
     *   We use hash ring to find coordinating node with detector id. \
     *   Coordinating node will find a worker node with least load and \
     *   dispatch AD task to that worker node.
     * 3.Worker node which will run AD task.
     *
     * This function is to forward the request to coordinating node.
     *
     * @param detector anomaly detector
     * @param detectionDateRange historical analysis date range
     * @param user user
     * @param availableTaskSlots available task slots
     * @param adTaskAction AD task action
     * @param transportService transport service
     * @param node ES node
     * @param listener action listener
     */
    protected void forwardDetectRequestToCoordinatingNode(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        Integer availableTaskSlots,
        ADTaskAction adTaskAction,
        TransportService transportService,
        DiscoveryNode node,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        Version adVersion = hashRing.getAdVersion(node.getId());
        transportService
            .sendRequest(
                node,
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(detector, detectionDateRange, user, adTaskAction, availableTaskSlots, adVersion),
                transportRequestOptions,
                new ActionListenerResponseHandler<>(listener, AnomalyDetectorJobResponse::new)
            );
    }

    /**
     * Forward AD task to coordinating node
     *
     * @param adTask AD task
     * @param adTaskAction AD task action
     * @param transportService transport service
     * @param listener action listener
     */
    protected void forwardADTaskToCoordinatingNode(
        ADTask adTask,
        ADTaskAction adTaskAction,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        logger.debug("Forward AD task to coordinating node, task id: {}, action: {}", adTask.getTaskId(), adTaskAction.name());
        transportService
            .sendRequest(
                getCoordinatingNode(adTask),
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(adTask, adTaskAction),
                transportRequestOptions,
                new ActionListenerResponseHandler<>(listener, AnomalyDetectorJobResponse::new)
            );
    }

    /**
     * Forward stale running entities to coordinating node to clean up.
     *
     * @param adTask AD task
     * @param adTaskAction AD task action
     * @param transportService transport service
     * @param staleRunningEntity stale running entities
     * @param listener action listener
     */
    protected void forwardStaleRunningEntitiesToCoordinatingNode(
        ADTask adTask,
        ADTaskAction adTaskAction,
        TransportService transportService,
        List<String> staleRunningEntity,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        transportService
            .sendRequest(
                getCoordinatingNode(adTask),
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(adTask, adTaskAction, staleRunningEntity),
                transportRequestOptions,
                new ActionListenerResponseHandler<>(listener, AnomalyDetectorJobResponse::new)
            );
    }

    /**
     * Check available task slots before start historical analysis and scale task lane.
     * This check will be done on lead node which will gather detector task slots of all
     * data nodes and calculate how many task slots available.
     *
     * @param adTask AD task
     * @param detector detector
     * @param detectionDateRange detection date range
     * @param user user
     * @param targetActionOfTaskSlotChecking target task action to run after task slot checking
     * @param transportService transport service
     * @param listener action listener
     */
    public void checkTaskSlots(
        ADTask adTask,
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ADTaskAction targetActionOfTaskSlotChecking,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String detectorId = detector.getDetectorId();
        logger.debug("Start checking task slots for detector: {}, task action: {}", detectorId, targetActionOfTaskSlotChecking);
        if (!checkingTaskSlot.tryAcquire()) {
            logger.info("Can't acquire checking task slot semaphore for detector {}", detectorId);
            listener
                .onFailure(
                    new LimitExceededException(detectorId, "Too many historical analysis request in short time. Please retry later.")
                );
            return;
        }
        ActionListener<AnomalyDetectorJobResponse> wrappedActionListener = ActionListener
            .runAfter(listener, () -> { checkingTaskSlot.release(1); });
        hashRing.getNodesWithSameLocalAdVersion(nodes -> {
            int maxAdTaskSlots = nodes.length * maxAdBatchTaskPerNode;
            ADStatsRequest adStatsRequest = new ADStatsRequest(nodes);
            adStatsRequest
                .addAll(ImmutableSet.of(AD_USED_BATCH_TASK_SLOT_COUNT.getName(), AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName()));
            client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
                int totalUsedTaskSlots = 0; // Total entity tasks running on worker nodes
                int totalAssignedTaskSlots = 0; // Total assigned task slots on coordinating nodes
                for (ADStatsNodeResponse response : adStatsResponse.getNodes()) {
                    totalUsedTaskSlots += (int) response.getStatsMap().get(AD_USED_BATCH_TASK_SLOT_COUNT.getName());
                    totalAssignedTaskSlots += (int) response.getStatsMap().get(AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName());
                }
                logger
                    .info(
                        "Current total used task slots is {}, total detector assigned task slots is {} when start historical "
                            + "analysis for detector {}",
                        totalUsedTaskSlots,
                        totalAssignedTaskSlots,
                        detectorId
                    );
                // In happy case, totalAssignedTaskSlots >= totalUsedTaskSlots. If some coordinating node left, then we can't
                // get detector task slots cached on it, so it's possible that totalAssignedTaskSlots < totalUsedTaskSlots.
                int currentUsedTaskSlots = Math.max(totalUsedTaskSlots, totalAssignedTaskSlots);
                if (currentUsedTaskSlots >= maxAdTaskSlots) {
                    wrappedActionListener.onFailure(new LimitExceededException("Total used task slots is more than max task slots"));
                    return;
                }
                int availableAdTaskSlots = maxAdTaskSlots - totalUsedTaskSlots;
                logger.info("Current available task slots is {} for historical analysis of detector {}", availableAdTaskSlots, detectorId);

                if (ADTaskAction.SCALE_ENTITY_TASK_SLOTS == targetActionOfTaskSlotChecking) {
                    forwardToCoordinatingNode(
                        adTask,
                        detector,
                        detectionDateRange,
                        user,
                        targetActionOfTaskSlotChecking,
                        transportService,
                        wrappedActionListener,
                        availableAdTaskSlots
                    );
                    return;
                }

                long dataStartTime = detectionDateRange.getStartTime().toEpochMilli();
                long dataEndTime = detectionDateRange.getEndTime().toEpochMilli();

                if (detector.isMultientityDetector()) {
                    if (detector.isMultiCategoryDetector()) {
                        searchFeatureDao
                            .getHighestCountEntities(
                                detector,
                                dataStartTime,
                                dataEndTime,
                                maxRunningEntitiesPerDetector,
                                1,// Get entities with minimum doc count 1 here to make sure we reserve enough task slots for HC detector
                                maxRunningEntitiesPerDetector,
                                ActionListener.wrap(topEntities -> {
                                    if (topEntities == null || topEntities.size() == 0) {
                                        wrappedActionListener.onFailure(new ResourceNotFoundException("No category found"));
                                        return;
                                    }
                                    int approvedTaskSlots = Math.min(availableAdTaskSlots, topEntities.size());
                                    // Send task to run multi-category HC detector on coordinating node
                                    forwardToCoordinatingNode(
                                        adTask,
                                        detector,
                                        detectionDateRange,
                                        user,
                                        targetActionOfTaskSlotChecking,
                                        transportService,
                                        wrappedActionListener,
                                        approvedTaskSlots
                                    );
                                }, e -> {
                                    logger.error("Failed to get top entities for multi-category HC " + detector.getDetectorId(), e);
                                    wrappedActionListener.onFailure(e);
                                })
                            );
                    } else {
                        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(detector.getTimeField())
                            .gte(dataStartTime)
                            .lte(dataEndTime)
                            .format("epoch_millis");
                        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().filter(rangeQueryBuilder);
                        String topEntitiesAgg = "topEntities";
                        TermsAggregationBuilder agg = new TermsAggregationBuilder(topEntitiesAgg)
                            .field(detector.getCategoryField().get(0))
                            .size(maxRunningEntitiesPerDetector);
                        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).aggregation(agg).size(0);
                        SearchRequest searchRequest = new SearchRequest()
                            .source(sourceBuilder)
                            .indices(detector.getIndices().toArray(new String[0]));
                        client.search(searchRequest, ActionListener.wrap(res -> {
                            StringTerms stringTerms = res.getAggregations().get(topEntitiesAgg);
                            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
                            int topEntitiesSize = buckets.size();
                            if (topEntitiesSize == 0) {
                                wrappedActionListener.onFailure(new ResourceNotFoundException("No category found"));
                                return;
                            }
                            int approvedTaskSlots = Math.min(availableAdTaskSlots, topEntitiesSize);
                            // Send task to run single-category HC detector on coordinating node
                            forwardToCoordinatingNode(
                                adTask,
                                detector,
                                detectionDateRange,
                                user,
                                targetActionOfTaskSlotChecking,
                                transportService,
                                wrappedActionListener,
                                approvedTaskSlots
                            );
                        }, e -> {
                            logger.error("Failed to get top entities for single-category HC " + detector.getDetectorId(), e);
                            wrappedActionListener.onFailure(e);
                        }));
                    }
                } else {
                    // Send task to run single-flow detector on coordinating node
                    forwardToCoordinatingNode(
                        adTask,
                        detector,
                        detectionDateRange,
                        user,
                        targetActionOfTaskSlotChecking,
                        transportService,
                        wrappedActionListener,
                        1
                    );
                }
            }, exception -> {
                logger.error("Failed to get node's task stats for detector " + detectorId, exception);
                wrappedActionListener.onFailure(exception);
            }));
        }, wrappedActionListener);
    }

    private void forwardToCoordinatingNode(
        ADTask adTask,
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ADTaskAction targetActionOfTaskSlotChecking,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> wrappedActionListener,
        int approvedTaskSlots
    ) {
        switch (targetActionOfTaskSlotChecking) {
            case START:
                logger
                    .info(
                        "Will assign {} task slots to run historical analysis for detector {}",
                        approvedTaskSlots,
                        detector.getDetectorId()
                    );
                startHistoricalAnalysis(detector, detectionDateRange, user, approvedTaskSlots, transportService, wrappedActionListener);
                break;
            case SCALE_ENTITY_TASK_SLOTS:
                logger
                    .info(
                        "There are {} task slots available now to scale historical analysis task lane for detector {}",
                        approvedTaskSlots,
                        adTask.getDetectorId()
                    );
                scaleTaskLaneOnCoordinatingNode(adTask, approvedTaskSlots, transportService, wrappedActionListener);
                break;
            default:
                wrappedActionListener.onFailure(new AnomalyDetectionException("Unknown task action " + targetActionOfTaskSlotChecking));
                break;
        }
    }

    private void scaleTaskLaneOnCoordinatingNode(
        ADTask adTask,
        int approvedTaskSlot,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        DiscoveryNode coordinatingNode = getCoordinatingNode(adTask);
        transportService
            .sendRequest(
                coordinatingNode,
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(adTask, approvedTaskSlot, ADTaskAction.SCALE_ENTITY_TASK_SLOTS),
                transportRequestOptions,
                new ActionListenerResponseHandler<>(listener, AnomalyDetectorJobResponse::new)
            );
    }

    private DiscoveryNode getCoordinatingNode(ADTask adTask) {
        String coordinatingNode = adTask.getCoordinatingNode();
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        DiscoveryNode targetNode = null;
        for (DiscoveryNode node : eligibleDataNodes) {
            if (node.getId().equals(coordinatingNode)) {
                targetNode = node;
                break;
            }
        }
        if (targetNode == null) {
            throw new ResourceNotFoundException(adTask.getDetectorId(), "AD task coordinating node not found");
        }
        return targetNode;
    }

    /**
     * Start anomaly detector on coordinating node.
     * Will init task index if not exist and write new AD task to index. If task index
     * exists, will check if there is task running. If no running task, reset old task
     * as not latest and clean old tasks which exceeds max old task doc limitation.
     * Then find out node with least load and dispatch task to that node(worker node).
     *
     * @param detector anomaly detector
     * @param detectionDateRange detection date range
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startDetector(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        try {
            if (detectionIndices.doesDetectorStateIndexExist()) {
                // If detection index exist, check if latest AD task is running
                getAndExecuteOnLatestDetectorLevelTask(detector.getDetectorId(), getADTaskTypes(detectionDateRange), (adTask) -> {
                    if (!adTask.isPresent() || isADTaskEnded(adTask.get())) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        listener.onFailure(new OpenSearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
                    }
                }, transportService, true, listener);
            } else {
                // If detection index doesn't exist, create index and execute detector.
                detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", DETECTION_STATE_INDEX);
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        String error = "Create index " + DETECTION_STATE_INDEX + " with mappings not acknowledged";
                        logger.warn(error);
                        listener.onFailure(new OpenSearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        logger.error("Failed to init anomaly detection state index", e);
                        listener.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            logger.error("Failed to start detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private ADTaskType getADTaskType(AnomalyDetector detector, DetectionDateRange detectionDateRange) {
        if (detectionDateRange == null) {
            return detector.isMultientityDetector() ? ADTaskType.REALTIME_HC_DETECTOR : ADTaskType.REALTIME_SINGLE_FLOW;
        } else {
            return detector.isMultientityDetector() ? ADTaskType.HISTORICAL_HC_DETECTOR : ADTaskType.HISTORICAL_SINGLE_FLOW;
        }
    }

    private List<ADTaskType> getADTaskTypes(DetectionDateRange detectionDateRange) {
        return getADTaskTypes(detectionDateRange, false);
    }

    /**
     * Get list of task types.
     * 1. If detection date range is null, will return all realtime task types
     * 2. If detection date range is not null, will return all historical detector level tasks types
     *    if resetLatestTaskStateFlag is true; otherwise return all historical tasks types include
     *    HC entity level task type.
     * @param detectionDateRange detection date range
     * @param resetLatestTaskStateFlag reset latest task state or not
     * @return list of AD task types
     */
    private List<ADTaskType> getADTaskTypes(DetectionDateRange detectionDateRange, boolean resetLatestTaskStateFlag) {
        if (detectionDateRange == null) {
            return REALTIME_TASK_TYPES;
        } else {
            if (resetLatestTaskStateFlag) {
                // return all task types include HC entity task to make sure we can reset all tasks latest flag
                return ALL_HISTORICAL_TASK_TYPES;
            } else {
                return HISTORICAL_DETECTOR_TASK_TYPES;
            }
        }
    }

    /**
     * Stop detector.
     * For realtime detector, will set detector job as disabled.
     * For historical detector, will set its AD task as cancelled.
     *
     * @param detectorId detector id
     * @param historical stop historical analysis or not
     * @param handler AD job action handler
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void stopDetector(
        String detectorId,
        boolean historical,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(detectorId, (detector) -> {
            if (historical) {
                // stop historical analyis
                getAndExecuteOnLatestDetectorLevelTask(
                    detectorId,
                    HISTORICAL_DETECTOR_TASK_TYPES,
                    (task) -> stopHistoricalAnalysis(detectorId, task, user, listener),
                    transportService,
                    true,
                    listener
                );
            } else {
                // stop realtime detector job
                handler.stopAnomalyDetectorJob(detectorId);
            }
        }, listener);
    }

    /**
     * Get anomaly detector and execute consumer function.
     *
     * @param detectorId detector id
     * @param consumer consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getDetector(String detectorId, Consumer<AnomalyDetector> consumer, ActionListener<T> listener) {
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onFailure(new OpenSearchStatusException(FAIL_TO_FIND_DETECTOR_MSG, RestStatus.NOT_FOUND));
                return;
            }
            try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                consumer.accept(detector);
            } catch (Exception e) {
                String message = "Failed to start anomaly detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    // TODO: delete this one later
    public <T> void getDetector(
        String detectorId,
        Consumer<AnomalyDetector> realTimeDetectorConsumer,
        Consumer<AnomalyDetector> historicalDetectorConsumer,
        ActionListener<T> listener
    ) {
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onFailure(new OpenSearchStatusException(FAIL_TO_FIND_DETECTOR_MSG, RestStatus.NOT_FOUND));
                return;
            }
            try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                if (detector.isRealTimeDetector()) {
                    // run realtime detector
                    realTimeDetectorConsumer.accept(detector);
                } else {
                    // run historical detector
                    historicalDetectorConsumer.accept(detector);
                }
            } catch (Exception e) {
                String message = "Failed to start anomaly detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    /**
     * Get latest AD task and execute consumer function.
     *
     * @param detectorId detector id
     * @param adTaskTypes AD task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAndExecuteOnLatestDetectorLevelTask(
        String detectorId,
        List<ADTaskType> adTaskTypes,
        Consumer<Optional<ADTask>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        getAndExecuteOnLatestADTask(detectorId, null, adTaskTypes, function, transportService, resetTaskState, listener);
    }

    /**
     * Get one latest AD task and execute consumer function.
     *
     * @param detectorId detector id
     * @param entityValue entity value
     * @param adTaskTypes AD task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAndExecuteOnLatestADTask(
        String detectorId,
        String entityValue,
        List<ADTaskType> adTaskTypes,
        Consumer<Optional<ADTask>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        getAndExecuteOnLatestADTasks(detectorId, entityValue, adTaskTypes, (taskList) -> {
            if (taskList != null && taskList.size() > 0) {
                function.accept(Optional.ofNullable(taskList.get(0)));
            } else {
                function.accept(Optional.empty());
            }
        }, transportService, resetTaskState, 1, listener);
    }

    /**
     * Get latest AD tasks and execute consumer function.
     * If resetTaskState is true, will collect latest task's profile data from all data nodes. If no data
     * node running the latest task, will reset the task state as STOPPED; otherwise, check if there is
     * any stale running entities(entity exists in coordinating node cache but no task running on worker
     * node) and clean up.
     *
     * @param detectorId detector id
     * @param entityValue entity value
     * @param adTaskTypes AD task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param size return how many AD tasks
     * @param listener action listener
     * @param <T> response type of action listener
     */
    public <T> void getAndExecuteOnLatestADTasks(
        String detectorId,
        String entityValue,
        List<ADTaskType> adTaskTypes,
        Consumer<List<ADTask>> function,
        TransportService transportService,
        boolean resetTaskState,
        int size,
        ActionListener<T> listener
    ) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        if (adTaskTypes != null && adTaskTypes.size() > 0) {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(adTaskTypes)));
        }
        if (entityValue != null) {
            String path = "entity";
            String entityValueFieldName = path + ".value";
            TermQueryBuilder entityValueFilterQuery = QueryBuilders.termQuery(entityValueFieldName, entityValue);
            NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(path, entityValueFilterQuery, ScoreMode.None);
            query.filter(nestedQueryBuilder);
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query).sort(EXECUTION_START_TIME_FIELD, SortOrder.DESC).size(size);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(DETECTION_STATE_INDEX);

        client.search(searchRequest, ActionListener.wrap(r -> {
            // https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/359#discussion_r558653132
            // getTotalHits will be null when we track_total_hits is false in the query request.
            // Add more checking here to cover some unknown cases.
            List<ADTask> adTasks = new ArrayList<>();
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
                // don't throw exception here as consumer functions need to handle missing task
                // in different way.
                function.accept(adTasks);
                return;
            }

            Iterator<SearchHit> iterator = r.getHits().iterator();
            while (iterator.hasNext()) {
                SearchHit searchHit = iterator.next();
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    ADTask adTask = ADTask.parse(parser, searchHit.getId());
                    adTasks.add(adTask);
                } catch (Exception e) {
                    String message = "Failed to parse AD task for detector " + detectorId + ", task id " + searchHit.getId();
                    logger.error(message, e);
                    listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
                }
            }
            // TODO: check realtime detector job and reset realtime task as stopped.
            if (resetTaskState) {
                resetLatestHistoricalDetectorTaskState(adTasks, function, transportService);
            } else {
                function.accept(adTasks);
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.accept(new ArrayList<>());
            } else {
                logger.error("Failed to search AD task for detector " + detectorId, e);
                listener.onFailure(e);
            }
        }));
    }

    private void resetLatestHistoricalDetectorTaskState(
        List<ADTask> adTasks,
        Consumer<List<ADTask>> function,
        TransportService transportService
    ) {
        List<ADTask> longRunningHistoricalTasks = adTasks
            .stream()
            .filter(t -> t.isHistoricalTask() && !t.isEntityTask() && !isADTaskEnded(t) && lastUpdateTimeExpired(t))
            .collect(Collectors.toList());

        if (longRunningHistoricalTasks.size() > 0) {
            ADTask adTask = longRunningHistoricalTasks.get(0);
            resetHistoricalDetectorTaskState(adTask, () -> function.accept(adTasks), transportService);
        } else {
            function.accept(adTasks);
        }
    }

    private void resetHistoricalDetectorTaskState(ADTask adTask, AnomalyDetectorFunction function, TransportService transportService) {
        // If AD task is still running, but its last updated time not refreshed for 2 piece intervals, we will get
        // task profile to check if it's really running. If task not running, reset state as STOPPED.
        // For example, ES process crashes, then all tasks running on it will stay as running. We can reset the task
        // state when get historical task with get detector API.
        if (!lastUpdateTimeExpired(adTask)) {
            function.execute();
            return;
        }
        String taskId = adTask.getTaskId();
        AnomalyDetector detector = adTask.getDetector();
        getADTaskProfile(adTask, ActionListener.wrap(taskProfile -> {
            if (taskProfile == null || !Objects.equals(taskId, taskProfile.getTaskId())) {
                logger.debug("AD task not found. Reset task state as stopped, task id: {}", adTask.getTaskId());
                // If no node is running this task, reset it as STOPPED.
                resetTaskStateAsStopped(adTask, transportService, false, () -> function.execute());
            } else if (!detector.isMultientityDetector() && taskProfile.getNodeId() == null) {
                logger.debug("AD task not running for single flow detector. Reset task state as stopped, task id: {}", adTask.getTaskId());
                resetTaskStateAsStopped(adTask, transportService, true, () -> function.execute());
            } else if (detector.isMultientityDetector() && isNullOrEmpty(taskProfile.getRunningEntities())) {
                logger.debug("AD task not running for HC detector. Reset task state as stopped, task id: {}", adTask.getTaskId());
                resetTaskStateAsStopped(adTask, transportService, true, () -> function.execute());
            } else {
                // If still running, check if there is any stale running entities and clean them
                function.execute();
                if (ADTaskType.HISTORICAL_HC_DETECTOR.name().equals(adTask.getTaskType())) {
                    // Check if any running entity not run on worker node. If yes, we need to remove it
                    // and poll next entity from pending entity queue and run it.
                    Integer runningEntitiesCount = taskProfile.getRunningEntitiesCount();
                    if (runningEntitiesCount != null && runningEntitiesCount > 0) {
                        List<String> runningTasksInCoordinatingNodeCache = taskProfile.getRunningEntities();
                        List<String> runningTasksOnWorkerNode = new ArrayList<>();
                        if (taskProfile.getEntityTaskProfiles() != null && taskProfile.getEntityTaskProfiles().size() > 0) {
                            taskProfile
                                .getEntityTaskProfiles()
                                .forEach(entryTask -> runningTasksOnWorkerNode.add(convertEntityToString(entryTask.getEntity(), detector)));
                        }

                        if (runningTasksInCoordinatingNodeCache.size() > runningTasksOnWorkerNode.size()) {
                            runningTasksInCoordinatingNodeCache.removeAll(runningTasksOnWorkerNode);
                            forwardStaleRunningEntitiesToCoordinatingNode(
                                adTask,
                                ADTaskAction.CLEAN_STALE_RUNNING_ENTITIES,
                                transportService,
                                runningTasksInCoordinatingNodeCache,
                                ActionListener
                                    .wrap(
                                        res -> logger.debug("Forwarded task to clean stale running entity, task id {}", taskId),
                                        ex -> logger.error("Failed to forward clean stale running entity for task " + taskId, ex)
                                    )
                            );
                        }
                    }
                }
            }
        }, e -> {
            logger.error("Failed to get AD task profile for task " + adTask.getTaskId(), e);
            function.execute();
        }));
    }

    private void stopHistoricalAnalysis(
        String detectorId,
        Optional<ADTask> adTask,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (!adTask.isPresent()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "Detector not started"));
            return;
        }

        if (isADTaskEnded(adTask.get())) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "No running task found"));
            return;
        }

        String taskId = adTask.get().getTaskId();
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        String userName = user == null ? null : user.getName();

        ADCancelTaskRequest cancelTaskRequest = new ADCancelTaskRequest(detectorId, userName, dataNodes);
        client
            .execute(
                ADCancelTaskAction.INSTANCE,
                cancelTaskRequest,
                ActionListener
                    .wrap(response -> { listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK)); }, e -> {
                        logger.error("Failed to cancel AD task " + taskId + ", detector id: " + detectorId, e);
                        listener.onFailure(e);
                    })
            );
    }

    private boolean lastUpdateTimeExpired(ADTask adTask) {
        return adTask.getLastUpdateTime().plus(2 * pieceIntervalSeconds, ChronoUnit.SECONDS).isBefore(Instant.now());
    }

    /**
     * Check if AD task ended.
     *
     * @param adTask AD task
     * @return true if task state is one of STOPPED, FINISHED or FAILED.
     */
    public boolean isADTaskEnded(ADTask adTask) {
        return ADTaskState.STOPPED.name().equals(adTask.getState())
            || ADTaskState.FINISHED.name().equals(adTask.getState())
            || ADTaskState.FAILED.name().equals(adTask.getState());
    }

    private void resetTaskStateAsStopped(
        ADTask adTask,
        TransportService transportService,
        boolean cleanDetectorCache,
        AnomalyDetectorFunction function
    ) {
        String taskId = adTask.getTaskId();
        adTask.setState(ADTaskState.STOPPED.name());
        if (cleanDetectorCache) {
            cleanDetectorCache(adTask, transportService, () -> resetTaskStateAsStopped(adTask, function, taskId));
        } else {
            resetTaskStateAsStopped(adTask, function, taskId);
        }
    }

    private void resetTaskStateAsStopped(ADTask adTask, AnomalyDetectorFunction function, String taskId) {
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
        updateADTask(adTask.getTaskId(), updatedFields, ActionListener.wrap(r -> {
            logger.debug("Reset task state as STOPPED successfully for task {}", taskId);
            if (function != null) {
                function.execute();
            }
            if (ADTaskType.HISTORICAL_HC_DETECTOR.name().equals(adTask.getTaskType())) {
                // Reset running entity tasks as STOPPED
                resetEntityTasksAsStopped(adTask.getTaskId());
            }
        }, e -> logger.error("Failed to update task state as STOPPED for task " + taskId, e)));
    }

    private void resetEntityTasksAsStopped(String detectorTaskId) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(DETECTION_STATE_INDEX);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(PARENT_TASK_ID_FIELD, detectorTaskId));
        query.filter(new TermQueryBuilder(TASK_TYPE_FIELD, ADTaskType.HISTORICAL_HC_ENTITY.name()));
        query.filter(new TermsQueryBuilder(STATE_FIELD, NOT_ENDED_STATES));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        String script = String.format(Locale.ROOT, "ctx._source.%s='%s';", STATE_FIELD, ADTaskState.STOPPED.name());
        updateByQueryRequest.setScript(new Script(script));

        client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (bulkFailures.isEmpty()) {
                logger.debug("Updated {} child entity tasks state for detector task {}", r.getUpdated(), detectorTaskId);
            } else {
                logger.error("Failed to update child entity task's state for detector task {} ", detectorTaskId);
            }
        }, e -> logger.error("Exception happened when update child entity task's state for detector task " + detectorTaskId, e)));
    }

    /**
     * Clean detector cache on coordinating node.
     * If task's coordinating node is still in cluster, will forward stop
     * task request to coordinating node, then coordinating node will
     * remove detector from cache.
     * If task's coordinating node is not in cluster, we don't need to
     * forward stop task request to coordinating node.
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param function will execute it when detector cache cleaned successfully or coordinating node left cluster
     */
    protected void cleanDetectorCache(ADTask adTask, TransportService transportService, AnomalyDetectorFunction function) {
        String coordinatingNode = adTask.getCoordinatingNode();
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        logger.debug("coordinatingNode is: " + coordinatingNode + " for task " + adTask.getTaskId());
        DiscoveryNode targetNode = null;
        for (DiscoveryNode node : eligibleDataNodes) {
            if (node.getId().equals(coordinatingNode)) {
                targetNode = node;
                break;
            }
        }
        if (targetNode != null) {
            logger.debug("coordinatingNode found, will clean detector cache on it, detectorId: " + adTask.getDetectorId());
            forwardDetectRequestToCoordinatingNode(
                adTask.getDetector(),
                adTask.getDetectionDateRange(),
                null,
                null,
                ADTaskAction.FINISHED,
                transportService,
                targetNode,
                ActionListener
                    .wrap(
                        r -> { function.execute(); },
                        e -> { logger.error("Failed to clear detector cache on coordinating node " + coordinatingNode, e); }
                    )
            );
        } else {
            logger
                .warn(
                    "coordinating node"
                        + coordinatingNode
                        + " left cluster for detector "
                        + adTask.getDetectorId()
                        + ", task id "
                        + adTask.getTaskId()
                );
            function.execute();
        }
    }

    /**
     * Get latest historical AD task profile.
     * Will not reset task state in this method.
     *
     * @param detectorId detector id
     * @param transportService transport service
     * @param profile detector profile
     * @param listener action listener
     */
    public void getLatestHistoricalTaskProfile(
        String detectorId,
        TransportService transportService,
        DetectorProfile profile,
        ActionListener<DetectorProfile> listener
    ) {
        getAndExecuteOnLatestADTask(detectorId, null, HISTORICAL_DETECTOR_TASK_TYPES, adTask -> {
            if (adTask.isPresent()) {
                getADTaskProfile(adTask.get(), ActionListener.wrap(adTaskProfile -> {
                    DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                    profileBuilder.adTaskProfile(adTaskProfile);
                    DetectorProfile detectorProfile = profileBuilder.build();
                    detectorProfile.merge(profile);
                    listener.onResponse(detectorProfile);
                }, e -> {
                    logger.error("Failed to get AD task profile for task " + adTask.get().getTaskId(), e);
                    listener.onFailure(e);
                }));
            } else {
                DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                listener.onResponse(profileBuilder.build());
            }
        }, transportService, false, listener);
    }

    /**
     * Get AD task profile.
     * @param adDetectorLevelTask detector level task
     * @param listener action listener
     */
    private void getADTaskProfile(ADTask adDetectorLevelTask, ActionListener<ADTaskProfile> listener) {
        String detectorId = adDetectorLevelTask.getDetectorId();

        hashRing.getAllEligibleDataNodesWithKnownAdVersion(dataNodes -> {
            ADTaskProfileRequest adTaskProfileRequest = new ADTaskProfileRequest(detectorId, dataNodes);
            client.execute(ADTaskProfileAction.INSTANCE, adTaskProfileRequest, ActionListener.wrap(response -> {
                if (response.hasFailures()) {
                    listener.onFailure(response.failures().get(0));
                    return;
                }

                List<ADEntityTaskProfile> adEntityTaskProfiles = new ArrayList<>();
                ADTaskProfile detectorTaskProfile = new ADTaskProfile(adDetectorLevelTask);
                for (ADTaskProfileNodeResponse node : response.getNodes()) {
                    ADTaskProfile taskProfile = node.getAdTaskProfile();
                    if (taskProfile != null) {
                        if (taskProfile.getNodeId() != null) {
                            detectorTaskProfile.setTaskId(taskProfile.getTaskId());
                            detectorTaskProfile.setShingleSize(taskProfile.getShingleSize());
                            detectorTaskProfile.setRcfTotalUpdates(taskProfile.getRcfTotalUpdates());
                            detectorTaskProfile.setThresholdModelTrained(taskProfile.getThresholdModelTrained());
                            detectorTaskProfile.setThresholdModelTrainingDataSize(taskProfile.getThresholdModelTrainingDataSize());
                            detectorTaskProfile.setModelSizeInBytes(taskProfile.getModelSizeInBytes());
                            detectorTaskProfile.setNodeId(taskProfile.getNodeId());
                            detectorTaskProfile.setTotalEntitiesCount(taskProfile.getTotalEntitiesCount());
                            detectorTaskProfile.setDetectorTaskSlots(taskProfile.getDetectorTaskSlots());
                            detectorTaskProfile.setPendingEntitiesCount(taskProfile.getPendingEntitiesCount());
                            detectorTaskProfile.setRunningEntitiesCount(taskProfile.getRunningEntitiesCount());
                            detectorTaskProfile.setRunningEntities(taskProfile.getRunningEntities());
                            detectorTaskProfile.setAdTaskType(taskProfile.getAdTaskType());
                        }
                        if (taskProfile.getEntityTaskProfiles() != null) {
                            adEntityTaskProfiles.addAll(taskProfile.getEntityTaskProfiles());
                        }
                    }
                }
                if (adEntityTaskProfiles != null && adEntityTaskProfiles.size() > 0) {
                    detectorTaskProfile.setEntityTaskProfiles(adEntityTaskProfiles);
                }
                listener.onResponse(detectorTaskProfile);
            }, e -> {
                logger.error("Failed to get task profile for task " + adDetectorLevelTask.getTaskId(), e);
                listener.onFailure(e);
            }));
        }, listener);

    }

    private boolean validateDetector(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        String error = null;
        if (detector.getFeatureAttributes().size() == 0) {
            error = "Can't start detector job as no features configured";
        } else if (detector.getEnabledFeatureIds().size() == 0) {
            error = "Can't start detector job as no enabled features configured";
        }
        if (error != null) {
            listener.onFailure(new OpenSearchStatusException(error, RestStatus.BAD_REQUEST));
            return false;
        }
        return true;
    }

    /**
     * Start historical detector on coordinating node.
     * Will init task index if not exist and write new AD task to index. If task index
     * exists, will check if there is task running. If no running task, reset old task
     * as not latest and clean old tasks which exceeds limitation. Then find out node
     * with least load and dispatch task to that node(worker node).
     *
     * @param detector anomaly detector
     * @param detectionDateRange detection date range
     * @param user user
     * @param availableTaskSLots available task slots
     * @param transportService transport service
     * @param listener action listener
     */
    public void startHistoricalAnalysisTask(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        Integer availableTaskSLots,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        try {
            if (detectionIndices.doesDetectorStateIndexExist()) {
                // If detection index exist, check if latest AD task is running
                getAndExecuteOnLatestDetectorLevelTask(detector.getDetectorId(), HISTORICAL_DETECTOR_TASK_TYPES, (adTask) -> {
                    if (!adTask.isPresent() || isADTaskEnded(adTask.get())) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        listener.onFailure(new OpenSearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
                    }
                }, transportService, true, listener);
            } else {
                // If detection index doesn't exist, create index and execute historical detector.
                detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", DETECTION_STATE_INDEX);
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        String error = "Create index " + DETECTION_STATE_INDEX + " with mappings not acknowledged";
                        logger.warn(error);
                        listener.onFailure(new OpenSearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        logger.error("Failed to init anomaly detection state index", e);
                        listener.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            logger.error("Failed to start historical detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void executeAnomalyDetector(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(DETECTION_STATE_INDEX);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detector.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        // make sure we reset all latest task as false when user switch from single entity to HC, vice versa.
        query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(getADTaskTypes(detectionDateRange, true))));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        String script = String.format(Locale.ROOT, "ctx._source.%s=%s;", IS_LATEST_FIELD, false);
        updateByQueryRequest.setScript(new Script(script));

        client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (bulkFailures.isEmpty()) {
                createNewADTask(detector, detectionDateRange, user, listener);
            } else {
                logger.error("Failed to update old task's state for detector: {}, response: {} ", detector.getDetectorId(), r.toString());
                listener.onFailure(bulkFailures.get(0).getCause());
            }
        }, e -> {
            logger.error("Failed to reset old tasks as not latest for detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }));
    }

    private void createNewADTask(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String userName = user == null ? null : user.getName();
        Instant now = Instant.now();
        String taskType = getADTaskType(detector, detectionDateRange).name();
        ADTask adTask = new ADTask.Builder()
            .detectorId(detector.getDetectorId())
            .detector(detector)
            .isLatest(true)
            .taskType(taskType)
            .executionStartTime(now)
            .taskProgress(0.0f)
            .initProgress(0.0f)
            .state(ADTaskState.CREATED.name())
            .lastUpdateTime(now)
            .startedBy(userName)
            .coordinatingNode(clusterService.localNode().getId())
            .detectionDateRange(detectionDateRange)
            .user(user)
            .build();

        createADTaskDirectly(
            adTask,
            r -> onIndexADTaskResponse(
                r,
                adTask,
                (response, delegatedListener) -> cleanOldAdTaskDocs(response, adTask, delegatedListener),
                listener
            ),
            listener
        );
    }

    /**
     * Create AD task directly without checking index exists of not.
     *
     * @param adTask AD task
     * @param function consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void createADTaskDirectly(ADTask adTask, Consumer<IndexResponse> function, ActionListener<T> listener) {
        IndexRequest request = new IndexRequest(DETECTION_STATE_INDEX);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request
                .source(adTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client.index(request, ActionListener.wrap(r -> function.accept(r), e -> {
                logger.error("Failed to create AD task for detector " + adTask.getDetectorId(), e);
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            logger.error("Failed to create AD task for detector " + adTask.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void onIndexADTaskResponse(
        IndexResponse response,
        ADTask adTask,
        BiConsumer<IndexResponse, ActionListener<AnomalyDetectorJobResponse>> function,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (response == null || response.getResult() != CREATED) {
            String errorMsg = getShardsFailure(response);
            listener.onFailure(new OpenSearchStatusException(errorMsg, response.status()));
            return;
        }
        adTask.setTaskId(response.getId());
        ActionListener<AnomalyDetectorJobResponse> delegatedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
            handleADTaskException(adTask, e);
            if (e instanceof DuplicateTaskException) {
                listener.onFailure(new OpenSearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
            } else {
                listener.onFailure(e);
                adTaskCacheManager.removeDetector(adTask.getDetectorId());
            }
        });
        try {
            // Put detector id in cache. If detector id already in cache, will throw
            // DuplicateTaskException. This is to solve race condition when user send
            // multiple start request for one historical detector.
            if (adTask.isHistoricalTask()) {
                adTaskCacheManager.add(adTask.getDetectorId(), adTask);
            }
        } catch (Exception e) {
            delegatedListener.onFailure(e);
            return;
        }
        if (function != null) {
            function.accept(response, delegatedListener);
        }
    }

    private void cleanOldAdTaskDocs(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> delegatedListener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, adTask.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, false));

        if (adTask.isHistoricalTask()) {
            // If historical task, only delete detector level task. It may take longer time to delete entity tasks.
            // We will delete child task (entity task) of detector level task in hourly cron job.
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(HISTORICAL_DETECTOR_TASK_TYPES)));
        } else {
            // We don't have entity level task for realtime detection, so will delete all tasks.
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(REALTIME_TASK_TYPES)));
        }

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
            .query(query)
            .sort(EXECUTION_START_TIME_FIELD, SortOrder.DESC)
            // Search query "from" starts from 0.
            .from(maxOldAdTaskDocsPerDetector)
            .size(MAX_OLD_AD_TASK_DOCS);
        searchRequest.source(sourceBuilder).indices(DETECTION_STATE_INDEX);
        String detectorId = adTask.getDetectorId();

        deleteTaskDocs(detectorId, searchRequest, () -> {
            if (adTask.isHistoricalTask()) {
                // run batch result action for historical detection
                runBatchResultAction(response, adTask, delegatedListener);
            } else {
                // return response directly for realtime detection
                AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                    response.getId(),
                    response.getVersion(),
                    response.getSeqNo(),
                    response.getPrimaryTerm(),
                    RestStatus.OK
                );
                delegatedListener.onResponse(anomalyDetectorJobResponse);
            }
        }, delegatedListener);
    }

    private <T> void deleteTaskDocs(
        String detectorId,
        SearchRequest searchRequest,
        AnomalyDetectorFunction function,
        ActionListener<T> listener
    ) {
        ActionListener<SearchResponse> searchListener = ActionListener.wrap(r -> {
            Iterator<SearchHit> iterator = r.getHits().iterator();
            if (iterator.hasNext()) {
                BulkRequest bulkRequest = new BulkRequest();
                while (iterator.hasNext()) {
                    SearchHit searchHit = iterator.next();
                    try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        ADTask adTask = ADTask.parse(parser, searchHit.getId());
                        logger.debug("Delete old task: {} of detector: {}", adTask.getTaskId(), adTask.getDetectorId());
                        bulkRequest.add(new DeleteRequest(DETECTION_STATE_INDEX).id(adTask.getTaskId()));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
                client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.wrap(res -> {
                    logger.info("Old AD tasks deleted for detector {}", detectorId);
                    BulkItemResponse[] bulkItemResponses = res.getItems();
                    if (bulkItemResponses != null && bulkItemResponses.length > 0) {
                        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                            if (!bulkItemResponse.isFailed()) {
                                logger.debug("Add detector task into cache. Task id: {}", bulkItemResponse.getId());
                                // add deleted task in cache and delete its child tasks and AD results
                                adTaskCacheManager.addDeletedDetectorTask(bulkItemResponse.getId());
                            }
                        }
                    }
                    // delete child tasks and AD results of this task
                    cleanChildTasksAndADResultsOfDeletedTask();

                    function.execute();
                }, e -> {
                    logger.warn("Failed to clean AD tasks for detector " + detectorId, e);
                    listener.onFailure(e);
                }));
            } else {
                function.execute();
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.execute();
            } else {
                listener.onFailure(e);
            }
        });

        client.search(searchRequest, searchListener);
    }

    /**
     * Poll deleted detector task from cache and delete its child tasks and AD results.
     */
    public void cleanChildTasksAndADResultsOfDeletedTask() {
        if (!adTaskCacheManager.hasDeletedDetectorTask()) {
            return;
        }
        threadPool.schedule(() -> {
            String taskId = adTaskCacheManager.pollDeletedDetectorTask();
            if (taskId == null) {
                return;
            }
            DeleteByQueryRequest deleteADResultsRequest = new DeleteByQueryRequest(ALL_AD_RESULTS_INDEX_PATTERN);
            deleteADResultsRequest.setQuery(new TermsQueryBuilder(TASK_ID_FIELD, taskId));
            client.execute(DeleteByQueryAction.INSTANCE, deleteADResultsRequest, ActionListener.wrap(res -> {
                logger.debug("Successfully deleted AD results of task " + taskId);
                DeleteByQueryRequest deleteChildTasksRequest = new DeleteByQueryRequest(DETECTION_STATE_INDEX);
                deleteChildTasksRequest.setQuery(new TermsQueryBuilder(PARENT_TASK_ID_FIELD, taskId));

                client.execute(DeleteByQueryAction.INSTANCE, deleteChildTasksRequest, ActionListener.wrap(r -> {
                    logger.debug("Successfully deleted child tasks of task " + taskId);
                    cleanChildTasksAndADResultsOfDeletedTask();
                }, e -> { logger.error("Failed to delete child tasks of task " + taskId, e); }));
            }, ex -> { logger.error("Failed to delete AD results for task " + taskId, ex); }));
        }, TimeValue.timeValueSeconds(DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS), AD_BATCH_TASK_THREAD_POOL_NAME);
    }

    private void runBatchResultAction(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                .info(
                    "AD task {} of detector {} dispatched to {} node {}",
                    adTask.getTaskId(),
                    adTask.getDetectorId(),
                    remoteOrLocal,
                    r.getNodeId()
                );
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                response.getId(),
                response.getVersion(),
                response.getSeqNo(),
                response.getPrimaryTerm(),
                RestStatus.OK
            );
            listener.onResponse(anomalyDetectorJobResponse);
        }, e -> listener.onFailure(e)));
    }

    /**
     * Handle exceptions for AD task. Update task state and record error message.
     *
     * @param adTask AD task
     * @param e exception
     */
    public void handleADTaskException(ADTask adTask, Exception e) {
        // TODO: handle timeout exception
        String state = ADTaskState.FAILED.name();
        Map<String, Object> updatedFields = new HashMap<>();
        if (e instanceof DuplicateTaskException) {
            // If user send multiple start detector request, we will meet race condition.
            // Cache manager will put first request in cache and throw DuplicateTaskException
            // for the second request. We will delete the second task.
            logger
                .warn(
                    "There is already one running task for detector, detectorId:"
                        + adTask.getDetectorId()
                        + ". Will delete task "
                        + adTask.getTaskId()
                );
            deleteADTask(adTask.getTaskId());
            return;
        }
        if (e instanceof ADTaskCancelledException) {
            logger.info("AD task cancelled, taskId: {}, detectorId: {}", adTask.getTaskId(), adTask.getDetectorId());
            state = ADTaskState.STOPPED.name();
            String stoppedBy = ((ADTaskCancelledException) e).getCancelledBy();
            if (stoppedBy != null) {
                updatedFields.put(STOPPED_BY_FIELD, stoppedBy);
            }
        } else {
            logger.error("Failed to execute AD batch task, task id: " + adTask.getTaskId() + ", detector id: " + adTask.getDetectorId(), e);
        }
        updatedFields.put(ERROR_FIELD, getErrorMessage(e));
        updatedFields.put(STATE_FIELD, state);
        updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
        updateADTask(adTask.getTaskId(), updatedFields);
    }

    /**
     * Update AD task with specific fields.
     *
     * @param taskId AD task id
     * @param updatedFields updated fields, key: filed name, value: new value
     */
    public void updateADTask(String taskId, Map<String, Object> updatedFields) {
        updateADTask(taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated AD task successfully: {}, task id: {}", response.status(), taskId);
            } else {
                logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task: " + taskId, e); }));
    }

    /**
     * Update AD task for specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateADTask(String taskId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(DETECTION_STATE_INDEX, taskId);
        Map<String, Object> updatedContent = new HashMap<>();
        updatedContent.putAll(updatedFields);
        updatedContent.put(LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
        updateRequest.doc(updatedContent);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.update(updateRequest, listener);
    }

    /**
     * Delete AD task with task id.
     *
     * @param taskId AD task id
     */
    public void deleteADTask(String taskId) {
        deleteADTask(
            taskId,
            ActionListener
                .wrap(
                    r -> { logger.info("Deleted AD task {} with status: {}", taskId, r.status()); },
                    e -> { logger.error("Failed to delete AD task " + taskId, e); }
                )
        );
    }

    /**
     * Delete AD task with task id.
     *
     * @param taskId AD task id
     * @param listener action listener
     */
    public void deleteADTask(String taskId, ActionListener<DeleteResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(DETECTION_STATE_INDEX, taskId);
        client.delete(deleteRequest, listener);
    }

    /**
     * Cancel running task by detector id.
     *
     * @param detectorId detector id
     * @param reason reason to cancel AD task
     * @param userName which user cancel the AD task
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancelLocalTaskByDetectorId(String detectorId, String reason, String userName) {
        ADTaskCancellationState cancellationState = adTaskCacheManager.cancelByDetectorId(detectorId, reason, userName);
        logger
            .debug(
                "Cancelled AD task for detector: {}, state: {}, cancelled by: {}, reason: {}",
                detectorId,
                cancellationState,
                userName,
                reason
            );
        return cancellationState;
    }

    /**
     * Delete AD tasks docs.
     *
     * @param detectorId detector id
     * @param function AD function
     * @param listener action listener
     */
    public void deleteADTasks(String detectorId, AnomalyDetectorFunction function, ActionListener<DeleteResponse> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(DETECTION_STATE_INDEX);

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));

        request.setQuery(query);
        client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(r -> {
            if (r.getBulkFailures() == null || r.getBulkFailures().size() == 0) {
                logger.info("AD tasks deleted for detector {}", detectorId);
                deleteADResultOfDetector(detectorId);
                function.execute();
            } else {
                listener.onFailure(new OpenSearchStatusException("Failed to delete all AD tasks", RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.execute();
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private void deleteADResultOfDetector(String detectorId) {
        if (!deleteADResultWhenDeleteDetector) {
            return;
        }
        DeleteByQueryRequest deleteADResultsRequest = new DeleteByQueryRequest(ALL_AD_RESULTS_INDEX_PATTERN);
        deleteADResultsRequest.setQuery(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        client
            .execute(
                DeleteByQueryAction.INSTANCE,
                deleteADResultsRequest,
                ActionListener
                    .wrap(response -> { logger.debug("Successfully deleted AD results of detector " + detectorId); }, exception -> {
                        logger.error("Failed to delete AD results for detector " + detectorId, exception);
                        adTaskCacheManager.addDeletedDetector(detectorId);
                    })
            );
    }

    /**
     * Clean AD results of deleted detector.
     */
    public void cleanADResultOfDeletedDetector() {
        String detectorId = adTaskCacheManager.pollDeletedDetector();
        if (detectorId != null) {
            deleteADResultOfDetector(detectorId);
        }
    }

    /**
     * Remove detector from cache on coordinating node.
     *
     * @param detectorId detector id
     */
    public void removeDetectorFromCache(String detectorId) {
        adTaskCacheManager.removeDetector(detectorId);
    }

    public void updateLatestRealtimeADTask(
        String detectorId,
        Map<String, Object> updatedFields,
        String newState,
        Float newInitProgress,
        String newError
    ) {
        updateLatestADTask(detectorId, ADTaskType.REALTIME_TASK_TYPES, updatedFields, ActionListener.wrap(r -> {
            logger.debug("Updated latest realtime AD task successfully for detector {}", detectorId);
            adTaskCacheManager.updateRealtimeTaskCache(detectorId, newState, newInitProgress, newError);
        }, e -> { logger.error("Failed to update realtime task for detector " + detectorId, e); }));
    }

    /**
     * Update latest AD task of detector.
     *
     * @param detectorId detector id
     * @param taskTypes task types
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateLatestADTask(
        String detectorId,
        List<ADTaskType> taskTypes,
        Map<String, Object> updatedFields,
        ActionListener<UpdateResponse> listener
    ) {
        getAndExecuteOnLatestDetectorLevelTask(detectorId, taskTypes, (adTask) -> {
            if (adTask.isPresent()) {
                updateADTask(adTask.get().getTaskId(), updatedFields, listener);
            } else {
                listener.onFailure(new ResourceNotFoundException(detectorId, "can't find latest task"));
            }
        }, null, false, listener);
    }

    /**
     * Update latest realtime task.
     *
     * @param detectorId detector id
     * @param state task state
     * @param error error
     * @param listener action listener
     */
    public void updateLatestRealtimeTask(
        String detectorId,
        ADTaskState state,
        Exception error,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getAndExecuteOnLatestDetectorLevelTask(detectorId, REALTIME_TASK_TYPES, (adTask) -> {
            if (adTask.isPresent() && !isADTaskEnded(adTask.get())) {
                Map<String, Object> updatedFields = new HashMap<>();
                updatedFields.put(ADTask.STATE_FIELD, state.name());
                if (error != null) {
                    updatedFields.put(ADTask.ERROR_FIELD, error.getMessage());
                }
                updateADTask(adTask.get().getTaskId(), updatedFields, ActionListener.wrap(r -> {
                    if (error == null) {
                        listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
                    } else {
                        listener.onFailure(error);
                    }
                }, e -> { listener.onFailure(e); }));
            } else {
                listener.onFailure(new OpenSearchStatusException("Anomaly detector job is already stopped: " + detectorId, RestStatus.OK));
            }
        }, null, false, listener);
    }

    /**
     * Set latest realtime task as STOPPED.
     * @param detectorId detector id
     */
    public void stopLatestRealtimeTask(String detectorId) {
        updateLatestRealtimeTask(detectorId, ADTaskState.STOPPED.name(), null, null, null);
    }

    /**
     * Update latest realtime task's state, init progress, estimated left minutes and error field.
     * @param detectorId detector id
     * @param newState new task state which will override state calculated with rcf total updates
     * @param rcfTotalUpdates total updates of RCF model
     * @param detectorIntervalInMinutes detector interval in minutes
     * @param error error message
     */
    public void updateLatestRealtimeTask(
        String detectorId,
        String newState,
        Long rcfTotalUpdates,
        Long detectorIntervalInMinutes,
        String error
    ) {
        updateLatestRealtimeTask(
            detectorId,
            newState,
            rcfTotalUpdates,
            detectorIntervalInMinutes,
            error,
            ActionListener
                .wrap(
                    r -> logger.info("Updated latest realtiem task successfully for " + detectorId),
                    e -> logger.warn("Update latest realtime task failed", e)
                )
        );
    }

    public void updateLatestRealtimeTask(
        String detectorId,
        String newState,
        Long rcfTotalUpdates,
        Long detectorIntervalInMinutes,
        String error,
        ActionListener<UpdateResponse> listener
    ) {
        Float initProgress = null;
        String state = null;
        // calculate init progress and task state with RCF total updates
        if (detectorIntervalInMinutes != null && rcfTotalUpdates != null) {
            state = ADTaskState.INIT.name();
            if (rcfTotalUpdates < NUM_MIN_SAMPLES) {
                initProgress = (float) rcfTotalUpdates / NUM_MIN_SAMPLES;
            } else {
                state = ADTaskState.RUNNING.name();
                initProgress = 1.0f;
            }
        }
        // Check if new state is not null and override state calculated with rcf total updates
        if (newState != null) {
            state = newState;
        }

        error = Optional.ofNullable(error).orElse("");
        if (!adTaskCacheManager.isRealtimeTaskChanged(detectorId, state, initProgress, error)) {
            return;
        }
        if (ADTaskState.STOPPED.name().equals(state)) {
            adTaskCacheManager.removeRealtimeTaskCache(detectorId);
        }
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(COORDINATING_NODE_FIELD, clusterService.localNode().getId());
        if (initProgress != null) {
            updatedFields.put(INIT_PROGRESS_FIELD, initProgress);
            updatedFields.put(ESTIMATED_MINUTES_LEFT_FIELD, Math.max(0, NUM_MIN_SAMPLES - rcfTotalUpdates) * detectorIntervalInMinutes);
        }
        if (state != null) {
            updatedFields.put(STATE_FIELD, state);
        }
        if (error != null) {
            updatedFields.put(ERROR_FIELD, error);
        }
        Float newInitProgress = initProgress;
        String newError = error;
        updateLatestADTask(detectorId, ADTaskType.REALTIME_TASK_TYPES, updatedFields, ActionListener.wrap(r -> {
            logger.debug("Updated latest realtime AD task successfully for detector {}", detectorId);
            adTaskCacheManager.updateRealtimeTaskCache(detectorId, newState, newInitProgress, newError);
            listener.onResponse(r);
        }, e -> {
            logger.error("Failed to update realtime task for detector " + detectorId, e);
            listener.onFailure(e);
        }));
    }

    /**
     * Create realtime task for AD job. This may happen when user migrate from old AD version on or before OpenSearch1.0.
     * @param job AD realtime job
     */
    public void createRealtimeTask(AnomalyDetectorJob job) {
        ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs = new ConcurrentLinkedQueue<>();
        detectorJobs.add(job);
        dataMigrator.backfillRealtimeTask(detectorJobs, false);
    }

    /**
     * Send entity task done message to coordinating node.
     *
     * @param adTask AD task
     * @param exception exception of entity task
     * @param transportService transport service
     */
    protected void entityTaskDone(ADTask adTask, Exception exception, TransportService transportService) {
        entityTaskDone(
            adTask,
            exception,
            transportService,
            ActionListener
                .wrap(
                    r -> logger.debug("AD task forwarded to coordinating node, task id {}", adTask.getTaskId()),
                    e -> logger
                        .error(
                            "AD task failed to forward to coordinating node "
                                + adTask.getCoordinatingNode()
                                + " for task "
                                + adTask.getTaskId(),
                            e
                        )
                )
        );
    }

    private void entityTaskDone(
        ADTask adTask,
        Exception exception,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        try {
            ADTaskAction action = getAdEntityTaskAction(adTask, exception);
            forwardADTaskToCoordinatingNode(adTask, action, transportService, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Get AD entity task action based on exception.
     * 1. If exception is null, return NEXT_ENTITY action which will poll next
     *    entity to run.
     * 2. If exception is retryable, return PUSH_BACK_ENTITY action which will
     *    push entity back to pendig queue.
     * 3. If exception is task cancelled exception, return CANCEL action which
     *    will stop HC detector run.
     *
     * @param adTask AD task
     * @param exception exception
     * @return AD task action
     */
    private ADTaskAction getAdEntityTaskAction(ADTask adTask, Exception exception) {
        ADTaskAction action = ADTaskAction.NEXT_ENTITY;
        if (exception != null) {
            adTask.setError(getErrorMessage(exception));
            if (exception instanceof LimitExceededException && isRetryableError(exception.getMessage())) {
                action = ADTaskAction.PUSH_BACK_ENTITY;
            } else if (exception instanceof ADTaskCancelledException) {
                action = ADTaskAction.CANCEL;
            }
        }
        return action;
    }

    /**
     * Check if error is retryable.
     *
     * @param error error
     * @return retryable or not
     */
    public boolean isRetryableError(String error) {
        if (error == null) {
            return false;
        }
        return retryableErrors.stream().filter(e -> error.contains(e)).findFirst().isPresent();
    }

    /**
     * Set state for HC detector level task when all entities done.
     *
     * The state could be FINISHED,FAILED or STOPPED.
     * 1. If input task state is FINISHED, will check FINISHED entity task count. If
     * there is no FINISHED entity task, will set HC detector level task as FAILED; otherwise
     * set as FINISHED.
     * 2. If input task state is not FINISHED, will set HC detector level task's state as the same.
     *
     * @param adTask AD task
     * @param state AD task state
     * @param listener action listener
     */
    public void setHCDetectorTaskDone(ADTask adTask, ADTaskState state, ActionListener<AnomalyDetectorJobResponse> listener) {
        String detectorId = adTask.getDetectorId();
        String taskId = adTask.isEntityTask() ? adTask.getParentTaskId() : adTask.getTaskId();

        String detectorTaskId = adTask.isEntityTask() ? adTask.getParentTaskId() : adTask.getTaskId();

        ActionListener<UpdateResponse> wrappedListener = ActionListener.wrap(response -> {
            logger.info("Historical HC detector done with state: {}. Remove from cache, detector id:{}", state.name(), detectorId);
            this.removeDetectorFromCache(detectorId);
        }, e -> {
            logger.error("Failed to update task: " + taskId, e);
            this.removeDetectorFromCache(detectorId);
        });

        if (state == ADTaskState.FINISHED) {
            this.countEntityTasksByState(detectorTaskId, ImmutableList.of(ADTaskState.FINISHED), ActionListener.wrap(r -> {
                logger.info("number of finished entity tasks: {}, for detector {}", r, adTask.getDetectorId());
                // Set task as FAILED if no finished entity task; otherwise set as FINISHED
                ADTaskState hcDetectorTaskState = r == 0 ? ADTaskState.FAILED : ADTaskState.FINISHED;
                updateADHCDetectorTask(
                    detectorId,
                    taskId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            hcDetectorTaskState.name(),
                            TASK_PROGRESS_FIELD,
                            1.0,
                            EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli()
                        ),
                    wrappedListener
                );
            }, e -> {
                logger.error("Failed to get finished entity tasks", e);
                updateADHCDetectorTask(
                    detectorId,
                    taskId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            ADTaskState.FAILED.name(),// set as FAILED if fail to get finished entity tasks.
                            TASK_PROGRESS_FIELD,
                            1.0,
                            ERROR_FIELD,
                            getErrorMessage(e),
                            EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli()
                        ),
                    wrappedListener
                );
            }));
        } else {
            updateADHCDetectorTask(
                detectorId,
                taskId,
                ImmutableMap
                    .of(STATE_FIELD, state.name(), ERROR_FIELD, adTask.getError(), EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()),
                wrappedListener
            );
        }

        listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK));
    }

    /**
     * Count entity tasks by state with detector level task id(parent task id).
     *
     * @param detectorTaskId detector level task id
     * @param taskStates task states
     * @param listener action listener
     */
    public void countEntityTasksByState(String detectorTaskId, List<ADTaskState> taskStates, ActionListener<Long> listener) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        queryBuilder.filter(new TermQueryBuilder(PARENT_TASK_ID_FIELD, detectorTaskId));
        if (taskStates != null && taskStates.size() > 0) {
            queryBuilder.filter(new TermsQueryBuilder(STATE_FIELD, taskStates.stream().map(s -> s.name()).collect(Collectors.toList())));
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.size(0);
        sourceBuilder.trackTotalHits(true);
        SearchRequest request = new SearchRequest();
        request.source(sourceBuilder);
        request.indices(DETECTION_STATE_INDEX);
        client.search(request, ActionListener.wrap(r -> {
            TotalHits totalHits = r.getHits().getTotalHits();
            listener.onResponse(totalHits.value);
        }, e -> listener.onFailure(e)));
    }

    /**
     * Update HC detector level task with default action listener. There might be
     * multiple entity tasks update detector task concurrently. So we will check
     * if detector task is updating or not to avoid and can only update if it's
     * not updating now, otherwise it may cause version conflict exception.
     *
     * @param detectorId detector id
     * @param taskId AD task id
     * @param updatedFields updated fields, key: filed name, value: new value
     */
    public void updateADHCDetectorTask(String detectorId, String taskId, Map<String, Object> updatedFields) {
        updateADHCDetectorTask(detectorId, taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated AD task successfully: {}, taskId: {}", response.status(), taskId);
            } else {
                logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task1: " + taskId, e); }));
    }

    /**
     * Update HC detector level task. There might be multiple entity tasks update
     * detector task concurrently. So we will check if detector task is updating
     * or not to avoid and can only update if it's not updating now, otherwise it
     * may cause version conflict exception.
     *
     * @param detectorId detector id
     * @param taskId AD task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateADHCDetectorTask(
        String detectorId,
        String taskId,
        Map<String, Object> updatedFields,
        ActionListener<UpdateResponse> listener
    ) {
        if (adTaskCacheManager.tryAcquireTaskUpdatingSemaphore(detectorId)) {
            try {
                updateADTask(
                    taskId,
                    updatedFields,
                    ActionListener.runAfter(listener, () -> { adTaskCacheManager.releaseTaskUpdatingSemaphore(detectorId); })
                );
            } catch (Exception e) {
                logger.error("Failed to update detector task " + taskId, e);
                adTaskCacheManager.releaseTaskUpdatingSemaphore(detectorId);
            }
        } else {
            boolean taskDone = Objects.equals(ADTaskState.FAILED.name(), updatedFields.get(STATE_FIELD))
                || Objects.equals(ADTaskState.FINISHED.name(), updatedFields.get(STATE_FIELD));
            if (taskDone) {
                // make sure we set correct task state when task finished/failed.
                logger.info("Update HC detector as done, detectorId: {}, taskId:{}", detectorId, taskId);
                updateADTask(taskId, updatedFields, listener);
            } else {
                logger.info("HC detector task is updating, detectorId:{}, taskId:{}", detectorId, taskId);
                listener.onFailure(new AnomalyDetectionException("HC detector task is updating"));
            }

        }
    }

    /**
     * Scale task slots and check the scale delta:
     *   1. If scale delta is negative, that means we need to scale down, will not start next entity.
     *   2. If scale delta is positive, will start next entity in current lane.
     *
     * This method will be called by {@link org.opensearch.ad.transport.ForwardADTaskTransportAction}.
     *
     * @param adTask ad entity task
     * @param transportService transport service
     * @param listener action listener
     */
    public void runNextEntityForHCADHistorical(
        ADTask adTask,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        int scaleDelta = scaleTaskSlots(
            adTask,
            transportService,
            ActionListener
                .wrap(
                    r -> { logger.debug("Scale up task slots done for detector {}, task {}", adTask.getDetectorId(), adTask.getTaskId()); },
                    e -> { logger.error("Failed to scale up task slots for task " + adTask.getTaskId(), e); }
                )
        );
        if (scaleDelta < 0) {
            logger
                .warn(
                    "Need to scale down task slots. Stop current lane for detector {}, task {}",
                    adTask.getDetectorId(),
                    adTask.getTaskId()
                );
            listener.onFailure(new AnomalyDetectionException(adTask.getDetectorId(), "need to scale down task slots"));
            return;
        }
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                .info(
                    "AD entity task {} of detector {} dispatched to {} node {}",
                    adTask.getTaskId(),
                    adTask.getDetectorId(),
                    remoteOrLocal,
                    r.getNodeId()
                );
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                adTask.getDetectorId(),
                0,
                0,
                0,
                RestStatus.OK
            );
            listener.onResponse(anomalyDetectorJobResponse);
        }, e -> { listener.onFailure(e); }));
    }

    /**
     * Scale task slots and return scale delta.
     *   1. If scale delta is positive, will forward scale task slots request to lead node.
     *   2. If scale delta is negative, will decrease detector task slots in cache directly.
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param scaleUpListener action listener
     * @return task slots scale delta
     */
    private int scaleTaskSlots(
        ADTask adTask,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> scaleUpListener
    ) {
        String detectorId = adTask.getDetectorId();
        if (!scaleEntityTaskLane.tryAcquire()) {
            logger.debug("Can't get scaleEntityTaskLane semaphore");
            return 0;
        }
        try {
            int scaleDelta = detectorTaskSlotScaleDelta(detectorId);
            logger.debug("start to scale task slots for detector {} with delta {}", detectorId, scaleDelta);
            if (adTaskCacheManager.getAvailableNewEntityTaskLanes(detectorId) <= 0 && scaleDelta > 0) {
                // scale up to run more entities in parallel
                Instant lastScaleEntityTaskLaneTime = adTaskCacheManager.getLastScaleEntityTaskLaneTime(detectorId);
                if (lastScaleEntityTaskLaneTime == null) {
                    logger.debug("lastScaleEntityTaskLaneTime is null for detector {}", detectorId);
                    scaleEntityTaskLane.release();
                    return 0;
                }
                boolean lastScaleTimeExpired = lastScaleEntityTaskLaneTime
                    .plusMillis(SCALE_ENTITY_TASK_LANE_INTERVAL_IN_MILLIS)
                    .isBefore(Instant.now());
                if (lastScaleTimeExpired) {
                    adTaskCacheManager.refreshLastScaleEntityTaskLaneTime(detectorId);
                    logger.debug("Forward scale entity task lane request to lead node for detector {}", detectorId);
                    forwardScaleTaskSlotRequestToLeadNode(
                        adTask,
                        transportService,
                        ActionListener.runAfter(scaleUpListener, () -> scaleEntityTaskLane.release())
                    );
                } else {
                    logger
                        .debug(
                            "lastScaleEntityTaskLaneTime is not expired yet: {} for detector {}",
                            lastScaleEntityTaskLaneTime,
                            detectorId
                        );
                    scaleEntityTaskLane.release();
                }
            } else {
                if (scaleDelta < 0) { // scale down to release task slots for other detectors
                    int runningEntityCount = adTaskCacheManager.getRunningEntityCount(detectorId) + adTaskCacheManager
                        .getTempEntityCount(detectorId);
                    int assignedTaskSlots = adTaskCacheManager.getDetectorTaskSlots(detectorId);
                    int scaleDownDelta = Math.min(assignedTaskSlots - runningEntityCount, 0 - scaleDelta);
                    logger
                        .debug(
                            "Scale down task slots, scaleDelta: {}, assignedTaskSlots: {}, runningEntityCount: {}, scaleDownDelta: {}",
                            scaleDelta,
                            assignedTaskSlots,
                            runningEntityCount,
                            scaleDownDelta
                        );
                    adTaskCacheManager.scaleDownHCDetectorTaskSlots(detectorId, scaleDownDelta);
                }
                scaleEntityTaskLane.release();
            }
            return scaleDelta;
        } catch (Exception e) {
            logger.error("Failed to forward scale entity task lane request to lead node for detector " + detectorId, e);
            scaleEntityTaskLane.release();
            return 0;
        }
    }

    /**
     * Calculate scale delta for detector task slots.
     * Detector's task lane limit should be less than or equal to:
     *   1. Current unfinished entities: pending + running + temp
     *   2. Total task slots on cluster level: eligible_data_nodes * task_slots_per_node
     *   3. Max running entities per detector which is dynamic setting
     *
     * Task slots scale delta = task lane limit - current assigned task slots
     *
     * If current assigned task slots to this detector is less than task lane limit, we need
     * to scale up(return positive value); otherwise we need to scale down (return negative
     * value).
     *
     * @param detectorId detector id
     * @return detector task slots scale delta
     */
    public int detectorTaskSlotScaleDelta(String detectorId) {
        DiscoveryNode[] eligibleDataNodes = hashRing.getNodesWithSameLocalAdVersionDirectly();
        int unfinishedEntities = adTaskCacheManager.getUnfinishedEntityCount(detectorId);
        int totalTaskSlots = eligibleDataNodes.length * maxAdBatchTaskPerNode;
        int taskLaneLimit = Math.min(unfinishedEntities, Math.min(totalTaskSlots, maxRunningEntitiesPerDetector));
        adTaskCacheManager.setDetectorTaskLaneLimit(detectorId, taskLaneLimit);

        int assignedTaskSlots = adTaskCacheManager.getDetectorTaskSlots(detectorId);
        int scaleDelta = taskLaneLimit - assignedTaskSlots;
        logger
            .debug(
                "Calculate task slot scale delta for detector {}, totalTaskSlots: {}, maxRunningEntitiesPerDetector: {}, "
                    + "unfinishedEntities: {}, taskLaneLimit: {}, assignedTaskSlots: {}, scaleDelta: {}",
                totalTaskSlots,
                maxRunningEntitiesPerDetector,
                unfinishedEntities,
                taskLaneLimit,
                assignedTaskSlots,
                scaleDelta
            );
        return scaleDelta;
    }

    /**
     * Calculate historical analysis task progress of HC detector.
     * task_progress = finished_entity_count / total_entity_count
     * @param detectorId detector id
     * @return task progress
     */
    public float hcDetectorProgress(String detectorId) {
        int entityCount = adTaskCacheManager.getTopEntityCount(detectorId);
        int leftEntities = adTaskCacheManager.getPendingEntityCount(detectorId) + adTaskCacheManager.getRunningEntityCount(detectorId);
        return 1 - (float) leftEntities / entityCount;
    }

    /**
     * Get local task profiles of detector.
     * @param detectorId detector id
     * @return list of AD task profile
     */
    public ADTaskProfile getLocalADTaskProfilesByDetectorId(String detectorId) {
        List<String> tasksOfDetector = adTaskCacheManager.getTasksOfDetector(detectorId);
        ADTaskProfile detectorTaskProfile = null;

        String localNodeId = clusterService.localNode().getId();
        if (adTaskCacheManager.isHCTaskRunning(detectorId)) {
            detectorTaskProfile = new ADTaskProfile();
            if (adTaskCacheManager.isHCTaskCoordinatingNode(detectorId)) {
                detectorTaskProfile.setNodeId(localNodeId);
                detectorTaskProfile.setTaskId(adTaskCacheManager.getDetectorTaskId(detectorId));
                detectorTaskProfile.setDetectorTaskSlots(adTaskCacheManager.getDetectorTaskSlots(detectorId));
                detectorTaskProfile.setTotalEntitiesCount(adTaskCacheManager.getTopEntityCount(detectorId));
                detectorTaskProfile.setPendingEntitiesCount(adTaskCacheManager.getPendingEntityCount(detectorId));
                detectorTaskProfile.setRunningEntitiesCount(adTaskCacheManager.getRunningEntityCount(detectorId));
                detectorTaskProfile.setRunningEntities(adTaskCacheManager.getRunningEntities(detectorId));
                detectorTaskProfile.setAdTaskType(ADTaskType.HISTORICAL_HC_DETECTOR.name());
            }
            if (tasksOfDetector.size() > 0) {
                List<ADEntityTaskProfile> entityTaskProfiles = new ArrayList<>();

                tasksOfDetector.forEach(taskId -> {
                    ADEntityTaskProfile entityTaskProfile = new ADEntityTaskProfile(
                        adTaskCacheManager.getShingle(taskId).size(),
                        adTaskCacheManager.getRcfModel(taskId).getTotalUpdates(),
                        adTaskCacheManager.isThresholdModelTrained(taskId),
                        adTaskCacheManager.getThresholdModelTrainingDataSize(taskId),
                        adTaskCacheManager.getModelSize(taskId),
                        localNodeId,
                        adTaskCacheManager.getEntity(taskId),
                        taskId,
                        ADTaskType.HISTORICAL_HC_ENTITY.name()
                    );
                    entityTaskProfiles.add(entityTaskProfile);
                });
                detectorTaskProfile.setEntityTaskProfiles(entityTaskProfiles);
            }
        } else {
            if (tasksOfDetector.size() > 1) {
                String error = "Multiple tasks are running for detector: "
                    + detectorId
                    + ". You can stop detector to kill all running tasks.";
                logger.error(error);
                throw new LimitExceededException(error);
            }
            if (tasksOfDetector.size() == 1) {
                String taskId = tasksOfDetector.get(0);
                detectorTaskProfile = new ADTaskProfile(
                    adTaskCacheManager.getDetectorTaskId(detectorId),
                    adTaskCacheManager.getShingle(taskId).size(),
                    adTaskCacheManager.getRcfModel(taskId).getTotalUpdates(),
                    adTaskCacheManager.isThresholdModelTrained(taskId),
                    adTaskCacheManager.getThresholdModelTrainingDataSize(taskId),
                    adTaskCacheManager.getModelSize(taskId),
                    localNodeId
                );
                // Single-flow detector only has 1 task slot.
                // Can't use adTaskCacheManager.getDetectorTaskSlots(detectorId) here as task may run on worker node.
                // Detector task slots stored in coordinating node cache.
                detectorTaskProfile.setDetectorTaskSlots(1);
            }
        }
        return detectorTaskProfile;
    }

    /**
     * Remove stale running entity from coordinating node cache. If no more entities, reset task as STOPPED.
     *
     * Explain details with an example.
     *
     * Note:
     *    CN: coordinating mode;
     *    WN1: worker node 1;
     *    WN2: worker node 2.
     *    [x,x] means running entity in cache.
     *    eX like e1: entity.
     *
     * Assume HC detector can run 2 entities at most and current cluster state is:
     *     CN: [e1, e2];
     *     WN1: [e1]
     *     WN2: [e2]
     *
     * If WN1 crashes, then e1 will never removed from CN cache. User can call get detector API with "task=true"
     * to reset task state. Let's say User1 and User2 call get detector API at the same time. Then User1 and User2
     * both know e1 is stale running entity and try to remove from CN cache. If User1 request arrives first, then
     * it will remove e1 from CN, then CN cache will be [e2]. As we can run 2 entities per HC detector, so we can
     * kick off another pending entity. Then CN cache changes to [e2, e3]. Then User2 request arrives, it will find
     * e1 not in CN cache ([e2, e3]) which means e1 has been removed by other request. We can't kick off another
     * pending entity for User2 request, otherwise we will run more than 2 entities for this HC detector.
     *
     * Why we don't put the stale running entity back to pending and retry?
     * The stale entity has been ran on some worker node and the old task run may generate some or all AD results
     * for the stale entity. Just because of the worker node crash or entity task done message not received by
     * coordinating node, the entity will never be deleted from running entity queue. We can check if the stale
     * entity has all AD results generated for the whole date range. If not, we can rerun. This make the design
     * complex as we need to store model checkpoints to resume from last break point and we need to handle kinds
     * of edge cases. Here we just ignore the stale running entity rather than rerun it. We plan to add scheduler
     * on historical analysis, then we will store model checkpoints. Will support resuming failed tasks by then.
     * //TODO: support resuming failed task
     *
     * @param adTask AD task
     * @param entity entity value
     * @param transportService transport service
     * @param listener action listener
     */
    public synchronized void removeStaleRunningEntity(
        ADTask adTask,
        String entity,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String detectorId = adTask.getDetectorId();
        boolean removed = adTaskCacheManager.removeRunningEntity(detectorId, entity);
        if (removed && adTaskCacheManager.getPendingEntityCount(detectorId) > 0) {
            logger.debug("kick off next pending entities");
            this.runNextEntityForHCADHistorical(adTask, transportService, listener);
        } else {
            if (!adTaskCacheManager.hasEntity(detectorId)) {
                setHCDetectorTaskDone(adTask, ADTaskState.STOPPED, listener);
            }
        }
    }

    public boolean skipUpdateHCRealtimeTask(String detectorId, String error) {
        ADRealtimeTaskCache realtimeTaskCache = adTaskCacheManager.getRealtimeTaskCache(detectorId);
        return realtimeTaskCache != null
            && realtimeTaskCache.getInitProgress() == 1.0
            && Objects.equals(error, realtimeTaskCache.getError());
    }

    /**
     * Maintain running historical tasks.
     * Search current running latest tasks, then maintain tasks one by one.
     * Get task profile to check if task is really running on worker node.
     * 1. If not running, reset task state as STOPPED.
     * 2. If task is running and task for HC detector, check if there is any stale running entities and
     *    clean up.
     *
     * @param transportService transport service
     * @param requestId request id
     * @param size return how many tasks
     */
    public void maintainRunningHistoricalTasks(TransportService transportService, String requestId, int size) {
        // request id could be null, `+ ""` is for backward compatibility consideration
        // Find owning node with highest AD version to make sure we only have 1 node maintain running historical tasks
        // and we use the latest logic.
        Optional<DiscoveryNode> owningNode = hashRing.getOwningNodeWithHighestAdVersion(requestId + "");
        if (!owningNode.isPresent() || !clusterService.localNode().getId().equals(owningNode.get().getId())) {
            return;
        }
        logger.info("Start to maintain running historical tasks");

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(HISTORICAL_DETECTOR_TASK_TYPES)));
        query.filter(new TermsQueryBuilder(STATE_FIELD, NOT_ENDED_STATES));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // default maintain interval is 5 seconds, so maintain 10 tasks will take at least 50 seconds.
        sourceBuilder.query(query).sort(LAST_UPDATE_TIME_FIELD, SortOrder.DESC).size(size);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(DETECTION_STATE_INDEX);

        client.search(searchRequest, ActionListener.wrap(r -> {
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
                return;
            }
            ConcurrentLinkedQueue<ADTask> taskQueue = new ConcurrentLinkedQueue<>();
            Iterator<SearchHit> iterator = r.getHits().iterator();
            while (iterator.hasNext()) {
                SearchHit searchHit = iterator.next();
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    taskQueue.add(ADTask.parse(parser, searchHit.getId()));
                } catch (Exception e) {
                    logger.error("Maintaining running task: failed to parse AD task " + searchHit.getId(), e);
                }
            }
            maintainRunningHistoricalTask(taskQueue, transportService);
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                // the method will be called hourly
                // don't log stack trace as most of OpenSearch domains have no AD installed
                logger.debug(STATE_INDEX_NOT_EXIST_MSG);
            } else {
                logger.error("Failed to search historical tasks in maintaining job", e);
            }
        }));
    }

    private void maintainRunningHistoricalTask(ConcurrentLinkedQueue<ADTask> taskQueue, TransportService transportService) {
        ADTask adTask = taskQueue.poll();
        if (adTask == null) {
            return;
        }
        threadPool.schedule(() -> {
            resetHistoricalDetectorTaskState(adTask, () -> {
                logger.debug("Finished maintaining running historical task {}", adTask.getTaskId());
                maintainRunningHistoricalTask(taskQueue, transportService);
            }, transportService);
        }, TimeValue.timeValueSeconds(DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS), AD_BATCH_TASK_THREAD_POOL_NAME);
    }

    /**
     * Maintain running realtime tasks:
     * 1. check if realtime time job enabled or not, if job not enabled or not found, reset latest
     *    realtime task as STOPPED and remove realtime task cache.
     * 2. If AD job index doesn't exist, reset all running realtime tasks for all detectors as STOPPED
     *    and clear all realtime tasks cache.
     */
    public void maintainRunningRealtimeTasks() {
        threadPool.schedule(() -> {
            String[] detectorsInRealtimeTaskCache = adTaskCacheManager.getDetectorIdsInRealtimeTaskCache();
            // Default maintain interval is 5 seconds, so 100 detectors will take 500 seconds at least.
            int maxDetectorsToMaintain = Math.min(100, detectorsInRealtimeTaskCache.length);
            if (maxDetectorsToMaintain == 0) {
                logger.debug("No running realtime tasks to maintain");
                return;
            }
            ConcurrentLinkedQueue<String> detectorIdQueue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < maxDetectorsToMaintain; i++) {
                detectorIdQueue.add(detectorsInRealtimeTaskCache[i]);
            }
            maintainRunningRealtimeTask(detectorIdQueue);
        }, TimeValue.timeValueSeconds(ThreadLocalRandom.current().nextLong(2, 60)), AD_BATCH_TASK_THREAD_POOL_NAME);
    }

    private void maintainRunningRealtimeTask(ConcurrentLinkedQueue<String> detectorIdQueue) {
        String detectorId = detectorIdQueue.poll();
        if (detectorId == null) {
            return;
        }
        String logPrefix = "[maintainRunningRealtimeTask]";
        threadPool.schedule(() -> {
            GetRequest getJobRequest = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX, detectorId);
            client.get(getJobRequest, ActionListener.wrap(r -> {
                if (r.isExists()) {
                    try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                        if (!job.isEnabled()) {
                            logger
                                .debug(
                                    "{} Maintain running realtime task: AD job is disabled, reset realtime task as stopped for detector {}",
                                    logPrefix,
                                    detectorId
                                );
                            stopLatestRealtimeTask(detectorId);
                        } else {
                            logger.debug("{} AD job is running for detector {}", logPrefix, detectorId);
                        }
                    } catch (IOException e) {
                        logger.error(logPrefix + " Failed to parse AD job " + detectorId, e);
                    }
                } else {
                    logger.debug("{} AD job is not found, reset realtime task as stopped for detector {}", logPrefix, detectorId);
                    stopLatestRealtimeTask(detectorId);
                }
                maintainRunningRealtimeTask(detectorIdQueue);
            }, e -> {
                if (e instanceof IndexNotFoundException) {
                    logger.debug(logPrefix + " AD job index deleted");
                    adTaskCacheManager.clearRealtimeTaskCache();
                    resetAllRealtimeTasksAsStopped();
                } else {
                    // Stop poll next detector in case any exception happen to avoid adding more load to cluster
                    logger.error(logPrefix + " Failed to get detector job for detector " + detectorId, e);
                }
            }));
        }, TimeValue.timeValueSeconds(DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS), AD_BATCH_TASK_THREAD_POOL_NAME);
    }

    private void resetAllRealtimeTasksAsStopped() {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(DETECTION_STATE_INDEX);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(REALTIME_TASK_TYPES)));
        query.filter(new TermsQueryBuilder(STATE_FIELD, NOT_ENDED_STATES));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        String script = String.format(Locale.ROOT, "ctx._source.%s='%s';", STATE_FIELD, ADTaskState.STOPPED.name());
        updateByQueryRequest.setScript(new Script(script));

        client
            .execute(
                UpdateByQueryAction.INSTANCE,
                updateByQueryRequest,
                ActionListener
                    .wrap(
                        r -> { logger.info("Set all running realtime tasks as stopped"); },
                        e -> logger.error("Exception happened when set all realtime tasks as stopped ", e)
                    )
            );
    }

    public String convertEntityToString(ADTask adTask) {
        if (adTask == null || !adTask.isEntityTask()) {
            return null;
        }
        AnomalyDetector detector = adTask.getDetector();
        return convertEntityToString(adTask.getEntity(), detector);
    }

    /**
     * Convert {@link Entity} instance to string.
     * @param entity entity
     * @param detector detector
     * @return entity string value
     */
    public String convertEntityToString(Entity entity, AnomalyDetector detector) {
        if (detector.isMultiCategoryDetector()) {
            try {
                XContentBuilder builder = entity.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
                return BytesReference.bytes(builder).utf8ToString();
            } catch (IOException e) {
                String error = "Failed to parse entity into string";
                logger.debug(error, e);
                throw new AnomalyDetectionException(error);
            }
        }
        if (detector.isMultientityDetector()) {
            String categoryField = detector.getCategoryField().get(0);
            return entity.getAttributes().get(categoryField);
        }
        return null;
    }

    /**
     * Parse entity string value into Entity {@link Entity} instance.
     * @param entityValue entity value
     * @param adTask AD task
     * @return Entity instance
     */
    public Entity parseEntityFromString(String entityValue, ADTask adTask) {
        AnomalyDetector detector = adTask.getDetector();
        if (detector.isMultiCategoryDetector()) {
            try {
                XContentParser parser = XContentType.JSON
                    .xContent()
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, entityValue);
                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
                return Entity.parse(parser);
            } catch (IOException e) {
                String error = "Failed to parse string into entity";
                logger.debug(error, e);
                throw new AnomalyDetectionException(error);
            }
        } else if (detector.isMultientityDetector()) {
            return Entity.createSingleAttributeEntity(detector.getCategoryField().get(0), entityValue);
        }
        throw new IllegalArgumentException("Fail to parse to Entity for single flow detector");
    }

    /**
     * Get AD task with task id and execute listener.
     * @param taskId task id
     * @param listener action listener
     */
    public void getADTask(String taskId, ActionListener<Optional<ADTask>> listener) {
        GetRequest request = new GetRequest(DETECTION_STATE_INDEX, taskId);
        client.get(request, ActionListener.wrap(r -> {
            if (r != null && r.isExists()) {
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    ADTask adTask = ADTask.parse(parser, r.getId());
                    listener.onResponse(Optional.ofNullable(adTask));
                } catch (Exception e) {
                    logger.error("Failed to parse AD task " + r.getId(), e);
                    listener.onFailure(e);
                }
            } else {
                listener.onResponse(Optional.empty());
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                listener.onResponse(Optional.empty());
            } else {
                logger.error("Failed to get AD task " + taskId, e);
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Set old AD task's latest flag as false.
     * @param adTasks list of AD tasks
     */
    public void resetLatestFlagAsFalse(List<ADTask> adTasks) {
        if (adTasks == null || adTasks.size() == 0) {
            return;
        }
        BulkRequest bulkRequest = new BulkRequest();
        adTasks.forEach(task -> {
            try {
                task.setLatest(false);
                task.setLastUpdateTime(Instant.now());
                IndexRequest indexRequest = new IndexRequest(DETECTION_STATE_INDEX)
                    .id(task.getTaskId())
                    .source(task.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE));
                bulkRequest.add(indexRequest);
            } catch (Exception e) {
                logger.error("Fail to parse task AD task to XContent, task id " + task.getTaskId(), e);
            }
        });

        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.wrap(res -> {
            BulkItemResponse[] bulkItemResponses = res.getItems();
            if (bulkItemResponses != null && bulkItemResponses.length > 0) {
                for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                    if (!bulkItemResponse.isFailed()) {
                        logger.warn("Reset AD tasks latest flag as false Successfully. Task id: {}", bulkItemResponse.getId());
                    } else {
                        logger.warn("Failed to reset AD tasks latest flag as false. Task id: " + bulkItemResponse.getId());
                    }
                }
            }
        }, e -> { logger.warn("Failed to reset AD tasks latest flag as false", e); }));
    }

    public int getLocalAdUsedBatchTaskSlot() {
        return adTaskCacheManager.getTotalBatchTaskCount();
    }

    public int getLocalAdAssignedBatchTaskSlot() {
        return adTaskCacheManager.getTotalDetectorTaskSlots();
    }

}
