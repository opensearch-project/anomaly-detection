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

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.ad.AnomalyDetectorPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static org.opensearch.ad.constant.CommonErrorMessages.CAN_NOT_FIND_LATEST_TASK;
import static org.opensearch.ad.constant.CommonErrorMessages.CREATE_INDEX_NOT_ACKNOWLEDGED;
import static org.opensearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static org.opensearch.ad.constant.CommonErrorMessages.EXCEED_HISTORICAL_ANALYSIS_LIMIT;
import static org.opensearch.ad.constant.CommonErrorMessages.HC_DETECTOR_TASK_IS_UPDATING;
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
import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.ADTaskCancelledException;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.DuplicateTaskException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADEntityTaskProfile;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
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
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.Script;
import org.opensearch.search.SearchHit;
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
    public static final String AD_TASK_MAINTAINENCE_NODE_MODEL_ID = "ad_task_maintainence_node_model_id";
    // HC batch task timeout after 10 minutes if no update after last known run time.
    public static final int HC_BATCH_TASK_CACHE_TIMEOUT_IN_MILLIS = 600_000;
    private final Logger logger = LogManager.getLogger(this.getClass());
    static final String STATE_INDEX_NOT_EXIST_MSG = "State index does not exist.";
    private final Set<String> retryableErrors = ImmutableSet.of(EXCEED_HISTORICAL_ANALYSIS_LIMIT, NO_ELIGIBLE_NODE_TO_RUN_DETECTOR);
    private final Client client;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionIndices detectionIndices;
    private final DiscoveryNodeFilterer nodeFilter;
    private final ADTaskCacheManager adTaskCacheManager;

    private final HashRing hashRing;
    private volatile Integer maxOldAdTaskDocsPerDetector;
    private volatile Integer pieceIntervalSeconds;
    private volatile boolean deleteADResultWhenDeleteDetector;
    private volatile TransportRequestOptions transportRequestOptions;
    private final ThreadPool threadPool;
    private static int DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS = 5;
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
        ThreadPool threadPool
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.detectionIndices = detectionIndices;
        this.nodeFilter = nodeFilter;
        this.clusterService = clusterService;
        this.adTaskCacheManager = adTaskCacheManager;
        this.hashRing = hashRing;

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
    // public void startDetector(
    // String detectorId,
    // DetectionDateRange detectionDateRange,
    // IndexAnomalyDetectorJobActionHandler handler,
    // UserIdentity user,
    // TransportService transportService,
    // ActionListener<AnomalyDetectorJobResponse> listener
    // ) {
    // // upgrade index mapping of AD default indices
    // detectionIndices.update();
    //
    // getDetector(detectorId, (detector) -> {
    // if (!detector.isPresent()) {
    // listener.onFailure(new OpenSearchStatusException(FAIL_TO_FIND_DETECTOR_MSG + detectorId, RestStatus.NOT_FOUND));
    // return;
    // }
    //
    // // Validate if detector is ready to start. Will return null if ready to start.
    // String errorMessage = validateDetector(detector.get());
    // if (errorMessage != null) {
    // listener.onFailure(new OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST));
    // return;
    // }
    // String resultIndex = detector.get().getResultIndex();
    // if (resultIndex == null) {
    // startRealtimeOrHistoricalDetection(detectionDateRange, handler, user, transportService, listener, detector);
    // return;
    // }
    // detectionIndices
    // .initCustomResultIndexAndExecute(
    // resultIndex,
    // () -> startRealtimeOrHistoricalDetection(detectionDateRange, handler, user, transportService, listener, detector),
    // listener
    // );
    //
    // }, listener);
    // }

    // private void startRealtimeOrHistoricalDetection(
    // DetectionDateRange detectionDateRange,
    // IndexAnomalyDetectorJobActionHandler handler,
    // UserIdentity user,
    // TransportService transportService,
    // ActionListener<AnomalyDetectorJobResponse> listener,
    // Optional<AnomalyDetector> detector
    // ) {
    // try {
    // if (detectionDateRange == null) {
    // // start realtime job
    // handler.startAnomalyDetectorJob(detector.get());
    // } else {
    // // start historical analysis task
    // forwardApplyForTaskSlotsRequestToLeadNode(detector.get(), detectionDateRange, user, transportService, listener);
    // }
    // } catch (Exception e) {
    // logger.error("Failed to stash context", e);
    // listener.onFailure(e);
    // }
    // }

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
        UserIdentity user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        ForwardADTaskRequest forwardADTaskRequest = new ForwardADTaskRequest(
            detector,
            detectionDateRange,
            user,
            ADTaskAction.APPLY_FOR_TASK_SLOTS
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
        hashRing.buildAndGetOwningNodeWithSameLocalAdVersion(AD_TASK_LEAD_NODE_MODEL_ID, node -> {
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
        UserIdentity user,
        int availableTaskSlots,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String detectorId = detector.getDetectorId();
        hashRing.buildAndGetOwningNodeWithSameLocalAdVersion(detectorId, owningNode -> {
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
        UserIdentity user,
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
                // We need to check AD version of remote node as we may send clean detector cache request to old
                // node, check ADTaskManager#cleanDetectorCache.
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
     * @param afterCheckAction target task action to run after task slot checking
     * @param transportService transport service
     * @param listener action listener
     */
    public void checkTaskSlots(
        ADTask adTask,
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        UserIdentity user,
        ADTaskAction afterCheckAction,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String detectorId = detector.getDetectorId();
        logger.debug("Start checking task slots for detector: {}, task action: {}", detectorId, afterCheckAction);
        if (!checkingTaskSlot.tryAcquire()) {
            logger.info("Can't acquire checking task slot semaphore for detector {}", detectorId);
            listener
                .onFailure(
                    new OpenSearchStatusException(
                        "Too many historical analysis requests in short time. Please retry later.",
                        RestStatus.FORBIDDEN
                    )
                );
            return;
        }
        ActionListener<AnomalyDetectorJobResponse> wrappedActionListener = ActionListener.runAfter(listener, () -> {
            checkingTaskSlot.release(1);
            logger.debug("Release checking task slot semaphore on lead node for detector {}", detectorId);
        });
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
                    wrappedActionListener.onFailure(new OpenSearchStatusException("No available task slot", RestStatus.BAD_REQUEST));
                    return;
                }
                int availableAdTaskSlots = maxAdTaskSlots - currentUsedTaskSlots;
                logger.info("Current available task slots is {} for historical analysis of detector {}", availableAdTaskSlots, detectorId);

                if (ADTaskAction.SCALE_ENTITY_TASK_SLOTS == afterCheckAction) {
                    forwardToCoordinatingNode(
                        adTask,
                        detector,
                        detectionDateRange,
                        user,
                        afterCheckAction,
                        transportService,
                        wrappedActionListener,
                        availableAdTaskSlots
                    );
                    return;
                }

                // It takes long time to check top entities especially for multi-category HC. Tested with
                // 1.8 billion docs for multi-category HC, it took more than 20 seconds and caused timeout.
                // By removing top entity check, it took about 200ms to return. So just remove it to make
                // sure REST API can return quickly.
                // We may assign more task slots. For example, cluster has 4 data nodes, each node can run 2
                // batch tasks, so the available task slot number is 8. If max running entities per HC is 4,
                // then we will assign 4 tasks slots to this HC detector (4 is less than 8). The data index
                // only has 2 entities. So we assign 2 more task slots than actual need. But it's ok as we
                // will auto tune task slot when historical analysis task starts.
                int approvedTaskSlots = detector.isMultientityDetector()
                    ? Math.min(maxRunningEntitiesPerDetector, availableAdTaskSlots)
                    : 1;
                forwardToCoordinatingNode(
                    adTask,
                    detector,
                    detectionDateRange,
                    user,
                    afterCheckAction,
                    transportService,
                    wrappedActionListener,
                    approvedTaskSlots
                );
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
        UserIdentity user,
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

    protected void scaleTaskLaneOnCoordinatingNode(
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
     * Start anomaly detector.
     * For historical analysis, this method will be called on coordinating node.
     * For realtime task, we won't know AD job coordinating node until AD job starts. So
     * this method will be called on vanilla node.
     *
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
        UserIdentity user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        try {
            if (detectionIndices.doesDetectorStateIndexExist()) {
                // If detection index exist, check if latest AD task is running
                getAndExecuteOnLatestDetectorLevelTask(detector.getDetectorId(), getADTaskTypes(detectionDateRange), (adTask) -> {
                    if (!adTask.isPresent() || adTask.get().isDone()) {
                        updateLatestFlagOfOldTasksAndCreateNewTask(detector, detectionDateRange, user, listener);
                    } else {
                        listener.onFailure(new OpenSearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
                    }
                }, transportService, true, listener);
            } else {
                // If detection index doesn't exist, create index and execute detector.
                detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", DETECTION_STATE_INDEX);
                        updateLatestFlagOfOldTasksAndCreateNewTask(detector, detectionDateRange, user, listener);
                    } else {
                        String error = String.format(Locale.ROOT, CREATE_INDEX_NOT_ACKNOWLEDGED, DETECTION_STATE_INDEX);
                        logger.warn(error);
                        listener.onFailure(new OpenSearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        updateLatestFlagOfOldTasksAndCreateNewTask(detector, detectionDateRange, user, listener);
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
            return detector.isMultientityDetector() ? ADTaskType.REALTIME_HC_DETECTOR : ADTaskType.REALTIME_SINGLE_ENTITY;
        } else {
            return detector.isMultientityDetector() ? ADTaskType.HISTORICAL_HC_DETECTOR : ADTaskType.HISTORICAL_SINGLE_ENTITY;
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
    // public void stopDetector(
    // String detectorId,
    // boolean historical,
    // IndexAnomalyDetectorJobActionHandler handler,
    // UserIdentity user,
    // TransportService transportService,
    // ActionListener<AnomalyDetectorJobResponse> listener
    // ) {
    // getDetector(detectorId, (detector) -> {
    // if (!detector.isPresent()) {
    // listener.onFailure(new OpenSearchStatusException(FAIL_TO_FIND_DETECTOR_MSG + detectorId, RestStatus.NOT_FOUND));
    // return;
    // }
    // if (historical) {
    // // stop historical analyis
    // getAndExecuteOnLatestDetectorLevelTask(
    // detectorId,
    // HISTORICAL_DETECTOR_TASK_TYPES,
    // (task) -> stopHistoricalAnalysis(detectorId, task, user, listener),
    // transportService,
    // false,// don't need to reset task state when stop detector
    // listener
    // );
    // } else {
    // // stop realtime detector job
    // handler.stopAnomalyDetectorJob(detectorId);
    // }
    // }, listener);
    // }

    /**
     * Get anomaly detector and execute consumer function.
     * [Important!] Make sure listener returns in function
     *
     * @param detectorId detector id
     * @param function consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getDetector(String detectorId, Consumer<Optional<AnomalyDetector>> function, ActionListener<T> listener) {
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                function.accept(Optional.empty());
                return;
            }
            try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                function.accept(Optional.of(detector));
            } catch (Exception e) {
                String message = "Failed to parse anomaly detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> {
            logger.error("Failed to get detector " + detectorId, exception);
            listener.onFailure(exception);
        }));
    }

    /**
     * Get latest AD task and execute consumer function.
     * [Important!] Make sure listener returns in function
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
        getAndExecuteOnLatestADTask(detectorId, null, null, adTaskTypes, function, transportService, resetTaskState, listener);
    }

    /**
     * Get one latest AD task and execute consumer function.
     * [Important!] Make sure listener returns in function
     *
     * @param detectorId detector id
     * @param parentTaskId parent task id
     * @param entity entity value
     * @param adTaskTypes AD task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAndExecuteOnLatestADTask(
        String detectorId,
        String parentTaskId,
        Entity entity,
        List<ADTaskType> adTaskTypes,
        Consumer<Optional<ADTask>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        getAndExecuteOnLatestADTasks(detectorId, parentTaskId, entity, adTaskTypes, (taskList) -> {
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
     * [Important!] Make sure listener returns in function
     *
     * @param detectorId detector id
     * @param parentTaskId parent task id
     * @param entity entity value
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
        String parentTaskId,
        Entity entity,
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
        if (parentTaskId != null) {
            query.filter(new TermQueryBuilder(PARENT_TASK_ID_FIELD, parentTaskId));
        }
        if (adTaskTypes != null && adTaskTypes.size() > 0) {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(adTaskTypes)));
        }
        if (entity != null && !isNullOrEmpty(entity.getAttributes())) {
            String path = "entity";
            String entityKeyFieldName = path + ".name";
            String entityValueFieldName = path + ".value";

            for (Map.Entry<String, String> attribute : entity.getAttributes().entrySet()) {
                BoolQueryBuilder entityBoolQuery = new BoolQueryBuilder();
                TermQueryBuilder entityKeyFilterQuery = QueryBuilders.termQuery(entityKeyFieldName, attribute.getKey());
                TermQueryBuilder entityValueFilterQuery = QueryBuilders.termQuery(entityValueFieldName, attribute.getValue());

                entityBoolQuery.filter(entityKeyFilterQuery).filter(entityValueFilterQuery);
                NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(path, entityBoolQuery, ScoreMode.None);
                query.filter(nestedQueryBuilder);
            }
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
            if (resetTaskState) {
                resetLatestDetectorTaskState(adTasks, function, transportService, listener);
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

    /**
     * Reset latest detector task state. Will reset both historical and realtime tasks.
     * [Important!] Make sure listener returns in function
     *
     * @param adTasks ad tasks
     * @param function consumer function
     * @param transportService transport service
     * @param listener action listener
     * @param <T> response type of action listener
     */
    private <T> void resetLatestDetectorTaskState(
        List<ADTask> adTasks,
        Consumer<List<ADTask>> function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        List<ADTask> runningHistoricalTasks = new ArrayList<>();
        List<ADTask> runningRealtimeTasks = new ArrayList<>();
        for (ADTask adTask : adTasks) {
            if (!adTask.isEntityTask() && !adTask.isDone()) {
                if (!adTask.isHistoricalTask()) {
                    // try to reset task state if realtime task is not ended
                    runningRealtimeTasks.add(adTask);
                } else {
                    // try to reset task state if historical task not updated for 2 piece intervals
                    runningHistoricalTasks.add(adTask);
                }
            }
        }

        // resetHistoricalDetectorTaskState(
        // runningHistoricalTasks,
        // () -> resetRealtimeDetectorTaskState(runningRealtimeTasks, () -> function.accept(adTasks), transportService, listener),
        // transportService,
        // listener
        // );
    }

    // private <T> void resetRealtimeDetectorTaskState(
    // List<ADTask> runningRealtimeTasks,
    // AnomalyDetectorFunction function,
    // TransportService transportService,
    // ActionListener<T> listener
    // ) {
    // if (isNullOrEmpty(runningRealtimeTasks)) {
    // function.execute();
    // return;
    // }
    // ADTask adTask = runningRealtimeTasks.get(0);
    // String detectorId = adTask.getDetectorId();
    // GetRequest getJobRequest = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);
    // client.get(getJobRequest, ActionListener.wrap(r -> {
    // if (r.isExists()) {
    // try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
    // ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
    // AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
    // if (!job.isEnabled()) {
    // logger.debug("AD job is disabled, reset realtime task as stopped for detector {}", detectorId);
    // resetTaskStateAsStopped(adTask, function, transportService, listener);
    // } else {
    // function.execute();
    // }
    // } catch (IOException e) {
    // logger.error(" Failed to parse AD job " + detectorId, e);
    // listener.onFailure(e);
    // }
    // } else {
    // logger.debug("AD job is not found, reset realtime task as stopped for detector {}", detectorId);
    // resetTaskStateAsStopped(adTask, function, transportService, listener);
    // }
    // }, e -> {
    // logger.error("Fail to get AD realtime job for detector " + detectorId, e);
    // listener.onFailure(e);
    // }));
    // }

    private <T> void resetHistoricalDetectorTaskState(
        List<ADTask> runningHistoricalTasks,
        AnomalyDetectorFunction function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        if (isNullOrEmpty(runningHistoricalTasks)) {
            function.execute();
            return;
        }
        ADTask adTask = runningHistoricalTasks.get(0);
        // If AD task is still running, but its last updated time not refreshed for 2 piece intervals, we will get
        // task profile to check if it's really running. If task not running, reset state as STOPPED.
        // For example, ES process crashes, then all tasks running on it will stay as running. We can reset the task
        // state when get historical task with get detector API.
        if (!lastUpdateTimeOfHistoricalTaskExpired(adTask)) {
            function.execute();
            return;
        }
        String taskId = adTask.getTaskId();
        AnomalyDetector detector = adTask.getDetector();
        getADTaskProfile(adTask, ActionListener.wrap(taskProfile -> {
            boolean taskStopped = isTaskStopped(taskId, detector, taskProfile);
            if (taskStopped) {
                logger.debug("Reset task state as stopped, task id: {}", adTask.getTaskId());
                if (taskProfile.getTaskId() == null // This means coordinating node doesn't have HC detector cache
                    && detector.isMultientityDetector()
                    && !isNullOrEmpty(taskProfile.getEntityTaskProfiles())) {
                    // If coordinating node restarted, HC detector cache on it will be gone. But worker node still
                    // runs entity tasks, we'd better stop these entity tasks to clean up resource earlier.
                    stopHistoricalAnalysis(adTask.getDetectorId(), Optional.of(adTask), null, ActionListener.wrap(r -> {
                        logger.debug("Restop detector successfully");
                        resetTaskStateAsStopped(adTask, function, transportService, listener);
                    }, e -> {
                        logger.error("Failed to restop detector ", e);
                        listener.onFailure(e);
                    }));
                } else {
                    resetTaskStateAsStopped(adTask, function, transportService, listener);
                }
            } else {
                function.execute();
                // If still running, check if there is any stale running entities and clean them
                if (ADTaskType.HISTORICAL_HC_DETECTOR.name().equals(adTask.getTaskType())) {
                    // Check if any running entity not run on worker node. If yes, we need to remove it
                    // and poll next entity from pending entity queue and run it.
                    if (!isNullOrEmpty(taskProfile.getRunningEntities()) && hcBatchTaskExpired(taskProfile.getLatestHCTaskRunTime())) {
                        List<String> runningTasksInCoordinatingNodeCache = new ArrayList<>(taskProfile.getRunningEntities());
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

    private boolean isTaskStopped(String taskId, AnomalyDetector detector, ADTaskProfile taskProfile) {
        String detectorId = detector.getDetectorId();
        if (taskProfile == null || !Objects.equals(taskId, taskProfile.getTaskId())) {
            logger.debug("AD task not found for task {} detector {}", taskId, detectorId);
            // If no node is running this task, reset it as STOPPED.
            return true;
        }
        if (!detector.isMultientityDetector() && taskProfile.getNodeId() == null) {
            logger.debug("AD task not running for single entity detector {}, task {}", detectorId, taskId);
            return true;
        }
        if (detector.isMultientityDetector()
            && taskProfile.getTotalEntitiesInited()
            && isNullOrEmpty(taskProfile.getRunningEntities())
            && isNullOrEmpty(taskProfile.getEntityTaskProfiles())
            && hcBatchTaskExpired(taskProfile.getLatestHCTaskRunTime())) {
            logger.debug("AD task not running for HC detector {}, task {}", detectorId, taskId);
            return true;
        }
        return false;
    }

    public boolean hcBatchTaskExpired(Long latestHCTaskRunTime) {
        if (latestHCTaskRunTime == null) {
            return true;
        }
        return latestHCTaskRunTime + HC_BATCH_TASK_CACHE_TIMEOUT_IN_MILLIS < Instant.now().toEpochMilli();
    }

    private void stopHistoricalAnalysis(
        String detectorId,
        Optional<ADTask> adTask,
        UserIdentity user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (!adTask.isPresent()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "Detector not started"));
            return;
        }

        if (adTask.get().isDone()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "No running task found"));
            return;
        }

        String taskId = adTask.get().getTaskId();
        DiscoveryNode[] dataNodes = hashRing.getNodesWithSameLocalAdVersion();
        String userName = user == null ? null : user.getName();

        ADCancelTaskRequest cancelTaskRequest = new ADCancelTaskRequest(detectorId, taskId, userName, dataNodes);
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

    private boolean lastUpdateTimeOfHistoricalTaskExpired(ADTask adTask) {
        // Wait at least 10 seconds. Piece interval seconds is dynamic setting, user could change it to a smaller value.
        int waitingTime = Math.max(2 * pieceIntervalSeconds, 10);
        return adTask.getLastUpdateTime().plus(waitingTime, ChronoUnit.SECONDS).isBefore(Instant.now());
    }

    private <T> void resetTaskStateAsStopped(
        ADTask adTask,
        AnomalyDetectorFunction function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        cleanDetectorCache(adTask, transportService, () -> {
            String taskId = adTask.getTaskId();
            Map<String, Object> updatedFields = ImmutableMap.of(STATE_FIELD, ADTaskState.STOPPED.name());
            updateADTask(taskId, updatedFields, ActionListener.wrap(r -> {
                adTask.setState(ADTaskState.STOPPED.name());
                if (function != null) {
                    function.execute();
                }
                // For realtime anomaly detection, we only create detector level task, no entity level realtime task.
                if (ADTaskType.HISTORICAL_HC_DETECTOR.name().equals(adTask.getTaskType())) {
                    // Reset running entity tasks as STOPPED
                    resetEntityTasksAsStopped(taskId);
                }
            }, e -> {
                logger.error("Failed to update task state as STOPPED for task " + taskId, e);
                listener.onFailure(e);
            }));
        }, listener);
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
            if (isNullOrEmpty(bulkFailures)) {
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
     * [Important!] Make sure listener returns in function
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param function will execute it when detector cache cleaned successfully or coordinating node left cluster
     * @param listener action listener
     * @param <T> response type of listener
     */
    public <T> void cleanDetectorCache(
        ADTask adTask,
        TransportService transportService,
        AnomalyDetectorFunction function,
        ActionListener<T> listener
    ) {
        String coordinatingNode = adTask.getCoordinatingNode();
        String detectorId = adTask.getDetectorId();
        String taskId = adTask.getTaskId();
        try {
            forwardADTaskToCoordinatingNode(
                adTask,
                ADTaskAction.CLEAN_CACHE,
                transportService,
                ActionListener.wrap(r -> { function.execute(); }, e -> {
                    logger.error("Failed to clear detector cache on coordinating node " + coordinatingNode, e);
                    listener.onFailure(e);
                })
            );
        } catch (ResourceNotFoundException e) {
            logger
                .warn(
                    "Task coordinating node left cluster, taskId: {}, detectorId: {}, coordinatingNode: {}",
                    taskId,
                    detectorId,
                    coordinatingNode
                );
            function.execute();
        } catch (Exception e) {
            logger.error("Failed to forward clean cache event for detector " + detectorId + ", task " + taskId, e);
            listener.onFailure(e);
        }
    }

    protected void cleanDetectorCache(ADTask adTask, TransportService transportService, AnomalyDetectorFunction function) {
        String detectorId = adTask.getDetectorId();
        String taskId = adTask.getTaskId();
        cleanDetectorCache(
            adTask,
            transportService,
            function,
            ActionListener
                .wrap(
                    r -> { logger.debug("Successfully cleaned cache for detector {}, task {}", detectorId, taskId); },
                    e -> { logger.error("Failed to clean cache for detector " + detectorId + ", task " + taskId, e); }
                )
        );
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
        getAndExecuteOnLatestADTask(detectorId, null, null, HISTORICAL_DETECTOR_TASK_TYPES, adTask -> {
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
                            // HC detector: task profile from coordinating node
                            // Single entity detector: task profile from worker node
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

    private String validateDetector(AnomalyDetector detector) {
        String error = null;
        if (detector.getFeatureAttributes().size() == 0) {
            error = "Can't start detector job as no features configured";
        } else if (detector.getEnabledFeatureIds().size() == 0) {
            error = "Can't start detector job as no enabled features configured";
        }
        return error;
    }

    private void updateLatestFlagOfOldTasksAndCreateNewTask(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        UserIdentity user,
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
                // Realtime AD coordinating node is chosen by job scheduler, we won't know it until realtime AD job
                // runs. Just set realtime AD coordinating node as null here, and AD job runner will reset correct
                // coordinating node once realtime job starts.
                // For historical analysis, this method will be called on coordinating node, so we can set coordinating
                // node as local node.
                String coordinatingNode = detectionDateRange == null ? null : clusterService.localNode().getId();
                createNewADTask(detector, detectionDateRange, user, coordinatingNode, listener);
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
        UserIdentity user,
        String coordinatingNode,
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
            .coordinatingNode(coordinatingNode)
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
     * [Important!] Make sure listener returns in function
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
                // For historical AD task, clear historical task if any other exception happened.
                // For realtime AD, task cache will be inited when realtime job starts, check
                // ADTaskManager#initRealtimeTaskCacheAndCleanupStaleCache for details. Here the
                // realtime task cache not inited yet when create AD task, so no need to cleanup.
                if (adTask.isHistoricalTask()) {
                    adTaskCacheManager.removeHistoricalTaskCache(adTask.getDetectorId());
                }
                listener.onFailure(e);
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

    protected <T> void deleteTaskDocs(
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
     * @param detectorTaskId detector level task id
     * @param reason reason to cancel AD task
     * @param userName which user cancel the AD task
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancelLocalTaskByDetectorId(String detectorId, String detectorTaskId, String reason, String userName) {
        ADTaskCancellationState cancellationState = adTaskCacheManager.cancelByDetectorId(detectorId, detectorTaskId, reason, userName);
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
     * [Important!] Make sure listener returns in function
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
            logger.info("Failed to delete AD tasks for " + detectorId, e);
            if (e instanceof IndexNotFoundException) {
                deleteADResultOfDetector(detectorId);
                function.execute();
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private void deleteADResultOfDetector(String detectorId) {
        if (!deleteADResultWhenDeleteDetector) {
            logger.info("Won't delete ad result for {} as delete AD result setting is disabled", detectorId);
            return;
        }
        logger.info("Start to delete AD results of detector {}", detectorId);
        DeleteByQueryRequest deleteADResultsRequest = new DeleteByQueryRequest(ALL_AD_RESULTS_INDEX_PATTERN);
        deleteADResultsRequest.setQuery(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        client
            .execute(
                DeleteByQueryAction.INSTANCE,
                deleteADResultsRequest,
                ActionListener
                    .wrap(response -> { logger.debug("Successfully deleted AD results of detector " + detectorId); }, exception -> {
                        logger.error("Failed to delete AD results of detector " + detectorId, exception);
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
                listener.onFailure(new ResourceNotFoundException(detectorId, CAN_NOT_FIND_LATEST_TASK));
            }
        }, null, false, listener);
    }

    /**
     * Update latest realtime task.
     *
     * @param detectorId detector id
     * @param state task state
     * @param error error
     * @param transportService transport service
     * @param listener action listener
     */
    public void stopLatestRealtimeTask(
        String detectorId,
        ADTaskState state,
        Exception error,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getAndExecuteOnLatestDetectorLevelTask(detectorId, REALTIME_TASK_TYPES, (adTask) -> {
            if (adTask.isPresent() && !adTask.get().isDone()) {
                Map<String, Object> updatedFields = new HashMap<>();
                updatedFields.put(ADTask.STATE_FIELD, state.name());
                if (error != null) {
                    updatedFields.put(ADTask.ERROR_FIELD, error.getMessage());
                }
                AnomalyDetectorFunction function = () -> updateADTask(adTask.get().getTaskId(), updatedFields, ActionListener.wrap(r -> {
                    if (error == null) {
                        listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
                    } else {
                        listener.onFailure(error);
                    }
                }, e -> { listener.onFailure(e); }));

                String coordinatingNode = adTask.get().getCoordinatingNode();
                if (coordinatingNode != null && transportService != null) {
                    cleanDetectorCache(adTask.get(), transportService, function, listener);
                } else {
                    function.execute();
                }
            } else {
                listener.onFailure(new OpenSearchStatusException("Anomaly detector job is already stopped: " + detectorId, RestStatus.OK));
            }
        }, null, false, listener);
    }

    /**
     * Update realtime task cache on realtime detector's coordinating node.
     *
     * @param detectorId detector id
     * @param state new state
     * @param rcfTotalUpdates rcf total updates
     * @param detectorIntervalInMinutes detector interval in minutes
     * @param error error
     * @param listener action listener
     */
    public void updateLatestRealtimeTaskOnCoordinatingNode(
        String detectorId,
        String state,
        Long rcfTotalUpdates,
        Long detectorIntervalInMinutes,
        String error,
        ActionListener<UpdateResponse> listener
    ) {
        Float initProgress = null;
        String newState = null;
        // calculate init progress and task state with RCF total updates
        if (detectorIntervalInMinutes != null && rcfTotalUpdates != null) {
            newState = ADTaskState.INIT.name();
            if (rcfTotalUpdates < NUM_MIN_SAMPLES) {
                initProgress = (float) rcfTotalUpdates / NUM_MIN_SAMPLES;
            } else {
                newState = ADTaskState.RUNNING.name();
                initProgress = 1.0f;
            }
        }
        // Check if new state is not null and override state calculated with rcf total updates
        if (state != null) {
            newState = state;
        }

        error = Optional.ofNullable(error).orElse("");
        if (!adTaskCacheManager.isRealtimeTaskChanged(detectorId, newState, initProgress, error)) {
            // If task not changed, no need to update, just return
            listener.onResponse(null);
            return;
        }
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(COORDINATING_NODE_FIELD, clusterService.localNode().getId());
        if (initProgress != null) {
            updatedFields.put(INIT_PROGRESS_FIELD, initProgress);
            updatedFields.put(ESTIMATED_MINUTES_LEFT_FIELD, Math.max(0, NUM_MIN_SAMPLES - rcfTotalUpdates) * detectorIntervalInMinutes);
        }
        if (newState != null) {
            updatedFields.put(STATE_FIELD, newState);
        }
        if (error != null) {
            updatedFields.put(ERROR_FIELD, error);
        }
        Float finalInitProgress = initProgress;
        // Variable used in lambda expression should be final or effectively final
        String finalError = error;
        String finalNewState = newState;
        updateLatestADTask(detectorId, ADTaskType.REALTIME_TASK_TYPES, updatedFields, ActionListener.wrap(r -> {
            logger.debug("Updated latest realtime AD task successfully for detector {}", detectorId);
            adTaskCacheManager.updateRealtimeTaskCache(detectorId, finalNewState, finalInitProgress, finalError);
            listener.onResponse(r);
        }, e -> {
            logger.error("Failed to update realtime task for detector " + detectorId, e);
            listener.onFailure(e);
        }));
    }

    /**
     * Init realtime task cache and clean up realtime task cache on old coordinating node. Realtime AD
     * depends on job scheduler to choose node (job coordinating node) to run AD job. Nodes have primary
     * or replica shard of AD job index are candidate to run AD job. Job scheduler will build hash ring
     * on these candidate nodes and choose one to run AD job. If AD job index shard relocated, for example
     * new node added into cluster, then job scheduler will rebuild hash ring and may choose different
     * node to run AD job. So we need to init realtime task cache on new AD job coordinating node and
     * clean up cache on old coordinating node.
     *
     * If realtime task cache inited for the first time on this node, listener will return true; otherwise
     * listener will return false.
     *
     * @param detectorId detector id
     * @param detector anomaly detector
     * @param transportService transport service
     * @param listener listener
     */
    public void initRealtimeTaskCacheAndCleanupStaleCache(
        String detectorId,
        AnomalyDetector detector,
        TransportService transportService,
        ActionListener<Boolean> listener
    ) {
        try {
            if (adTaskCacheManager.getRealtimeTaskCache(detectorId) != null) {
                listener.onResponse(false);
                return;
            }

            getAndExecuteOnLatestDetectorLevelTask(detectorId, REALTIME_TASK_TYPES, (adTaskOptional) -> {
                if (!adTaskOptional.isPresent()) {
                    logger.debug("Can't find realtime task for detector {}, init realtime task cache directly", detectorId);
                    AnomalyDetectorFunction function = () -> createNewADTask(
                        detector,
                        null,
                        detector.getUser(),
                        clusterService.localNode().getId(),
                        ActionListener.wrap(r -> {
                            logger.info("Recreate realtime task successfully for detector {}", detectorId);
                            adTaskCacheManager.initRealtimeTaskCache(detectorId, detector.getDetectorIntervalInMilliseconds());
                            listener.onResponse(true);
                        }, e -> {
                            logger.error("Failed to recreate realtime task for detector " + detectorId, e);
                            listener.onFailure(e);
                        })
                    );
                    recreateRealtimeTask(function, listener);
                    return;
                }

                ADTask adTask = adTaskOptional.get();
                String localNodeId = clusterService.localNode().getId();
                String oldCoordinatingNode = adTask.getCoordinatingNode();
                if (oldCoordinatingNode != null && !localNodeId.equals(oldCoordinatingNode)) {
                    logger
                        .warn(
                            "AD realtime job coordinating node changed from {} to this node {} for detector {}",
                            oldCoordinatingNode,
                            localNodeId,
                            detectorId
                        );
                    cleanDetectorCache(adTask, transportService, () -> {
                        logger
                            .info(
                                "Realtime task cache cleaned on old coordinating node {} for detector {}",
                                oldCoordinatingNode,
                                detectorId
                            );
                        adTaskCacheManager.initRealtimeTaskCache(detectorId, detector.getDetectorIntervalInMilliseconds());
                        listener.onResponse(true);
                    }, listener);
                } else {
                    logger.info("Init realtime task cache for detector {}", detectorId);
                    adTaskCacheManager.initRealtimeTaskCache(detectorId, detector.getDetectorIntervalInMilliseconds());
                    listener.onResponse(true);
                }
            }, transportService, false, listener);
        } catch (Exception e) {
            logger.error("Failed to init realtime task cache for " + detectorId, e);
            listener.onFailure(e);
        }
    }

    private void recreateRealtimeTask(AnomalyDetectorFunction function, ActionListener<Boolean> listener) {
        if (detectionIndices.doesDetectorStateIndexExist()) {
            function.execute();
        } else {
            // If detection index doesn't exist, create index and execute function.
            detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                if (r.isAcknowledged()) {
                    logger.info("Created {} with mappings.", DETECTION_STATE_INDEX);
                    function.execute();
                } else {
                    String error = String.format(Locale.ROOT, CREATE_INDEX_NOT_ACKNOWLEDGED, DETECTION_STATE_INDEX);
                    logger.warn(error);
                    listener.onFailure(new OpenSearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                }
            }, e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                    function.execute();
                } else {
                    logger.error("Failed to init anomaly detection state index", e);
                    listener.onFailure(e);
                }
            }));
        }
    }

    public void refreshRealtimeJobRunTime(String detectorId) {
        adTaskCacheManager.refreshRealtimeJobRunTime(detectorId);
    }

    public void removeRealtimeTaskCache(String detectorId) {
        adTaskCacheManager.removeRealtimeTaskCache(detectorId);
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
            } else if (exception instanceof ADTaskCancelledException || exception instanceof EndRunException) {
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
        String detectorTaskId = adTask.getDetectorLevelTaskId();

        ActionListener<UpdateResponse> wrappedListener = ActionListener.wrap(response -> {
            logger.info("Historical HC detector done with state: {}. Remove from cache, detector id:{}", state.name(), detectorId);
            adTaskCacheManager.removeHistoricalTaskCache(detectorId);
        }, e -> {
            // HC detector task may fail to update as FINISHED for some edge case if failed to get updating semaphore.
            // Will reset task state when get detector with task or maintain tasks in hourly cron.
            if (e instanceof LimitExceededException && e.getMessage().contains(HC_DETECTOR_TASK_IS_UPDATING)) {
                logger.warn("HC task is updating, skip this update for task: " + taskId);
            } else {
                logger.error("Failed to update task: " + taskId, e);
            }
            adTaskCacheManager.removeHistoricalTaskCache(detectorId);
        });

        long timeoutInMillis = 2000;// wait for 2 seconds to acquire updating HC detector task semaphore
        if (state == ADTaskState.FINISHED) {
            this.countEntityTasksByState(detectorTaskId, ImmutableList.of(ADTaskState.FINISHED), ActionListener.wrap(r -> {
                logger.info("number of finished entity tasks: {}, for detector {}", r, adTask.getDetectorId());
                // Set task as FAILED if no finished entity task; otherwise set as FINISHED
                ADTaskState hcDetectorTaskState = r == 0 ? ADTaskState.FAILED : ADTaskState.FINISHED;
                // execute in AD batch task thread pool in case waiting for semaphore waste any shared OpenSearch thread pool
                threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
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
                        timeoutInMillis,
                        wrappedListener
                    );
                });

            }, e -> {
                logger.error("Failed to get finished entity tasks", e);
                String errorMessage = getErrorMessage(e);
                threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
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
                                errorMessage,
                                EXECUTION_END_TIME_FIELD,
                                Instant.now().toEpochMilli()
                            ),
                        timeoutInMillis,
                        wrappedListener
                    );
                });
            }));
        } else {
            threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                updateADHCDetectorTask(
                    detectorId,
                    taskId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            state.name(),
                            ERROR_FIELD,
                            adTask.getError(),
                            EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli()
                        ),
                    timeoutInMillis,
                    wrappedListener
                );
            });

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
        updateADHCDetectorTask(detectorId, taskId, updatedFields, 0, ActionListener.wrap(response -> {
            if (response == null) {
                logger.debug("Skip updating AD task: {}", taskId);
            } else if (response.status() == RestStatus.OK) {
                logger.debug("Updated AD task successfully: {}, taskId: {}", response.status(), taskId);
            } else {
                logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            }
        }, e -> {
            if (e instanceof LimitExceededException && e.getMessage().contains(HC_DETECTOR_TASK_IS_UPDATING)) {
                logger.warn("AD HC detector task is updating, skip this update for task: " + taskId);
            } else {
                logger.error("Failed to update AD HC detector task: " + taskId, e);
            }
        }));
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
     * @param timeoutInMillis the maximum time to wait for task updating semaphore, zero or negative means don't wait at all
     * @param listener action listener
     */
    private void updateADHCDetectorTask(
        String detectorId,
        String taskId,
        Map<String, Object> updatedFields,
        long timeoutInMillis,
        ActionListener<UpdateResponse> listener
    ) {
        try {
            if (adTaskCacheManager.tryAcquireTaskUpdatingSemaphore(detectorId, timeoutInMillis)) {
                try {
                    updateADTask(
                        taskId,
                        updatedFields,
                        ActionListener.runAfter(listener, () -> { adTaskCacheManager.releaseTaskUpdatingSemaphore(detectorId); })
                    );
                } catch (Exception e) {
                    logger.error("Failed to update detector task " + taskId, e);
                    adTaskCacheManager.releaseTaskUpdatingSemaphore(detectorId);
                    listener.onFailure(e);
                }
            } else if (!adTaskCacheManager.isHCTaskCoordinatingNode(detectorId)) {
                // It's possible that AD task cache cleaned up by other task. Return null to avoid too many failure logs.
                logger.info("HC detector task cache does not exist, detectorId:{}, taskId:{}", detectorId, taskId);
                listener.onResponse(null);
            } else {
                logger.info("HC detector task is updating, detectorId:{}, taskId:{}", detectorId, taskId);
                listener.onFailure(new LimitExceededException(HC_DETECTOR_TASK_IS_UPDATING));
            }
        } catch (Exception e) {
            logger.error("Failed to get AD HC detector task updating semaphore " + taskId, e);
            listener.onFailure(e);
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
        String detectorId = adTask.getDetectorId();
        int scaleDelta = scaleTaskSlots(
            adTask,
            transportService,
            ActionListener
                .wrap(
                    r -> { logger.debug("Scale up task slots done for detector {}, task {}", detectorId, adTask.getTaskId()); },
                    e -> { logger.error("Failed to scale up task slots for task " + adTask.getTaskId(), e); }
                )
        );
        if (scaleDelta < 0) {
            logger
                .warn(
                    "Have scaled down task slots. Will not poll next entity for detector {}, task {}, task slots: {}",
                    detectorId,
                    adTask.getTaskId(),
                    adTaskCacheManager.getDetectorTaskSlots(detectorId)
                );
            listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.ACCEPTED));
            return;
        }
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                .info(
                    "AD entity task {} of detector {} dispatched to {} node {}",
                    adTask.getTaskId(),
                    detectorId,
                    remoteOrLocal,
                    r.getNodeId()
                );
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK);
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
    protected int scaleTaskSlots(
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
        DiscoveryNode[] eligibleDataNodes = hashRing.getNodesWithSameLocalAdVersion();
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
                detectorId,
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
                detectorTaskProfile.setTotalEntitiesInited(adTaskCacheManager.topEntityInited(detectorId));
                detectorTaskProfile.setTotalEntitiesCount(adTaskCacheManager.getTopEntityCount(detectorId));
                detectorTaskProfile.setPendingEntitiesCount(adTaskCacheManager.getPendingEntityCount(detectorId));
                detectorTaskProfile.setRunningEntitiesCount(adTaskCacheManager.getRunningEntityCount(detectorId));
                detectorTaskProfile.setRunningEntities(adTaskCacheManager.getRunningEntities(detectorId));
                detectorTaskProfile.setAdTaskType(ADTaskType.HISTORICAL_HC_DETECTOR.name());
                Instant latestHCTaskRunTime = adTaskCacheManager.getLatestHCTaskRunTime(detectorId);
                if (latestHCTaskRunTime != null) {
                    detectorTaskProfile.setLatestHCTaskRunTime(latestHCTaskRunTime.toEpochMilli());
                }
            }
            if (tasksOfDetector.size() > 0) {
                List<ADEntityTaskProfile> entityTaskProfiles = new ArrayList<>();

                tasksOfDetector.forEach(taskId -> {
                    ADEntityTaskProfile entityTaskProfile = new ADEntityTaskProfile(
                        adTaskCacheManager.getShingle(taskId).size(),
                        adTaskCacheManager.getTRcfModel(taskId).getForest().getTotalUpdates(),
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
                    adTaskCacheManager.getTRcfModel(taskId).getForest().getTotalUpdates(),
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
        threadPool
            .executor(AD_BATCH_TASK_THREAD_POOL_NAME)
            .execute(
                () -> {
                    // Clean expired HC batch task run states as it may exists after HC historical analysis done if user cancel
                    // before querying top entities done. We will clean it in hourly cron, check "maintainRunningHistoricalTasks"
                    // method. Clean it up here when get task profile to release memory earlier.
                    adTaskCacheManager.cleanExpiredHCBatchTaskRunStates();
                }
            );
        logger.debug("Local AD task profile of detector {}: {}", detectorId, detectorTaskProfile);
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
            && realtimeTaskCache.getInitProgress() != null
            && realtimeTaskCache.getInitProgress().floatValue() == 1.0
            && Objects.equals(error, realtimeTaskCache.getError());
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

    /**
     * In normal case "assigned_task_slots" should always greater than or equals to "used_task_slots". One
     * example may help understand.
     *
     * If a cluster has 3 data nodes, HC detector was assigned 3 task slots. It will start task lane one by one,
     * so the timeline looks like:
     *
     * t1 -- node1: 1 entity running
     * t2 -- node1: 1 entity running ,
     *       node2: 1 entity running
     * t3 -- node1: 1 entity running ,
     *       node2: 1 entity running,
     *       node3: 1 entity running
     *
     * So if we check between t2 and t3, we can see assigned task slots (3) is greater than used task slots (2).
     *
     * Assume node1 is coordinating node, the assigned task slots will be cached on node1. If node1 left cluster,
     * then we don't know how many task slots was assigned to the detector. But the detector will not send out
     * more entity tasks as well due to coordinating node left. So we can calculate how many task slots used on
     * node2 and node3 to calculate how many task slots available for new detector.
     * @return assigned batch task slots
     */
    public int getLocalAdAssignedBatchTaskSlot() {
        return adTaskCacheManager.getTotalDetectorTaskSlots();
    }

    // =========================================================
    // Methods below are maintenance code triggered by hourly cron
    // =========================================================

    /**
     * Maintain running historical tasks.
     * Search current running latest tasks, then maintain tasks one by one.
     * Get task profile to check if task is really running on worker node.
     * 1. If not running, reset task state as STOPPED.
     * 2. If task is running and task for HC detector, check if there is any stale running entities and
     *    clean up.
     *
     * @param transportService transport service
     * @param size return how many tasks
     */
    public void maintainRunningHistoricalTasks(TransportService transportService, int size) {
        // Clean expired HC batch task run state cache.
        adTaskCacheManager.cleanExpiredHCBatchTaskRunStates();

        // Find owning node with highest AD version to make sure we only have 1 node maintain running historical tasks
        // and we use the latest logic.
        Optional<DiscoveryNode> owningNode = hashRing.getOwningNodeWithHighestAdVersion(AD_TASK_MAINTAINENCE_NODE_MODEL_ID);
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
                    logger.error("Maintaining running historical task: failed to parse AD task " + searchHit.getId(), e);
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
            resetHistoricalDetectorTaskState(ImmutableList.of(adTask), () -> {
                logger.debug("Finished maintaining running historical task {}", adTask.getTaskId());
                maintainRunningHistoricalTask(taskQueue, transportService);
            },
                transportService,
                ActionListener
                    .wrap(
                        r -> {
                            logger
                                .debug(
                                    "Reset historical task state done for task {}, detector {}",
                                    adTask.getTaskId(),
                                    adTask.getDetectorId()
                                );
                        },
                        e -> { logger.error("Failed to reset historical task state for task " + adTask.getTaskId(), e); }
                    )
            );
        }, TimeValue.timeValueSeconds(DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS), AD_BATCH_TASK_THREAD_POOL_NAME);
    }

    /**
     * Maintain running realtime tasks. Check if realtime task cache expires or not. Remove realtime
     * task cache directly if expired.
     */
    public void maintainRunningRealtimeTasks() {
        String[] detectorIds = adTaskCacheManager.getDetectorIdsInRealtimeTaskCache();
        if (detectorIds == null || detectorIds.length == 0) {
            return;
        }
        for (int i = 0; i < detectorIds.length; i++) {
            String detectorId = detectorIds[i];
            ADRealtimeTaskCache taskCache = adTaskCacheManager.getRealtimeTaskCache(detectorId);
            if (taskCache != null && taskCache.expired()) {
                adTaskCacheManager.removeRealtimeTaskCache(detectorId);
            }
        }
    }

}
