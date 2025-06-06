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
import static org.opensearch.ad.constant.ADCommonMessages.DETECTOR_IS_RUNNING;
import static org.opensearch.ad.constant.ADCommonMessages.EXCEED_HISTORICAL_ANALYSIS_LIMIT;
import static org.opensearch.ad.constant.ADCommonMessages.HC_DETECTOR_TASK_IS_UPDATING;
import static org.opensearch.ad.constant.ADCommonMessages.NO_ELIGIBLE_NODE_TO_RUN_DETECTOR;
import static org.opensearch.ad.constant.ADCommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.indices.ADIndexManagement.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;
import static org.opensearch.ad.model.ADTaskType.REALTIME_TASK_TYPES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static org.opensearch.timeseries.model.TaskState.NOT_ENDED_STATES;
import static org.opensearch.timeseries.model.TaskType.taskTypeToString;
import static org.opensearch.timeseries.stats.InternalStatNames.AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT;
import static org.opensearch.timeseries.stats.InternalStatNames.AD_USED_BATCH_TASK_SLOT_COUNT;
import static org.opensearch.timeseries.util.RestHandlerUtils.createXContentParserFromRegistry;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
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
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.Version;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.ADTaskProfileRunner;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.ADBatchAnomalyResultAction;
import org.opensearch.ad.transport.ADBatchAnomalyResultRequest;
import org.opensearch.ad.transport.ADCancelTaskAction;
import org.opensearch.ad.transport.ADCancelTaskRequest;
import org.opensearch.ad.transport.ADStatsNodesAction;
import org.opensearch.ad.transport.ForwardADTaskAction;
import org.opensearch.ad.transport.ForwardADTaskRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.DuplicateTaskException;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TaskCancelledException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.function.ResponseTransformer;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityTaskProfile;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.StatsNodeResponse;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Manage AD task.
 */
public class ADTaskManager extends TaskManager<ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADIndexManagement> {
    public static final String AD_TASK_LEAD_NODE_MODEL_ID = "ad_task_lead_node_model_id";
    public static final String AD_TASK_MAINTAINENCE_NODE_MODEL_ID = "ad_task_maintainence_node_model_id";
    // HC batch task timeout after 10 minutes if no update after last known run time.
    public static final int HC_BATCH_TASK_CACHE_TIMEOUT_IN_MILLIS = 600_000;
    public final Logger logger = LogManager.getLogger(this.getClass());
    static final String STATE_INDEX_NOT_EXIST_MSG = "State index does not exist.";
    private final Set<String> retryableErrors = ImmutableSet.of(EXCEED_HISTORICAL_ANALYSIS_LIMIT, NO_ELIGIBLE_NODE_TO_RUN_DETECTOR);

    private final DiscoveryNodeFilterer nodeFilter;

    private final HashRing hashRing;
    private volatile Integer pieceIntervalSeconds;

    private volatile TransportRequestOptions transportRequestOptions;
    private final Semaphore checkingTaskSlot;

    private volatile Integer maxAdBatchTaskPerNode;
    private volatile Integer maxRunningEntitiesPerDetector;

    private final Semaphore scaleEntityTaskLane;
    private static final int SCALE_ENTITY_TASK_LANE_INTERVAL_IN_MILLIS = 10_000; // 10 seconds
    private final ADTaskProfileRunner taskProfileRunner;

    public ADTaskManager(
        Settings settings,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ADIndexManagement detectionIndices,
        DiscoveryNodeFilterer nodeFilter,
        HashRing hashRing,
        ADTaskCacheManager adTaskCacheManager,
        ThreadPool threadPool,
        NodeStateManager nodeStateManager,
        ADTaskProfileRunner taskProfileRunner
    ) {
        super(
            adTaskCacheManager,
            clusterService,
            client,
            DETECTION_STATE_INDEX,
            ADTaskType.REALTIME_TASK_TYPES,
            ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES,
            Collections.emptyList(),
            detectionIndices,
            nodeStateManager,
            AnalysisType.AD,
            xContentRegistry,
            DETECTOR_ID_FIELD,
            MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            settings,
            threadPool,
            ALL_AD_RESULTS_INDEX_PATTERN,
            AD_BATCH_TASK_THREAD_POOL_NAME,
            DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
            TaskState.STOPPED
        );

        this.nodeFilter = nodeFilter;
        this.hashRing = hashRing;

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);

        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);

        this.maxRunningEntitiesPerDetector = MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS, it -> maxRunningEntitiesPerDetector = it);

        transportRequestOptions = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AD_REQUEST_TIMEOUT.get(settings))
            .build();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_REQUEST_TIMEOUT, it -> {
            transportRequestOptions = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.REG).withTimeout(it).build();
        });

        this.checkingTaskSlot = new Semaphore(1);
        this.scaleEntityTaskLane = new Semaphore(1);
        this.taskProfileRunner = taskProfileRunner;
    }

    /**
     * When AD receives start historical analysis request for a detector, will
     * 1. Forward to lead node to check available task slots first.
     * 2. If available task slots exit, will forward request to coordinating node
     *    to gather information like top entities.
     * 3. Then coordinating node will choose one data node with least load as work
     *    node and dispatch historical analysis to it.
     *
     * @param config config accessor
     * @param detectionDateRange detection date range
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    @Override
    public void startHistorical(
        Config config,
        DateRange detectionDateRange,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        ForwardADTaskRequest forwardADTaskRequest = new ForwardADTaskRequest(
            (AnomalyDetector) config,
            detectionDateRange,
            user,
            ADTaskAction.APPLY_FOR_TASK_SLOTS
        );
        forwardRequestToLeadNode(forwardADTaskRequest, transportService, listener);
    }

    public void forwardScaleTaskSlotRequestToLeadNode(
        ADTask adTask,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        forwardRequestToLeadNode(new ForwardADTaskRequest(adTask, ADTaskAction.CHECK_AVAILABLE_TASK_SLOTS), transportService, listener);
    }

    public void forwardRequestToLeadNode(
        ForwardADTaskRequest forwardADTaskRequest,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        hashRing.buildAndGetOwningNodeWithSameLocalVersion(AD_TASK_LEAD_NODE_MODEL_ID, node -> {
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
                    new ActionListenerResponseHandler<>(listener, JobResponse::new)
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
        DateRange detectionDateRange,
        User user,
        int availableTaskSlots,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        String detectorId = detector.getId();
        hashRing.buildAndGetOwningNodeWithSameLocalVersion(detectorId, owningNode -> {
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
        DateRange detectionDateRange,
        User user,
        Integer availableTaskSlots,
        ADTaskAction adTaskAction,
        TransportService transportService,
        DiscoveryNode node,
        ActionListener<JobResponse> listener
    ) {
        Version adVersion = hashRing.getVersion(node.getId());
        transportService
            .sendRequest(
                node,
                ForwardADTaskAction.NAME,
                // We need to check AD version of remote node as we may send clean detector cache request to old
                // node, check ADTaskManager#cleanDetectorCache.
                new ForwardADTaskRequest(detector, detectionDateRange, user, adTaskAction, availableTaskSlots, adVersion),
                transportRequestOptions,
                new ActionListenerResponseHandler<>(listener, JobResponse::new)
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
        ActionListener<JobResponse> listener
    ) {
        logger.debug("Forward AD task to coordinating node, task id: {}, action: {}", adTask.getTaskId(), adTaskAction.name());
        transportService
            .sendRequest(
                getCoordinatingNode(adTask),
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(adTask, adTaskAction),
                transportRequestOptions,
                new ActionListenerResponseHandler<>(listener, JobResponse::new)
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
        ActionListener<JobResponse> listener
    ) {
        transportService
            .sendRequest(
                getCoordinatingNode(adTask),
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(adTask, adTaskAction, staleRunningEntity),
                transportRequestOptions,
                new ActionListenerResponseHandler<>(listener, JobResponse::new)
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
        DateRange detectionDateRange,
        User user,
        ADTaskAction afterCheckAction,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        String detectorId = detector.getId();
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
        ActionListener<JobResponse> wrappedActionListener = ActionListener.runAfter(listener, () -> {
            checkingTaskSlot.release(1);
            logger.debug("Release checking task slot semaphore on lead node for detector {}", detectorId);
        });
        hashRing.getNodesWithSameLocalVersion(nodes -> {
            int maxAdTaskSlots = nodes.length * maxAdBatchTaskPerNode;
            StatsRequest adStatsRequest = new StatsRequest(nodes);
            adStatsRequest
                .addAll(ImmutableSet.of(AD_USED_BATCH_TASK_SLOT_COUNT.getName(), AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName()));
            client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
                int totalUsedTaskSlots = 0; // Total entity tasks running on worker nodes
                int totalAssignedTaskSlots = 0; // Total assigned task slots on coordinating nodes
                for (StatsNodeResponse response : adStatsResponse.getNodes()) {
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
                int approvedTaskSlots = detector.isHighCardinality() ? Math.min(maxRunningEntitiesPerDetector, availableAdTaskSlots) : 1;
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
        DateRange detectionDateRange,
        User user,
        ADTaskAction targetActionOfTaskSlotChecking,
        TransportService transportService,
        ActionListener<JobResponse> wrappedActionListener,
        int approvedTaskSlots
    ) {
        switch (targetActionOfTaskSlotChecking) {
            case START:
                logger.info("Will assign {} task slots to run historical analysis for detector {}", approvedTaskSlots, detector.getId());
                startHistoricalAnalysis(detector, detectionDateRange, user, approvedTaskSlots, transportService, wrappedActionListener);
                break;
            case SCALE_ENTITY_TASK_SLOTS:
                logger
                    .info(
                        "There are {} task slots available now to scale historical analysis task lane for detector {}",
                        approvedTaskSlots,
                        adTask.getConfigId()
                    );
                scaleTaskLaneOnCoordinatingNode(adTask, approvedTaskSlots, transportService, wrappedActionListener);
                break;
            default:
                wrappedActionListener.onFailure(new TimeSeriesException("Unknown task action " + targetActionOfTaskSlotChecking));
                break;
        }
    }

    protected void scaleTaskLaneOnCoordinatingNode(
        ADTask adTask,
        int approvedTaskSlot,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        transportService
            .sendRequest(
                getCoordinatingNode(adTask),
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(adTask, approvedTaskSlot, ADTaskAction.SCALE_ENTITY_TASK_SLOTS),
                transportRequestOptions,
                new ActionListenerResponseHandler<>(listener, JobResponse::new)
            );
    }

    /**
     * Retrieves the coordinating node for the given ADTask.
     *
     * This method looks for a node in the list of eligible data nodes that matches the coordinating node ID
     * and version specified in the ADTask. The matching criteria are:
     * 1. The node ID must be equal to the coordinating node ID.
     * 2. Both node versions must be either null or equal.
     *
     * If the coordinating node ID and the local node have different software versions, this method will
     * throw a ResourceNotFoundException.
     *
     * @param adTask the ADTask containing the coordinating node information.
     * @return a DiscoveryNode containing the matching DiscoveryNode if found, or throws ResourceNotFoundException if no match is found.
     * The caller is supposed to handle the thrown exception.
     * @throws ResourceNotFoundException if the coordinating node has a different version than the local node or if the coordinating node is not found.
     */
    private DiscoveryNode getCoordinatingNode(ADTask adTask) {
        try {
            String coordinatingNodeId = adTask.getCoordinatingNode();
            Version coordinatingNodeVersion = hashRing.getVersion(coordinatingNodeId);
            Version localNodeVersion = hashRing.getVersion(clusterService.localNode().getId());
            if (!isSameVersion(coordinatingNodeVersion, localNodeVersion)) {
                throw new ResourceNotFoundException(
                    adTask.getConfigId(),
                    "AD task coordinating node has different version than local node"
                );
            }

            DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();

            for (DiscoveryNode node : eligibleDataNodes) {
                String nodeId = node.getId();
                if (nodeId == null) {
                    continue;
                }

                if (nodeId.equals(coordinatingNodeId)) {
                    return node;
                }
            }

            throw new ResourceNotFoundException(adTask.getConfigId(), "AD task coordinating node not found");
        } catch (Exception e) {
            logger.error("Error locating coordinating node", e);
            throw new ResourceNotFoundException(adTask.getConfigId(), "AD task coordinating node not found");
        }
    }

    private boolean isSameVersion(Version version1, Version version2) {
        return (version1 == null && version2 == null) || (version1 != null && version2 != null && version1.compareTo(version2) == 0);
    }

    @Override
    protected TaskType getTaskType(Config config, DateRange detectionDateRange, boolean runOnce) {
        if (detectionDateRange == null) {
            return config.isHighCardinality() ? ADTaskType.REALTIME_HC_DETECTOR : ADTaskType.REALTIME_SINGLE_ENTITY;
        } else {
            return config.isHighCardinality() ? ADTaskType.HISTORICAL_HC_DETECTOR : ADTaskType.HISTORICAL_SINGLE_ENTITY;
        }
    }

    /**
    * If resetTaskState is true, will collect latest task's profile data from all data nodes. If no data
    * node running the latest task, will reset the task state as STOPPED; otherwise, check if there is
    * any stale running entities(entity exists in coordinating node cache but no task running on worker
    * node) and clean up.
    */
    protected <T> void resetHistoricalConfigTaskState(
        List<TimeSeriesTask> runningHistoricalTasks,
        ExecutorFunction function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        if (ParseUtils.isNullOrEmpty(runningHistoricalTasks)) {
            function.execute();
            return;
        }
        ADTask adTask = (ADTask) runningHistoricalTasks.get(0);
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
        taskProfileRunner.getTaskProfile(adTask, ActionListener.wrap(taskProfile -> {
            boolean taskStopped = isTaskStopped(taskId, detector, taskProfile);
            if (taskStopped) {
                logger.debug("Reset task state as stopped, task id: {}", adTask.getTaskId());
                if (taskProfile.getTaskId() == null // This means coordinating node doesn't have HC detector cache
                    && detector.isHighCardinality()
                    && !ParseUtils.isNullOrEmpty(taskProfile.getEntityTaskProfiles())) {
                    // If coordinating node restarted, HC detector cache on it will be gone. But worker node still
                    // runs entity tasks, we'd better stop these entity tasks to clean up resource earlier.
                    stopHistoricalAnalysis(adTask.getConfigId(), Optional.of(adTask), null, ActionListener.wrap(r -> {
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
                    if (!ParseUtils.isNullOrEmpty(taskProfile.getRunningEntities())
                        && hcBatchTaskExpired(taskProfile.getLatestHCTaskRunTime())) {
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
        String detectorId = detector.getId();
        if (taskProfile == null || !Objects.equals(taskId, taskProfile.getTaskId())) {
            logger.debug("AD task not found for task {} detector {}", taskId, detectorId);
            // If no node is running this task, reset it as STOPPED.
            return true;
        }
        if (!detector.isHighCardinality() && taskProfile.getNodeId() == null) {
            logger.debug("AD task not running for single entity detector {}, task {}", detectorId, taskId);
            return true;
        }
        if (detector.isHighCardinality()
            && taskProfile.getTotalEntitiesInited()
            && ParseUtils.isNullOrEmpty(taskProfile.getRunningEntities())
            && ParseUtils.isNullOrEmpty(taskProfile.getEntityTaskProfiles())
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

    public void stopHistoricalAnalysis(String detectorId, Optional<ADTask> adTask, User user, ActionListener<JobResponse> listener) {
        if (!adTask.isPresent()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "Detector not started"));
            return;
        }

        if (adTask.get().isDone()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "No running task found"));
            return;
        }

        String taskId = adTask.get().getTaskId();
        DiscoveryNode[] dataNodes = hashRing.getNodesWithSameLocalVersion();
        String userName = user == null ? null : user.getName();

        ADCancelTaskRequest cancelTaskRequest = new ADCancelTaskRequest(detectorId, taskId, userName, dataNodes);
        client.execute(ADCancelTaskAction.INSTANCE, cancelTaskRequest, ActionListener.wrap(response -> {
            listener.onResponse(new JobResponse(taskId));
        }, e -> {
            logger.error("Failed to cancel AD task " + taskId + ", detector id: " + detectorId, e);
            listener.onFailure(e);
        }));
    }

    private boolean lastUpdateTimeOfHistoricalTaskExpired(TimeSeriesTask adTask) {
        // Wait at least 10 seconds. Piece interval seconds is dynamic setting, user could change it to a smaller value.
        int waitingTime = Math.max(2 * pieceIntervalSeconds, 10);
        return adTask.getLastUpdateTime().plus(waitingTime, ChronoUnit.SECONDS).isBefore(Instant.now());
    }

    @Override
    protected boolean isHistoricalHCTask(TimeSeriesTask task) {
        return ADTaskType.HISTORICAL_HC_DETECTOR.name().equals(task.getTaskType());
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
    @Override
    public <T> void cleanConfigCache(
        TimeSeriesTask adTask,
        TransportService transportService,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        String coordinatingNode = adTask.getCoordinatingNode();
        String detectorId = adTask.getConfigId();
        String taskId = adTask.getTaskId();
        try {
            forwardADTaskToCoordinatingNode((ADTask) adTask, ADTaskAction.CLEAN_CACHE, transportService, ActionListener.wrap(r -> {
                function.execute();
            }, e -> {
                logger.error("Failed to clear detector cache on coordinating node " + coordinatingNode, e);
                listener.onFailure(e);
            }));
        } catch (ResourceNotFoundException e) {
            logger
                .warn(
                    "Task coordinating node left cluster or has different software version, taskId: {}, detectorId: {}, coordinatingNode: {}",
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

    protected void cleanDetectorCache(ADTask adTask, TransportService transportService, ExecutorFunction function) {
        String detectorId = adTask.getConfigId();
        String taskId = adTask.getTaskId();
        cleanConfigCache(adTask, transportService, function, ActionListener.wrap(r -> {
            logger.debug("Successfully cleaned cache for detector {}, task {}", detectorId, taskId);
        }, e -> { logger.error("Failed to clean cache for detector " + detectorId + ", task " + taskId, e); }));
    }

    @Override
    protected <T> void createNewTask(
        Config config,
        DateRange detectionDateRange,
        boolean runOnce,
        User user,
        String coordinatingNode,
        TaskState initialState,
        ActionListener<T> listener
    ) {
        String userName = user == null ? null : user.getName();
        Instant now = Instant.now();
        String taskType = getTaskType(config, detectionDateRange, runOnce).name();
        ADTask adTask = new ADTask.Builder()
            .configId(config.getId())
            .detector((AnomalyDetector) config)
            .isLatest(true)
            .taskType(taskType)
            .executionStartTime(now)
            .taskProgress(0.0f)
            .initProgress(0.0f)
            .state(TaskState.CREATED.name())
            .lastUpdateTime(now)
            .startedBy(userName)
            .coordinatingNode(coordinatingNode)
            .detectionDateRange(detectionDateRange)
            .user(user)
            .build();

        createTaskDirectly(
            adTask,
            r -> onIndexConfigTaskResponse(
                r,
                adTask,
                (response, delegatedListener) -> cleanOldConfigTaskDocs(
                    response,
                    adTask,
                    (indexResponse) -> (T) new JobResponse(indexResponse.getId()),
                    delegatedListener
                ),
                listener
            ),
            listener
        );
    }

    @Override
    protected <T> void onIndexConfigTaskResponse(
        IndexResponse response,
        ADTask adTask,
        BiConsumer<IndexResponse, ActionListener<T>> function,
        ActionListener<T> listener
    ) {
        if (response == null || response.getResult() != CREATED) {
            String errorMsg = ExceptionUtil.getShardsFailure(response);
            listener.onFailure(new OpenSearchStatusException(errorMsg, response.status()));
            return;
        }
        adTask.setTaskId(response.getId());
        ActionListener<T> delegatedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
            handleTaskException(adTask, e);
            if (e instanceof DuplicateTaskException) {
                listener.onFailure(new OpenSearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
            } else {
                // For historical AD task, clear historical task if any other exception happened.
                // For realtime AD, task cache will be inited when realtime job starts, check
                // ADTaskManager#initRealtimeTaskCacheAndCleanupStaleCache for details. Here the
                // realtime task cache not inited yet when create AD task, so no need to cleanup.
                if (adTask.isHistoricalTask()) {
                    taskCacheManager.removeHistoricalTaskCache(adTask.getConfigId());
                }
                listener.onFailure(e);
            }
        });
        try {
            // Put config id in cache. If config id already in cache, will throw
            // DuplicateTaskException. This is to solve race condition when user send
            // multiple start request for one historical run.
            if (adTask.isHistoricalTask()) {
                taskCacheManager.add(adTask.getConfigId(), adTask);
            }
        } catch (Exception e) {
            delegatedListener.onFailure(e);
            return;
        }
        if (function != null) {
            function.accept(response, delegatedListener);
        }
    }

    @Override
    protected <T> void runBatchResultAction(
        IndexResponse response,
        ADTask adTask,
        ResponseTransformer<IndexResponse, T> responseTransformer,
        ActionListener<T> listener
    ) {
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                .info(
                    "AD task {} of detector {} dispatched to {} node {}",
                    adTask.getTaskId(),
                    adTask.getConfigId(),
                    remoteOrLocal,
                    r.getNodeId()
                );

            listener.onResponse(responseTransformer.transform(response));
        }, e -> listener.onFailure(e)));
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
        ADTaskCancellationState cancellationState = taskCacheManager.cancelByDetectorId(detectorId, detectorTaskId, reason, userName);
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
     * @param config config accessor
     * @param transportService transport service
     * @param listener listener
     */
    @Override
    public void initRealtimeTaskCacheAndCleanupStaleCache(
        String detectorId,
        Config config,
        TransportService transportService,
        ActionListener<Boolean> listener
    ) {
        try {
            if (taskCacheManager.getRealtimeTaskCache(detectorId) != null) {
                listener.onResponse(false);
                return;
            }

            AnomalyDetector detector = (AnomalyDetector) config;
            getAndExecuteOnLatestConfigLevelTask(detectorId, REALTIME_TASK_TYPES, (adTaskOptional) -> {
                if (!adTaskOptional.isPresent()) {
                    logger.debug("Can't find realtime task for detector {}, init realtime task cache directly", detectorId);
                    ExecutorFunction function = () -> createNewTask(
                        detector,
                        null,
                        false,
                        detector.getUser(),
                        clusterService.localNode().getId(),
                        TaskState.CREATED,
                        ActionListener.wrap(r -> {
                            logger.info("Recreate realtime task successfully for detector {}", detectorId);
                            taskCacheManager.initRealtimeTaskCache(detectorId, detector.getIntervalInMilliseconds());
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
                    cleanConfigCache(adTask, transportService, () -> {
                        logger
                            .info(
                                "Realtime task cache cleaned on old coordinating node {} for detector {}",
                                oldCoordinatingNode,
                                detectorId
                            );
                        taskCacheManager.initRealtimeTaskCache(detectorId, detector.getIntervalInMilliseconds());
                        listener.onResponse(true);
                    }, listener);
                } else {
                    logger.info("Init realtime task cache for detector {}", detectorId);
                    taskCacheManager.initRealtimeTaskCache(detectorId, detector.getIntervalInMilliseconds());
                    listener.onResponse(true);
                }
            }, transportService, false, listener);
        } catch (Exception e) {
            logger.error("Failed to init realtime task cache for " + detectorId, e);
            listener.onFailure(e);
        }
    }

    private void recreateRealtimeTask(ExecutorFunction function, ActionListener<Boolean> listener) {
        if (indexManagement.doesStateIndexExist()) {
            function.execute();
        } else {
            // If detection index doesn't exist, create index and execute function.
            indexManagement.initStateIndex(ActionListener.wrap(r -> {
                if (r.isAcknowledged()) {
                    logger.info("Created {} with mappings.", DETECTION_STATE_INDEX);
                    function.execute();
                } else {
                    String error = String.format(Locale.ROOT, CommonMessages.CREATE_INDEX_NOT_ACKNOWLEDGED, DETECTION_STATE_INDEX);
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
        ActionListener<JobResponse> listener
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
            adTask.setError(ExceptionUtil.getErrorMessage(exception));
            if (exception instanceof LimitExceededException && isRetryableError(exception.getMessage())) {
                action = ADTaskAction.PUSH_BACK_ENTITY;
            } else if (exception instanceof TaskCancelledException || exception instanceof EndRunException) {
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
    public void setHCDetectorTaskDone(ADTask adTask, TaskState state, ActionListener<JobResponse> listener) {
        String detectorId = adTask.getConfigId();
        String taskId = adTask.isHistoricalEntityTask() ? adTask.getParentTaskId() : adTask.getTaskId();
        String detectorTaskId = adTask.getConfigLevelTaskId();

        ActionListener<UpdateResponse> wrappedListener = ActionListener.wrap(response -> {
            logger.info("Historical HC detector done with state: {}. Remove from cache, detector id:{}", state.name(), detectorId);
            taskCacheManager.removeHistoricalTaskCache(detectorId);
        }, e -> {
            // HC detector task may fail to update as FINISHED for some edge case if failed to get updating semaphore.
            // Will reset task state when get detector with task or maintain tasks in hourly cron.
            if (e instanceof LimitExceededException && e.getMessage().contains(HC_DETECTOR_TASK_IS_UPDATING)) {
                logger.warn("HC task is updating, skip this update for task: " + taskId);
            } else {
                logger.error("Failed to update task: " + taskId, e);
            }
            taskCacheManager.removeHistoricalTaskCache(detectorId);
        });

        long timeoutInMillis = 2000;// wait for 2 seconds to acquire updating HC detector task semaphore
        if (state == TaskState.FINISHED) {
            this.countEntityTasksByState(detectorTaskId, ImmutableList.of(TaskState.FINISHED), ActionListener.wrap(r -> {
                logger.info("number of finished entity tasks: {}, for detector {}", r, adTask.getConfigId());
                // Set task as FAILED if no finished entity task; otherwise set as FINISHED
                TaskState hcDetectorTaskState = r == 0 ? TaskState.FAILED : TaskState.FINISHED;
                // execute in AD batch task thread pool in case waiting for semaphore waste any shared OpenSearch thread pool
                threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                    updateADHCDetectorTask(
                        detectorId,
                        taskId,
                        ImmutableMap
                            .of(
                                TimeSeriesTask.STATE_FIELD,
                                hcDetectorTaskState.name(),
                                TimeSeriesTask.TASK_PROGRESS_FIELD,
                                1.0,
                                TimeSeriesTask.EXECUTION_END_TIME_FIELD,
                                Instant.now().toEpochMilli()
                            ),
                        timeoutInMillis,
                        wrappedListener
                    );
                });

            }, e -> {
                logger.error("Failed to get finished entity tasks", e);
                String errorMessage = ExceptionUtil.getErrorMessage(e);
                threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
                    updateADHCDetectorTask(
                        detectorId,
                        taskId,
                        ImmutableMap
                            .of(
                                TimeSeriesTask.STATE_FIELD,
                                TaskState.FAILED.name(),// set as FAILED if fail to get finished entity tasks.
                                TimeSeriesTask.TASK_PROGRESS_FIELD,
                                1.0,
                                TimeSeriesTask.ERROR_FIELD,
                                errorMessage,
                                TimeSeriesTask.EXECUTION_END_TIME_FIELD,
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
                            TimeSeriesTask.STATE_FIELD,
                            state.name(),
                            TimeSeriesTask.ERROR_FIELD,
                            adTask.getError(),
                            TimeSeriesTask.EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli()
                        ),
                    timeoutInMillis,
                    wrappedListener
                );
            });

        }

        listener.onResponse(new JobResponse(taskId));
    }

    /**
     * Count entity tasks by state with detector level task id(parent task id).
     *
     * @param detectorTaskId detector level task id
     * @param taskStates task states
     * @param listener action listener
     */
    public void countEntityTasksByState(String detectorTaskId, List<TaskState> taskStates, ActionListener<Long> listener) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        queryBuilder.filter(new TermQueryBuilder(TimeSeriesTask.PARENT_TASK_ID_FIELD, detectorTaskId));
        if (taskStates != null && taskStates.size() > 0) {
            queryBuilder
                .filter(
                    new TermsQueryBuilder(TimeSeriesTask.STATE_FIELD, taskStates.stream().map(s -> s.name()).collect(Collectors.toList()))
                );
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
            listener.onResponse(totalHits.value());
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
            if (taskCacheManager.tryAcquireTaskUpdatingSemaphore(detectorId, timeoutInMillis)) {
                try {
                    updateTask(
                        taskId,
                        updatedFields,
                        ActionListener.runAfter(listener, () -> { taskCacheManager.releaseTaskUpdatingSemaphore(detectorId); })
                    );
                } catch (Exception e) {
                    logger.error("Failed to update detector task " + taskId, e);
                    taskCacheManager.releaseTaskUpdatingSemaphore(detectorId);
                    listener.onFailure(e);
                }
            } else if (!taskCacheManager.isHCTaskCoordinatingNode(detectorId)) {
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
    public void runNextEntityForHCADHistorical(ADTask adTask, TransportService transportService, ActionListener<JobResponse> listener) {
        String detectorId = adTask.getConfigId();
        int scaleDelta = scaleTaskSlots(adTask, transportService, ActionListener.wrap(r -> {
            logger.debug("Scale up task slots done for detector {}, task {}", detectorId, adTask.getTaskId());
        }, e -> { logger.error("Failed to scale up task slots for task " + adTask.getTaskId(), e); }));
        if (scaleDelta < 0) {
            logger
                .warn(
                    "Have scaled down task slots. Will not poll next entity for detector {}, task {}, task slots: {}",
                    detectorId,
                    adTask.getTaskId(),
                    taskCacheManager.getDetectorTaskSlots(detectorId)
                );
            listener.onResponse(new JobResponse(detectorId));
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
            JobResponse anomalyDetectorJobResponse = new JobResponse(detectorId);
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
    protected int scaleTaskSlots(ADTask adTask, TransportService transportService, ActionListener<JobResponse> scaleUpListener) {
        String detectorId = adTask.getConfigId();
        if (!scaleEntityTaskLane.tryAcquire()) {
            logger.debug("Can't get scaleEntityTaskLane semaphore");
            return 0;
        }
        try {
            int scaleDelta = detectorTaskSlotScaleDelta(detectorId);
            logger.debug("start to scale task slots for detector {} with delta {}", detectorId, scaleDelta);
            if (taskCacheManager.getAvailableNewEntityTaskLanes(detectorId) <= 0 && scaleDelta > 0) {
                // scale up to run more entities in parallel
                Instant lastScaleEntityTaskLaneTime = taskCacheManager.getLastScaleEntityTaskLaneTime(detectorId);
                if (lastScaleEntityTaskLaneTime == null) {
                    logger.debug("lastScaleEntityTaskLaneTime is null for detector {}", detectorId);
                    scaleEntityTaskLane.release();
                    return 0;
                }
                boolean lastScaleTimeExpired = lastScaleEntityTaskLaneTime
                    .plusMillis(SCALE_ENTITY_TASK_LANE_INTERVAL_IN_MILLIS)
                    .isBefore(Instant.now());
                if (lastScaleTimeExpired) {
                    taskCacheManager.refreshLastScaleEntityTaskLaneTime(detectorId);
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
                    int runningEntityCount = taskCacheManager.getRunningEntityCount(detectorId) + taskCacheManager
                        .getTempEntityCount(detectorId);
                    int assignedTaskSlots = taskCacheManager.getDetectorTaskSlots(detectorId);
                    int scaleDownDelta = Math.min(assignedTaskSlots - runningEntityCount, 0 - scaleDelta);
                    logger
                        .debug(
                            "Scale down task slots, scaleDelta: {}, assignedTaskSlots: {}, runningEntityCount: {}, scaleDownDelta: {}",
                            scaleDelta,
                            assignedTaskSlots,
                            runningEntityCount,
                            scaleDownDelta
                        );
                    taskCacheManager.scaleDownHCDetectorTaskSlots(detectorId, scaleDownDelta);
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
        DiscoveryNode[] eligibleDataNodes = hashRing.getNodesWithSameLocalVersion();
        int unfinishedEntities = taskCacheManager.getUnfinishedEntityCount(detectorId);
        int totalTaskSlots = eligibleDataNodes.length * maxAdBatchTaskPerNode;
        int taskLaneLimit = Math.min(unfinishedEntities, Math.min(totalTaskSlots, maxRunningEntitiesPerDetector));
        taskCacheManager.setDetectorTaskLaneLimit(detectorId, taskLaneLimit);

        int assignedTaskSlots = taskCacheManager.getDetectorTaskSlots(detectorId);
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
        int entityCount = taskCacheManager.getTopEntityCount(detectorId);
        int leftEntities = taskCacheManager.getPendingEntityCount(detectorId) + taskCacheManager.getRunningEntityCount(detectorId);
        return 1 - (float) leftEntities / entityCount;
    }

    /**
     * Get local task profiles of detector.
     * @param detectorId detector id
     * @return list of AD task profile
     */
    public ADTaskProfile getLocalADTaskProfilesByDetectorId(String detectorId) {
        List<String> tasksOfDetector = taskCacheManager.getTasksOfDetector(detectorId);
        ADTaskProfile detectorTaskProfile = null;

        String localNodeId = clusterService.localNode().getId();
        if (taskCacheManager.isHCTaskRunning(detectorId)) {
            detectorTaskProfile = new ADTaskProfile();
            if (taskCacheManager.isHCTaskCoordinatingNode(detectorId)) {
                detectorTaskProfile.setNodeId(localNodeId);
                detectorTaskProfile.setTaskId(taskCacheManager.getDetectorTaskId(detectorId));
                detectorTaskProfile.setDetectorTaskSlots(taskCacheManager.getDetectorTaskSlots(detectorId));
                detectorTaskProfile.setTotalEntitiesInited(taskCacheManager.topEntityInited(detectorId));
                detectorTaskProfile.setTotalEntitiesCount(taskCacheManager.getTopEntityCount(detectorId));
                detectorTaskProfile.setPendingEntitiesCount(taskCacheManager.getPendingEntityCount(detectorId));
                detectorTaskProfile.setRunningEntitiesCount(taskCacheManager.getRunningEntityCount(detectorId));
                detectorTaskProfile.setRunningEntities(taskCacheManager.getRunningEntities(detectorId));
                detectorTaskProfile.setTaskType(ADTaskType.HISTORICAL_HC_DETECTOR.name());
                Instant latestHCTaskRunTime = taskCacheManager.getLatestHCTaskRunTime(detectorId);
                if (latestHCTaskRunTime != null) {
                    detectorTaskProfile.setLatestHCTaskRunTime(latestHCTaskRunTime.toEpochMilli());
                }
            }
            if (tasksOfDetector.size() > 0) {
                List<EntityTaskProfile> entityTaskProfiles = new ArrayList<>();

                tasksOfDetector.forEach(taskId -> {
                    EntityTaskProfile entityTaskProfile = new EntityTaskProfile(
                        taskCacheManager.getTRcfModel(taskId).getForest().getTotalUpdates(),
                        taskCacheManager.isThresholdModelTrained(taskId),
                        taskCacheManager.getThresholdModelTrainingDataSize(taskId),
                        taskCacheManager.getModelSize(taskId),
                        localNodeId,
                        taskCacheManager.getEntity(taskId),
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
                    taskCacheManager.getDetectorTaskId(detectorId),
                    taskCacheManager.getTRcfModel(taskId).getForest().getTotalUpdates(),
                    taskCacheManager.isThresholdModelTrained(taskId),
                    taskCacheManager.getThresholdModelTrainingDataSize(taskId),
                    taskCacheManager.getModelSize(taskId),
                    localNodeId
                );
                // Single-flow detector only has 1 task slot.
                // Can't use adTaskCacheManager.getDetectorTaskSlots(detectorId) here as task may run on worker node.
                // Detector task slots stored in coordinating node cache.
                detectorTaskProfile.setDetectorTaskSlots(1);
            }
        }
        threadPool.executor(AD_BATCH_TASK_THREAD_POOL_NAME).execute(() -> {
            // Clean expired HC batch task run states as it may exists after HC historical analysis done if user cancel
            // before querying top entities done. We will clean it in hourly cron, check "maintainRunningHistoricalTasks"
            // method. Clean it up here when get task profile to release memory earlier.
            taskCacheManager.cleanExpiredHCBatchTaskRunStates();
        });
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
        ActionListener<JobResponse> listener
    ) {
        String detectorId = adTask.getConfigId();
        boolean removed = taskCacheManager.removeRunningEntity(detectorId, entity);
        if (removed && taskCacheManager.getPendingEntityCount(detectorId) > 0) {
            logger.debug("kick off next pending entities");
            this.runNextEntityForHCADHistorical(adTask, transportService, listener);
        } else {
            if (!taskCacheManager.hasEntity(detectorId)) {
                setHCDetectorTaskDone(adTask, TaskState.STOPPED, listener);
            }
        }
    }

    public String convertEntityToString(ADTask adTask) {
        if (adTask == null || !adTask.isHistoricalEntityTask()) {
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
        if (detector.hasMultipleCategories()) {
            try {
                XContentBuilder builder = entity.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
                return BytesReference.bytes(builder).utf8ToString();
            } catch (IOException e) {
                String error = "Failed to parse entity into string";
                logger.debug(error, e);
                throw new TimeSeriesException(error);
            }
        }
        if (detector.isHighCardinality()) {
            String categoryField = detector.getCategoryFields().get(0);
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
        if (detector.hasMultipleCategories()) {
            try {
                XContentParser parser = XContentType.JSON
                    .xContent()
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, entityValue);
                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
                return Entity.parse(parser);
            } catch (IOException e) {
                String error = "Failed to parse string into entity";
                logger.debug(error, e);
                throw new TimeSeriesException(error);
            }
        } else if (detector.isHighCardinality()) {
            return Entity.createSingleAttributeEntity(detector.getCategoryFields().get(0), entityValue);
        }
        throw new IllegalArgumentException("Fail to parse to Entity for single flow detector");
    }

    public int getLocalAdUsedBatchTaskSlot() {
        return taskCacheManager.getTotalBatchTaskCount();
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
        return taskCacheManager.getTotalDetectorTaskSlots();
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
        taskCacheManager.cleanExpiredHCBatchTaskRunStates();

        // Find owning node with highest AD version to make sure we only have 1 node maintain running historical tasks
        // and we use the latest logic.
        Optional<DiscoveryNode> owningNode = hashRing.getOwningNodeWithHighestVersion(AD_TASK_MAINTAINENCE_NODE_MODEL_ID);
        if (!owningNode.isPresent() || !clusterService.localNode().getId().equals(owningNode.get().getId())) {
            return;
        }
        logger.info("Start to maintain running historical tasks");

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(TimeSeriesTask.IS_LATEST_FIELD, true));
        query.filter(new TermsQueryBuilder(TimeSeriesTask.TASK_TYPE_FIELD, taskTypeToString(HISTORICAL_DETECTOR_TASK_TYPES)));
        query.filter(new TermsQueryBuilder(TimeSeriesTask.STATE_FIELD, NOT_ENDED_STATES));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // default maintain interval is 5 seconds, so maintain 10 tasks will take at least 50 seconds.
        sourceBuilder.query(query).sort(TimeSeriesTask.LAST_UPDATE_TIME_FIELD, SortOrder.DESC).size(size);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(DETECTION_STATE_INDEX);

        client.search(searchRequest, ActionListener.wrap(r -> {
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value() == 0) {
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
            resetHistoricalConfigTaskState(ImmutableList.of(adTask), () -> {
                logger.debug("Finished maintaining running historical task {}", adTask.getTaskId());
                maintainRunningHistoricalTask(taskQueue, transportService);
            }, transportService, ActionListener.wrap(r -> {
                logger.debug("Reset historical task state done for task {}, detector {}", adTask.getTaskId(), adTask.getConfigId());
            }, e -> { logger.error("Failed to reset historical task state for task " + adTask.getTaskId(), e); }));
        }, TimeValue.timeValueSeconds(DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS), AD_BATCH_TASK_THREAD_POOL_NAME);
    }

    @Override
    protected BiCheckedFunction<XContentParser, String, ADTask, IOException> getTaskParser() {
        return ADTask::parse;
    }

    @Override
    public void createRunOnceTaskAndCleanupStaleTasks(
        String configId,
        Config config,
        TransportService transportService,
        ActionListener<ADTask> listener
    ) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("AD has no run once yet");
    }

    /**
     * Get list of task types.
     * 1. If date range is null, will return all realtime task types
     * 2. If date range is not null, will return all historical detector level tasks types
     *
     * @param dateRange detection date range
     * @return list of AD task types
     */
    @Override
    public List<ADTaskType> getTaskTypes(DateRange dateRange, boolean runOnce) {
        if (dateRange == null) {
            return REALTIME_TASK_TYPES;
        } else {
            return HISTORICAL_DETECTOR_TASK_TYPES;
        }
    }

    /**
     * Reset latest config task state. Will reset both historical and realtime tasks.
     * [Important!] Make sure listener returns in function
     *
     * @param tasks tasks
     * @param function consumer function
     * @param transportService transport service
     * @param listener action listener
     * @param <T> response type of action listener
     */
    @Override
    protected <T> void resetLatestConfigTaskState(
        List<ADTask> tasks,
        Consumer<List<ADTask>> function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        List<TimeSeriesTask> runningHistoricalTasks = new ArrayList<>();
        List<TimeSeriesTask> runningRealtimeTasks = new ArrayList<>();

        for (TimeSeriesTask task : tasks) {
            if (!task.isHistoricalEntityTask() && !task.isDone()) {
                if (task.isRealTimeTask()) {
                    runningRealtimeTasks.add(task);
                } else if (task.isHistoricalTask()) {
                    runningHistoricalTasks.add(task);
                }
            }
        }

        // resetRealtimeCOnfigTaskState has to be the innermost function call as we return listener there
        // AD has no run once and forecasting has no historical. So the run once and historical reset
        // function only forwards function call and does not return listener
        resetHistoricalConfigTaskState(
            runningHistoricalTasks,
            () -> resetRealtimeConfigTaskState(runningRealtimeTasks, () -> function.accept(tasks), transportService, listener),
            transportService,
            listener
        );
    }

    @Override
    protected String triageState(Boolean hasResult, String error, Long rcfTotalUpdates) {
        if (rcfTotalUpdates < TimeSeriesSettings.NUM_MIN_SAMPLES) {
            return TaskState.INIT.name();
        } else {
            return TaskState.RUNNING.name();
        }
    }

    @Override
    protected boolean forbidOverrideChange(String configId, String newState, String oldState) {
        return TaskState.INIT.name().equals(newState) && TaskState.RUNNING.name().equals(oldState);
    }
}
