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

package org.opensearch.ad.transport;

import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.common.inject.Inject;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableMap;

public class ForwardADTaskTransportAction extends HandledTransportAction<ForwardADTaskRequest, JobResponse> {
    private final Logger logger = LogManager.getLogger(ForwardADTaskTransportAction.class);
    private final TransportService transportService;
    private final ADTaskManager adTaskManager;
    private final ADTaskCacheManager adTaskCacheManager;
    private final ADIndexJobActionHandler indexJobHander;

    // =========================================================
    // Fields below contains cache for realtime AD on coordinating
    // node. We need to clean up these caches when receive FINISHED
    // action for realtime task.
    // =========================================================
    // NodeStateManager caches anomaly detector's backpressure counter for realtime detection.
    private final NodeStateManager stateManager;
    // FeatureManager caches anomaly detector's feature data points for shingling of realtime detection.
    private final FeatureManager featureManager;

    @Inject
    public ForwardADTaskTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ADTaskManager adTaskManager,
        ADTaskCacheManager adTaskCacheManager,
        FeatureManager featureManager,
        NodeStateManager stateManager,
        ADIndexJobActionHandler indexJobHander
    ) {
        super(ForwardADTaskAction.NAME, transportService, actionFilters, ForwardADTaskRequest::new);
        this.adTaskManager = adTaskManager;
        this.transportService = transportService;
        this.adTaskCacheManager = adTaskCacheManager;
        this.featureManager = featureManager;
        this.stateManager = stateManager;
        this.indexJobHander = indexJobHander;
    }

    @Override
    protected void doExecute(Task task, ForwardADTaskRequest request, ActionListener<JobResponse> listener) {
        ADTaskAction adTaskAction = request.getAdTaskAction();
        AnomalyDetector detector = request.getDetector();
        DateRange detectionDateRange = request.getDetectionDateRange();
        String detectorId = detector.getId();
        ADTask adTask = request.getAdTask();
        User user = request.getUser();
        Integer availableTaskSlots = request.getAvailableTaskSLots();

        String entityValue = adTaskManager.convertEntityToString(adTask);

        switch (adTaskAction) {
            case APPLY_FOR_TASK_SLOTS:
                logger.debug("Received APPLY_FOR_TASK_SLOTS action for detector {}", detectorId);
                adTaskManager.checkTaskSlots(adTask, detector, detectionDateRange, user, ADTaskAction.START, transportService, listener);
                break;
            case CHECK_AVAILABLE_TASK_SLOTS:
                logger.debug("Received CHECK_AVAILABLE_TASK_SLOTS action for detector {}", detectorId);
                adTaskManager
                    .checkTaskSlots(
                        adTask,
                        detector,
                        detectionDateRange,
                        user,
                        ADTaskAction.SCALE_ENTITY_TASK_SLOTS,
                        transportService,
                        listener
                    );
                break;
            case START:
                // Start historical analysis for detector
                logger.debug("Received START action for detector {}", detectorId);
                indexJobHander.startConfig(detector, detectionDateRange, user, transportService, ActionListener.wrap(r -> {
                    adTaskCacheManager.setDetectorTaskSlots(detector.getId(), availableTaskSlots);
                    listener.onResponse(r);
                }, e -> listener.onFailure(e)));
                break;
            case NEXT_ENTITY:
                logger.debug("Received NEXT_ENTITY action for detector {}, task {}", detectorId, adTask.getTaskId());
                // Run next entity for HC detector historical analysis.
                if (detector.isHighCardinality()) { // AD task could be HC detector level task or entity task
                    adTaskCacheManager.removeRunningEntity(detectorId, entityValue);
                    if (!adTaskCacheManager.hasEntity(detectorId)) {
                        adTaskCacheManager.setDetectorTaskSlots(detectorId, 0);
                        logger.info("Historical HC detector done, will remove from cache, detector id:{}", detectorId);
                        listener.onResponse(new JobResponse(detectorId));
                        TaskState state = !adTask.isHistoricalEntityTask() && adTask.getError() != null
                            ? TaskState.FAILED
                            : TaskState.FINISHED;
                        adTaskManager.setHCDetectorTaskDone(adTask, state, listener);
                    } else {
                        logger.debug("Run next entity for detector " + detectorId);
                        adTaskManager.runNextEntityForHCADHistorical(adTask, transportService, listener);
                        adTaskManager
                            .updateADHCDetectorTask(
                                detectorId,
                                adTask.getParentTaskId(),
                                ImmutableMap
                                    .of(
                                        TimeSeriesTask.STATE_FIELD,
                                        TaskState.RUNNING.name(),
                                        TimeSeriesTask.TASK_PROGRESS_FIELD,
                                        adTaskManager.hcDetectorProgress(detectorId),
                                        TimeSeriesTask.ERROR_FIELD,
                                        adTask.getError() != null ? adTask.getError() : ""
                                    )
                            );
                    }
                } else {
                    logger
                        .warn(
                            "Can only handle HC entity task for NEXT_ENTITY action, taskId:{} , taskType:{}",
                            adTask.getTaskId(),
                            adTask.getTaskType()
                        );
                    listener.onFailure(new IllegalArgumentException("Unsupported task"));
                }
                break;
            case PUSH_BACK_ENTITY:
                logger.debug("Received PUSH_BACK_ENTITY action for detector {}, task {}", detectorId, adTask.getTaskId());
                // Push back entity to pending entities queue and run next entity.
                if (adTask.isHistoricalEntityTask()) { // AD task must be entity level task.
                    adTaskCacheManager.removeRunningEntity(detectorId, entityValue);
                    if (adTaskManager.isRetryableError(adTask.getError())
                        && !adTaskCacheManager.exceedRetryLimit(adTask.getConfigId(), adTask.getTaskId())) {
                        // If retryable exception happens when run entity task, will push back entity to the end
                        // of pending entities queue, then we can retry it later.
                        adTaskCacheManager.pushBackEntity(adTask.getTaskId(), adTask.getConfigId(), entityValue);
                    } else {
                        // If exception is not retryable or exceeds retry limit, will remove this entity.
                        adTaskCacheManager.removeEntity(adTask.getConfigId(), entityValue);
                        logger.warn("Entity task failed, task id: {}, entity: {}", adTask.getTaskId(), adTask.getEntity().toString());
                    }
                    if (!adTaskCacheManager.hasEntity(detectorId)) {
                        adTaskCacheManager.setDetectorTaskSlots(detectorId, 0);
                        adTaskManager.setHCDetectorTaskDone(adTask, TaskState.FINISHED, listener);
                    } else {
                        logger.debug("scale task slots for PUSH_BACK_ENTITY, detector {} task {}", detectorId, adTask.getTaskId());
                        int taskSlots = adTaskCacheManager.scaleDownHCDetectorTaskSlots(detectorId, 1);
                        if (taskSlots == 1) {
                            logger.debug("After scale down, only 1 task slot reserved for detector {}, run next entity", detectorId);
                            adTaskManager.runNextEntityForHCADHistorical(adTask, transportService, listener);
                        }
                        listener.onResponse(new JobResponse(adTask.getTaskId()));
                    }
                } else {
                    logger.warn("Can only push back entity task");
                    listener.onFailure(new IllegalArgumentException("Can only push back entity task"));
                }
                break;
            case SCALE_ENTITY_TASK_SLOTS:
                logger.debug("Received SCALE_ENTITY_TASK_LANE action for detector {}", detectorId);
                // Check current available task slots and scale entity task lane.
                if (availableTaskSlots != null && availableTaskSlots > 0) {
                    int newSlots = Math.min(availableTaskSlots, adTaskManager.detectorTaskSlotScaleDelta(detectorId));
                    if (newSlots > 0) {
                        adTaskCacheManager.setAllowedRunningEntities(detectorId, newSlots);
                        adTaskCacheManager.scaleUpDetectorTaskSlots(detectorId, newSlots);
                    }
                }
                listener.onResponse(new JobResponse(detector.getId()));
                break;
            case CANCEL:
                logger.debug("Received CANCEL action for detector {}", detectorId);
                // Cancel HC detector's historical analysis.
                // Don't support single detector for this action as single entity task will be cancelled directly
                // on worker node.
                if (detector.isHighCardinality()) {
                    adTaskCacheManager.clearPendingEntities(detectorId);
                    adTaskCacheManager.removeRunningEntity(detectorId, entityValue);
                    if (!adTaskCacheManager.hasEntity(detectorId) || !adTask.isHistoricalEntityTask()) {
                        adTaskManager.setHCDetectorTaskDone(adTask, TaskState.STOPPED, listener);
                    }
                    listener.onResponse(new JobResponse(adTask.getTaskId()));
                } else {
                    listener.onFailure(new IllegalArgumentException("Only support cancel HC now"));
                }
                break;
            case CLEAN_STALE_RUNNING_ENTITIES:
                logger.debug("Received CLEAN_STALE_RUNNING_ENTITIES action for detector {}", detectorId);
                // Clean stale running entities of HC detector. For example, some worker node crashed or failed to send
                // entity task done message to coordinating node, then coordinating node can't remove running entity
                // from cache. We will check task profile when get task. If some entities exist in coordinating cache but
                // doesn't exist in worker node's cache, we will clean up these stale running entities on coordinating node.
                List<String> staleRunningEntities = request.getStaleRunningEntities();
                logger
                    .debug(
                        "Clean stale running entities of task {}, staleRunningEntities: {}",
                        adTask.getTaskId(),
                        Arrays.toString(staleRunningEntities.toArray(new String[0]))
                    );
                for (String entity : staleRunningEntities) {
                    adTaskManager.removeStaleRunningEntity(adTask, entity, transportService, listener);
                }
                listener.onResponse(new JobResponse(adTask.getTaskId()));
                break;
            case CLEAN_CACHE:
                boolean historicalTask = adTask.isHistoricalTask();
                logger
                    .debug(
                        "Received CLEAN_CACHE action for detector {}, taskId: {}, historical: {}",
                        detectorId,
                        adTask.getTaskId(),
                        historicalTask
                    );
                if (historicalTask) {
                    // Don't clear task cache if still has running entity. CLEAN_STALE_RUNNING_ENTITIES will clean
                    // stale running entity.
                    adTaskCacheManager.removeHistoricalTaskCacheIfNoRunningEntity(detectorId);
                } else {
                    adTaskCacheManager.removeRealtimeTaskCache(detectorId);
                    // If hash ring changed like new node added when scale out, the realtime job coordinating node may
                    // change, then we should clean up cache on old coordinating node.
                    stateManager.clear(detectorId);
                }
                listener.onResponse(new JobResponse(detector.getId()));
                break;
            default:
                listener.onFailure(new OpenSearchStatusException("Unsupported AD task action " + adTaskAction, RestStatus.BAD_REQUEST));
                break;
        }

    }
}
