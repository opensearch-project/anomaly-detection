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

package org.opensearch.ad;

import static org.opensearch.ad.constant.CommonErrorMessages.CAN_NOT_FIND_LATEST_TASK;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.ad.transport.ProfileAction;
import org.opensearch.ad.transport.ProfileRequest;
import org.opensearch.ad.transport.RCFPollingAction;
import org.opensearch.ad.transport.RCFPollingRequest;
import org.opensearch.ad.transport.handler.AnomalyIndexHandler;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.threadpool.ThreadPool;

public class ExecuteADResultResponseRecorder {
    private static final Logger log = LogManager.getLogger(ExecuteADResultResponseRecorder.class);

    private AnomalyDetectionIndices anomalyDetectionIndices;
    private AnomalyIndexHandler<AnomalyResult> anomalyResultHandler;
    private ADTaskManager adTaskManager;
    private DiscoveryNodeFilterer nodeFilter;
    private ThreadPool threadPool;
    private Client client;

    public ExecuteADResultResponseRecorder(
        AnomalyDetectionIndices anomalyDetectionIndices,
        AnomalyIndexHandler<AnomalyResult> anomalyResultHandler,
        ADTaskManager adTaskManager,
        DiscoveryNodeFilterer nodeFilter,
        ThreadPool threadPool,
        Client client
    ) {
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.anomalyResultHandler = anomalyResultHandler;
        this.adTaskManager = adTaskManager;
        this.nodeFilter = nodeFilter;
        this.threadPool = threadPool;
        this.client = client;
    }

    public void indexAnomalyResult(
        Instant detectionStartTime,
        Instant executionStartTime,
        AnomalyResultResponse response,
        AnomalyDetector detector
    ) {
        String detectorId = detector.getDetectorId();
        try {
            // skipping writing to the result index if not necessary
            // For a single-entity detector, the result is not useful if error is null
            // and rcf score (thus anomaly grade/confidence) is null.
            // For a HCAD detector, we don't need to save on the detector level.
            // We return 0 or Double.NaN rcf score if there is no error.
            if ((response.getAnomalyScore() <= 0 || Double.isNaN(response.getAnomalyScore())) && response.getError() == null) {
                updateRealtimeTask(response, detectorId);
                return;
            }
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) detector.getWindowDelay();
            Instant dataStartTime = detectionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            User user = detector.getUser();

            if (response.getError() != null) {
                log.info("Anomaly result action run successfully for {} with error {}", detectorId, response.getError());
            }

            AnomalyResult anomalyResult = response
                .toAnomalyResult(
                    detectorId,
                    dataStartTime,
                    dataEndTime,
                    executionStartTime,
                    Instant.now(),
                    anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT),
                    user,
                    response.getError()
                );

            String resultIndex = detector.getResultIndex();
            anomalyResultHandler.index(anomalyResult, detectorId, resultIndex);
            updateRealtimeTask(response, detectorId);
        } catch (EndRunException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + detectorId, e);
        }
    }

    /**
     * Update real time task (one document per detector in state index). If the real-time task has no changes compared with local cache,
     * the task won't update. Task only updates when the state changed, or any error happened, or AD job stopped. Task is mainly consumed
     * by the front-end to track detector status. For single-stream detectors, we embed model total updates in AnomalyResultResponse and
     * update state accordingly. For HCAD, we won't wait for model finishing updating before returning a response to the job scheduler
     * since it might be long before all entities finish execution. So we don't embed model total updates in AnomalyResultResponse.
     * Instead, we issue a profile request to poll each model node and get the maximum total updates among all models.
     * @param response response returned from executing AnomalyResultAction
     * @param detectorId Detector Id
     */
    private void updateRealtimeTask(AnomalyResultResponse response, String detectorId) {
        if (response.isHCDetector() != null && response.isHCDetector()) {
            if (adTaskManager.skipUpdateHCRealtimeTask(detectorId, response.getError())) {
                return;
            }
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            Set<DetectorProfileName> profiles = new HashSet<>();
            profiles.add(DetectorProfileName.INIT_PROGRESS);
            ProfileRequest profileRequest = new ProfileRequest(detectorId, profiles, true, dataNodes);
            Runnable profileHCInitProgress = () -> {
                client.execute(ProfileAction.INSTANCE, profileRequest, ActionListener.wrap(r -> {
                    log.debug("Update latest realtime task for HC detector {}, total updates: {}", detectorId, r.getTotalUpdates());
                    updateLatestRealtimeTask(
                        detectorId,
                        null,
                        r.getTotalUpdates(),
                        response.getDetectorIntervalInMinutes(),
                        response.getError()
                    );
                }, e -> { log.error("Failed to update latest realtime task for " + detectorId, e); }));
            };
            if (!adTaskManager.isHCRealtimeTaskStartInitializing(detectorId)) {
                // real time init progress is 0 may mean this is a newly started detector
                // Delay real time cache update by one minute. If we are in init status, the delay may give the model training time to
                // finish. We can change the detector running immediately instead of waiting for the next interval.
                threadPool.schedule(profileHCInitProgress, new TimeValue(60, TimeUnit.SECONDS), AnomalyDetectorPlugin.AD_THREAD_POOL_NAME);
            } else {
                profileHCInitProgress.run();
            }

        } else {
            log
                .debug(
                    "Update latest realtime task for single stream detector {}, total updates: {}",
                    detectorId,
                    response.getRcfTotalUpdates()
                );
            updateLatestRealtimeTask(
                detectorId,
                null,
                response.getRcfTotalUpdates(),
                response.getDetectorIntervalInMinutes(),
                response.getError()
            );
        }
    }

    private void updateLatestRealtimeTask(
        String detectorId,
        String taskState,
        Long rcfTotalUpdates,
        Long detectorIntervalInMinutes,
        String error
    ) {
        // Don't need info as this will be printed repeatedly in each interval
        adTaskManager
            .updateLatestRealtimeTaskOnCoordinatingNode(
                detectorId,
                taskState,
                rcfTotalUpdates,
                detectorIntervalInMinutes,
                error,
                ActionListener.wrap(r -> {
                    if (r != null) {
                        log.debug("Updated latest realtime task successfully for detector {}, taskState: {}", detectorId, taskState);
                    }
                }, e -> {
                    if ((e instanceof ResourceNotFoundException) && e.getMessage().contains(CAN_NOT_FIND_LATEST_TASK)) {
                        // Clear realtime task cache, will recreate AD task in next run, check AnomalyResultTransportAction.
                        log.error("Can't find latest realtime task of detector " + detectorId);
                        adTaskManager.removeRealtimeTaskCache(detectorId);
                    } else {
                        log.error("Failed to update latest realtime task for detector " + detectorId, e);
                    }
                })
            );
    }

    /**
     * The function is not only indexing the result with the exception, but also updating the task state after
     * 60s if the exception is related to cold start (index not found exceptions) for a single stream detector.
     *
     * @param detectionStartTime execution start time
     * @param executionStartTime execution end time
     * @param errorMessage Error message to record
     * @param taskState AD task state (e.g., stopped)
     * @param detector Detector config accessor
     */
    public void indexAnomalyResultException(
        Instant detectionStartTime,
        Instant executionStartTime,
        String errorMessage,
        String taskState,
        AnomalyDetector detector
    ) {
        String detectorId = detector.getDetectorId();
        try {
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) detector.getWindowDelay();
            Instant dataStartTime = detectionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            User user = detector.getUser();

            AnomalyResult anomalyResult = new AnomalyResult(
                detectorId,
                null, // no task id
                new ArrayList<FeatureData>(),
                dataStartTime,
                dataEndTime,
                executionStartTime,
                Instant.now(),
                errorMessage,
                null, // single-stream detectors have no entity
                user,
                anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT),
                null // no model id
            );
            String resultIndex = detector.getResultIndex();
            if (resultIndex != null && !anomalyDetectionIndices.doesIndexExist(resultIndex)) {
                // Set result index as null, will write exception to default result index.
                anomalyResultHandler.index(anomalyResult, detectorId, null);
            } else {
                anomalyResultHandler.index(anomalyResult, detectorId, resultIndex);
            }

            if (errorMessage.contains(CommonErrorMessages.NO_CHECKPOINT_ERR_MSG) && !detector.isMultiCategoryDetector()) {
                // single stream detector raises ResourceNotFoundException containing CommonErrorMessages.NO_CHECKPOINT_ERR_MSG
                // when there is no checkpoint.
                // Delay real time cache update by one minute so we will have trained models by then and update the state
                // document accordingly.
                threadPool.schedule(() -> {
                    RCFPollingRequest request = new RCFPollingRequest(detectorId);
                    client.execute(RCFPollingAction.INSTANCE, request, ActionListener.wrap(rcfPollResponse -> {
                        long totalUpdates = rcfPollResponse.getTotalUpdates();
                        // if there are updates, don't record failures
                        updateLatestRealtimeTask(
                            detectorId,
                            taskState,
                            totalUpdates,
                            detector.getDetectorIntervalInMinutes(),
                            totalUpdates > 0 ? "" : errorMessage
                        );
                    }, e -> {
                        log.error("Fail to execute RCFRollingAction", e);
                        updateLatestRealtimeTask(detectorId, taskState, null, null, errorMessage);
                    }));
                }, new TimeValue(60, TimeUnit.SECONDS), AnomalyDetectorPlugin.AD_THREAD_POOL_NAME);
            } else {
                updateLatestRealtimeTask(detectorId, taskState, null, null, errorMessage);
            }

        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + detectorId, e);
        }
    }

}
