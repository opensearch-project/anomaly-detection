/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionType;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHits;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.ProfileRequest;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.ExceptionUtil;

public abstract class ExecuteResultResponseRecorder<IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, IndexableResultType extends IndexableResult, ProfileActionType extends ActionType<ProfileResponse>> {

    private static final Logger log = LogManager.getLogger(ExecuteResultResponseRecorder.class);

    protected IndexManagementType indexManagement;
    private ResultBulkIndexingHandler<IndexableResultType, IndexType, IndexManagementType> resultHandler;
    protected TaskManagerType taskManager;
    private DiscoveryNodeFilterer nodeFilter;
    private ThreadPool threadPool;
    private String threadPoolName;
    private Client client;
    private NodeStateManager nodeStateManager;
    private Clock clock;
    protected IndexType resultIndex;
    private AnalysisType analysisType;
    private ProfileActionType profileAction;

    public ExecuteResultResponseRecorder(
        IndexManagementType indexManagement,
        ResultBulkIndexingHandler<IndexableResultType, IndexType, IndexManagementType> resultHandler,
        TaskManagerType taskManager,
        DiscoveryNodeFilterer nodeFilter,
        ThreadPool threadPool,
        String threadPoolName,
        Client client,
        NodeStateManager nodeStateManager,
        Clock clock,
        IndexType resultIndex,
        AnalysisType analysisType,
        ProfileActionType profileAction
    ) {
        this.indexManagement = indexManagement;
        this.resultHandler = resultHandler;
        this.taskManager = taskManager;
        this.nodeFilter = nodeFilter;
        this.threadPool = threadPool;
        this.threadPoolName = threadPoolName;
        this.client = client;
        this.nodeStateManager = nodeStateManager;
        this.clock = clock;
        this.resultIndex = resultIndex;
        this.analysisType = analysisType;
        this.profileAction = profileAction;
    }

    public void indexResult(
        Instant detectionStartTime,
        Instant executionStartTime,
        ResultResponse<IndexableResultType> response,
        Config config
    ) {
        String configId = config.getId();
        try {
            if (!response.shouldSave()) {
                updateRealtimeTask(response, configId, clock);
                return;
            }
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) config.getWindowDelay();
            Instant dataStartTime = detectionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            User user = config.getUser();

            if (response.getError() != null) {
                log.info("Result action run for {} with error {}", configId, response.getError());
            }

            List<IndexableResultType> analysisResults = response
                .toIndexableResults(
                    config,
                    dataStartTime,
                    dataEndTime,
                    executionStartTime,
                    clock.instant(),
                    indexManagement.getSchemaVersion(resultIndex),
                    user,
                    response.getError()
                );

            String resultIndex = config.getCustomResultIndexOrAlias();
            resultHandler
                .bulk(
                    resultIndex,
                    analysisResults,
                    configId,
                    ActionListener
                        .<BulkResponse>wrap(
                            r -> {},
                            exception -> log.error(String.format(Locale.ROOT, "Fail to bulk for %s", configId), exception)
                        )
                );
            updateRealtimeTask(response, configId, clock);
        } catch (EndRunException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to index result for " + configId, e);
        }
    }

    /**
     *
     * If result action is handled asynchronously, response won't contain the result.
     * This function wait some time before fetching update.
     * One side-effect is if the config is already deleted the latest task will get deleted too.
     * This delayed update can cause ResourceNotFoundException.
     *
     * @param response response returned from executing AnomalyResultAction
     * @param configId config Id
     * @param clock clock to get current time
     */
    protected void delayedUpdate(ResultResponse<IndexableResultType> response, String configId, Clock clock) {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        Set<ProfileName> profiles = new HashSet<>();
        profiles.add(ProfileName.INIT_PROGRESS);
        ProfileRequest profileRequest = new ProfileRequest(configId, profiles, dataNodes);
        Runnable profileInitProgress = () -> {
            nodeStateManager.getConfig(configId, analysisType, false, ActionListener.wrap(configOptional -> {
                if (!configOptional.isPresent()) {
                    log.warn("fail to get config");
                    return;
                }

                Config config = configOptional.get();
                if (config.isLongInterval()) {
                    log.info("Update latest realtime task for long-interval config {}", configId);
                    updateLatestRealtimeTask(configId, null, 0L, response.getConfigIntervalInMinutes(), response.getError(), clock);
                } else {
                    client.execute(profileAction, profileRequest, ActionListener.wrap(r -> {
                        log.info("Update latest realtime task for config {}, total updates: {}", configId, r.getTotalUpdates());
                        updateLatestRealtimeTask(
                            configId,
                            null,
                            r.getTotalUpdates(),
                            response.getConfigIntervalInMinutes(),
                            response.getError(),
                            clock
                        );
                    }, e -> { log.error("Failed to update latest realtime task for " + configId, e); }));
                }
            }, e -> log.warn("fail to get config", e)));
        };
        if (!taskManager.isRealtimeTaskStartInitializing(configId)) {
            // real time init progress is 0 may mean this is a newly started detector
            // Delay real time cache update by one minute. If we are in init status, the delay may give the model training time to
            // finish. We can change the detector running immediately instead of waiting for the next interval.
            threadPool.schedule(profileInitProgress, new TimeValue(60, TimeUnit.SECONDS), threadPoolName);
        } else {
            profileInitProgress.run();
        }
    }

    protected void updateLatestRealtimeTask(
        String configId,
        String taskState,
        Long rcfTotalUpdates,
        Long configIntervalInMinutes,
        String error,
        Clock clock
    ) {
        ActionListener<UpdateResponse> listener = ActionListener.wrap(r -> {
            if (r != null) {
                log.info("Updated latest realtime task successfully for config {}, taskState: {}", configId, taskState);
            }
        }, e -> {
            if ((e instanceof ResourceNotFoundException) && e.getMessage().contains(CommonMessages.CAN_NOT_FIND_LATEST_TASK)) {
                // Clear realtime task cache, will recreate task in next run, check ADResultProcessor.
                log.error("Can't find latest realtime task of config " + configId);
                taskManager.removeRealtimeTaskCache(configId);
            } else {
                log.error("Failed to update latest realtime task for config " + configId, e);
            }
        });

        hasRecentResult(
            configId,
            configIntervalInMinutes,
            error,
            clock,
            ActionListener
                .wrap(
                    r -> taskManager
                        .updateLatestRealtimeTaskOnCoordinatingNode(
                            configId,
                            taskState,
                            rcfTotalUpdates,
                            configIntervalInMinutes,
                            error,
                            r,
                            listener
                        ),
                    e -> {
                        log.error("Fail to confirm rcf update", e);
                        taskManager
                            .updateLatestRealtimeTaskOnCoordinatingNode(
                                configId,
                                taskState,
                                rcfTotalUpdates,
                                configIntervalInMinutes,
                                error,
                                null,
                                listener
                            );
                    }
                )
        );
    }

    /**
     * The function is not only indexing the result with the exception, but also updating the task state after
     * 60s if the exception is related to cold start (index not found exceptions) for a single stream detector.
     *
     * @param executeStartTime execution start time
     * @param executeEndTime execution end time
     * @param errorMessage Error message to record
     * @param taskState task state (e.g., stopped)
     * @param config config accessor
     * @param clock clock to get current time
     */
    public void indexResultException(
        Instant executeStartTime,
        Instant executeEndTime,
        String errorMessage,
        String taskState,
        Config config,
        Clock clock
    ) {
        String configId = config.getId();
        try {
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) config.getWindowDelay();
            Instant dataStartTime = executeStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executeEndTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            User user = config.getUser();

            IndexableResultType resultToSave = createErrorResult(configId, dataStartTime, dataEndTime, executeEndTime, errorMessage, user);
            String resultIndexOrAlias = config.getCustomResultIndexOrAlias();
            resultHandler.index(resultToSave, configId, resultIndexOrAlias);

            updateLatestRealtimeTask(configId, taskState, null, null, errorMessage, clock);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + configId, e);
        }
    }

    /**
     * Check whether result index has results in the past two intervals. We don't check for one interval
     * considering there might be refresh/flush time between writing result and result being available
     * for search.
     *
     * @param configId Config id
     * @param overrideIntervalMinutes config interval in minutes
     * @param error Error
     * @param clock Clock to get current time
     * @param listener Callback to return whether any result has been generated in the past two intervals.
     */
    private void hasRecentResult(
        String configId,
        Long overrideIntervalMinutes,
        String error,
        Clock clock,
        ActionListener<Boolean> listener
    ) {
        // run once does not need to cache
        nodeStateManager.getConfig(configId, analysisType, false, ActionListener.wrap(configOptional -> {
            if (!configOptional.isPresent()) {
                listener.onFailure(new TimeSeriesException(configId, "fail to get config"));
                return;
            }

            Config config = configOptional.get();

            long intervalMinutes = overrideIntervalMinutes != null ? overrideIntervalMinutes : config.getIntervalInMinutes();

            ProfileUtil
                .confirmRealtimeResultStatus(
                    config,
                    clock.millis() - 2 * intervalMinutes * 60000,
                    client,
                    analysisType,
                    ActionListener.wrap(searchResponse -> {
                        ActionListener.completeWith(listener, () -> {
                            SearchHits hits = searchResponse.getHits();
                            Boolean hasResult = false;
                            if (hits.getTotalHits().value > 0L) {
                                // true if we have already had results after job enabling time
                                hasResult = true;
                            }
                            return hasResult;
                        });
                    }, exception -> {
                        if (ExceptionUtil.isIndexNotAvailable(exception)) {
                            // result index is not created yet
                            listener.onResponse(false);
                        } else {
                            listener.onFailure(exception);
                        }
                    })
                );
        }, e -> listener.onFailure(new TimeSeriesException(configId, "fail to get config"))));
    }

    protected abstract IndexableResultType createErrorResult(
        String configId,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executeEndTime,
        String errorMessage,
        User user
    );

    // protected abstract void updateRealtimeTask(ResultResponseType response, String configId);
    protected abstract void updateRealtimeTask(ResultResponse<IndexableResultType> response, String configId, Clock clock);
}
