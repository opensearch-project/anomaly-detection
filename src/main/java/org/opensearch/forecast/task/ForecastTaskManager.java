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

package org.opensearch.forecast.task;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.forecast.constant.ForecastCommonMessages.FORECASTER_IS_RUNNING;
import static org.opensearch.forecast.indices.ForecastIndexManagement.ALL_FORECAST_RESULTS_INDEX_PATTERN;
import static org.opensearch.forecast.model.ForecastTask.FORECASTER_ID_FIELD;
import static org.opensearch.forecast.model.ForecastTaskType.REALTIME_TASK_TYPES;
import static org.opensearch.forecast.settings.ForecastSettings.DELETE_FORECAST_RESULT_WHEN_DELETE_FORECASTER;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_OLD_TASK_DOCS_PER_FORECASTER;
import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.AD_BATCH_TASK_THREAD_POOL_NAME;
import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME;
import static org.opensearch.timeseries.model.TimeSeriesTask.TASK_ID_FIELD;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.common.exception.DuplicateTaskException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.function.ResponseTransformer;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.transport.TransportService;

public class ForecastTaskManager extends
    TaskManager<TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement> {
    private final Logger logger = LogManager.getLogger(ForecastTaskManager.class);

    public ForecastTaskManager(
        TaskCacheManager forecastTaskCacheManager,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ForecastIndexManagement forecastIndices,
        ClusterService clusterService,
        Settings settings,
        ThreadPool threadPool,
        NodeStateManager nodeStateManager
    ) {
        super(
            forecastTaskCacheManager,
            clusterService,
            client,
            ForecastIndex.STATE.getIndexName(),
            ForecastTaskType.REALTIME_TASK_TYPES,
            Collections.emptyList(),
            ForecastTaskType.RUN_ONCE_TASK_TYPES,
            forecastIndices,
            nodeStateManager,
            AnalysisType.FORECAST,
            xContentRegistry,
            FORECASTER_ID_FIELD,
            MAX_OLD_TASK_DOCS_PER_FORECASTER,
            settings,
            threadPool,
            ALL_FORECAST_RESULTS_INDEX_PATTERN,
            FORECAST_THREAD_POOL_NAME,
            DELETE_FORECAST_RESULT_WHEN_DELETE_FORECASTER,
            TaskState.INACTIVE
        );
    }

    /**
     * Init realtime task cache Realtime forecast depending on job scheduler to choose node (job coordinating node)
     * to run forecast job. Nodes have primary or replica shard of the job index are candidate to run forecast job.
     * Job scheduler will build hash ring on these candidate nodes and choose one to run forecast job.
     * If forecast job index shard relocated, for example new node added into cluster, then job scheduler will
     * rebuild hash ring and may choose different node to run forecast job. So we need to init realtime task cache
     * on new forecast job coordinating node.
     *
     * If realtime task cache inited for the first time on this node, listener will return true; otherwise
     * listener will return false.
     *
     * We don't clean up realtime task cache on old coordinating node as HourlyCron will clear cache on old coordinating node.
     *
     * @param forecasterId forecaster id
     * @param forecaster forecaster
     * @param transportService transport service
     * @param listener listener
     */
    @Override
    public void initRealtimeTaskCacheAndCleanupStaleCache(
        String forecasterId,
        Config forecaster,
        TransportService transportService,
        ActionListener<Boolean> listener
    ) {
        try {
            if (taskCacheManager.getRealtimeTaskCache(forecasterId) != null) {
                listener.onResponse(false);
                return;
            }

            getAndExecuteOnLatestConfigLevelTask(forecasterId, REALTIME_TASK_TYPES, (forecastTaskOptional) -> {
                if (forecastTaskOptional.isEmpty()) {
                    logger.debug("Can't find realtime task for forecaster {}, init realtime task cache directly", forecasterId);
                    ExecutorFunction function = () -> createNewTask(
                        forecaster,
                        null,
                        false,
                        forecaster.getUser(),
                        clusterService.localNode().getId(),
                        TaskState.CREATED,
                        ActionListener.wrap(r -> {
                            logger.info("Recreate realtime task successfully for forecaster {}", forecasterId);
                            taskCacheManager.initRealtimeTaskCache(forecasterId, forecaster.getIntervalInMilliseconds());
                            listener.onResponse(true);
                        }, e -> {
                            logger.error("Failed to recreate realtime task for forecaster " + forecasterId, e);
                            listener.onFailure(e);
                        })
                    );
                    recreateRealtimeTaskBeforeExecuting(function, listener);
                    return;
                }

                logger.info("Init realtime task cache for forecaster {}", forecasterId);
                taskCacheManager.initRealtimeTaskCache(forecasterId, forecaster.getIntervalInMilliseconds());
                listener.onResponse(true);
            }, transportService, false, listener);
        } catch (Exception e) {
            logger.error("Failed to init realtime task cache for " + forecasterId, e);
            listener.onFailure(e);
        }
    }

    /**
     * Update forecast task with specific fields.
     *
     * @param taskId forecast task id
     * @param updatedFields updated fields, key: filed name, value: new value
     */
    public void updateForecastTask(String taskId, Map<String, Object> updatedFields) {
        updateForecastTask(taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated forecast task successfully: {}, task id: {}", response.status(), taskId);
            } else {
                logger.error("Failed to update forecast task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task: " + taskId, e); }));
    }

    /**
     * Update forecast task for specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateForecastTask(String taskId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(ForecastIndex.STATE.getIndexName(), taskId);
        Map<String, Object> updatedContent = new HashMap<>();
        updatedContent.putAll(updatedFields);
        updatedContent.put(TimeSeriesTask.LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
        updateRequest.doc(updatedContent);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.update(updateRequest, listener);
    }

    private void recreateRealtimeTaskBeforeExecuting(ExecutorFunction function, ActionListener<Boolean> listener) {
        if (indexManagement.doesStateIndexExist()) {
            function.execute();
        } else {
            // If forecast state index doesn't exist, create index and execute function.
            indexManagement.initStateIndex(ActionListener.wrap(r -> {
                if (r.isAcknowledged()) {
                    logger.info("Created {} with mappings.", ForecastIndex.STATE.getIndexName());
                    function.execute();
                } else {
                    String error = String
                        .format(Locale.ROOT, CommonMessages.CREATE_INDEX_NOT_ACKNOWLEDGED, ForecastIndex.STATE.getIndexName());
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
     * Poll deleted detector task from cache and delete its child tasks and AD results.
     */
    @Override
    public void cleanChildTasksAndResultsOfDeletedTask() {
        if (!taskCacheManager.hasDeletedTask()) {
            return;
        }
        threadPool.schedule(() -> {
            String taskId = taskCacheManager.pollDeletedTask();
            if (taskId == null) {
                return;
            }
            DeleteByQueryRequest deleteForecastResultsRequest = new DeleteByQueryRequest(ALL_FORECAST_RESULTS_INDEX_PATTERN);
            deleteForecastResultsRequest.setQuery(new TermsQueryBuilder(TASK_ID_FIELD, taskId));
            client.execute(DeleteByQueryAction.INSTANCE, deleteForecastResultsRequest, ActionListener.wrap(res -> {
                logger.debug("Successfully deleted forecast results of task " + taskId);
                DeleteByQueryRequest deleteChildTasksRequest = new DeleteByQueryRequest(ForecastIndex.STATE.getIndexName());
                deleteChildTasksRequest.setQuery(new TermsQueryBuilder(TimeSeriesTask.PARENT_TASK_ID_FIELD, taskId));

                client.execute(DeleteByQueryAction.INSTANCE, deleteChildTasksRequest, ActionListener.wrap(r -> {
                    logger.debug("Successfully deleted child tasks of task " + taskId);
                    cleanChildTasksAndResultsOfDeletedTask();
                }, e -> { logger.error("Failed to delete child tasks of task " + taskId, e); }));
            }, ex -> { logger.error("Failed to delete forecast results for task " + taskId, ex); }));
        }, TimeValue.timeValueSeconds(DEFAULT_MAINTAIN_INTERVAL_IN_SECONDS), AD_BATCH_TASK_THREAD_POOL_NAME);
    }

    @Override
    public void startHistorical(
        Config config,
        DateRange dateRange,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        // TODO Auto-generated method stub

    }

    @Override
    protected TaskType getTaskType(Config config, DateRange dateRange, boolean runOnce) {
        if (runOnce) {
            return config.isHighCardinality()
                ? ForecastTaskType.RUN_ONCE_FORECAST_HC_FORECASTER
                : ForecastTaskType.RUN_ONCE_FORECAST_SINGLE_STREAM;
        } else {
            return config.isHighCardinality()
                ? ForecastTaskType.REALTIME_FORECAST_HC_FORECASTER
                : ForecastTaskType.REALTIME_FORECAST_SINGLE_STREAM;
        }
    }

    @Override
    protected <T> void createNewTask(
        Config config,
        DateRange dateRange,
        boolean runOnce,
        User user,
        String coordinatingNode,
        TaskState initialState,
        ActionListener<T> listener
    ) {
        String userName = user == null ? null : user.getName();
        Instant now = Instant.now();
        String taskType = getTaskType(config, dateRange, runOnce).name();
        ForecastTask.Builder forecastTaskBuilder = new ForecastTask.Builder()
            .configId(config.getId())
            .forecaster((Forecaster) config)
            .isLatest(true)
            .taskType(taskType)
            .executionStartTime(now)
            .state(initialState.name())
            .lastUpdateTime(now)
            .startedBy(userName)
            .coordinatingNode(coordinatingNode)
            .user(user);

        ResponseTransformer<IndexResponse, T> responseTransformer;

        final ForecastTask forecastTask;

        // used for run once
        if (initialState == TaskState.INIT_TEST) {
            forecastTask = forecastTaskBuilder.build();
            responseTransformer = (indexResponse) -> (T) forecastTask;
        } else {
            forecastTask = forecastTaskBuilder.taskProgress(0.0f).initProgress(0.0f).dateRange(dateRange).build();
            // used for real time
            responseTransformer = (indexResponse) -> (T) new JobResponse(indexResponse.getId());
        }

        createTaskDirectly(
            forecastTask,
            r -> onIndexConfigTaskResponse(
                r,
                forecastTask,
                (response, delegatedListener) -> cleanOldConfigTaskDocs(response, forecastTask, responseTransformer, delegatedListener),
                listener
            ),
            listener
        );

    }

    @Override
    public <T> void cleanConfigCache(
        TimeSeriesTask task,
        TransportService transportService,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        // no op for forecaster as we rely on state ttl to auto clean it
        // only execute function
        function.execute();
    }

    @Override
    protected boolean isHistoricalHCTask(TimeSeriesTask task) {
        // we have no backtesting
        return false;
    }

    @Override
    protected <T> void onIndexConfigTaskResponse(
        IndexResponse response,
        ForecastTask forecastTask,
        BiConsumer<IndexResponse, ActionListener<T>> function,
        ActionListener<T> listener
    ) {
        if (response == null || response.getResult() != CREATED) {
            String errorMsg = ExceptionUtil.getShardsFailure(response);
            listener.onFailure(new OpenSearchStatusException(errorMsg, response.status()));
            return;
        }
        forecastTask.setTaskId(response.getId());
        ActionListener<T> delegatedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
            handleTaskException(forecastTask, e);
            if (e instanceof DuplicateTaskException) {
                listener.onFailure(new OpenSearchStatusException(FORECASTER_IS_RUNNING, RestStatus.BAD_REQUEST));
            } else {
                // TODO: For historical forecast task, what to do if any exception happened?
                // For realtime forecast, task cache will be inited when realtime job starts, check
                // ForecastTaskManager#initRealtimeTaskCache for details. Here the
                // realtime task cache not inited yet when create forecast task, so no need to cleanup.
                listener.onFailure(e);
            }
        });
        // TODO: what to do if this is a historical task?
        if (function != null) {
            function.accept(response, delegatedListener);
        }
    }

    @Override
    protected <T> void runBatchResultAction(
        IndexResponse response,
        ForecastTask tsTask,
        ResponseTransformer<IndexResponse, T> responseTransformer,
        ActionListener<T> listener
    ) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Forecast does not support back testing yet.");
    }

    @Override
    protected BiCheckedFunction<XContentParser, String, ForecastTask, IOException> getTaskParser() {
        return ForecastTask::parse;
    }

    @Override
    public void createRunOnceTaskAndCleanupStaleTasks(
        String configId,
        Config config,
        TransportService transportService,
        ActionListener<ForecastTask> listener
    ) {
        ForecastTaskType taskType = config.isHighCardinality()
            ? ForecastTaskType.RUN_ONCE_FORECAST_HC_FORECASTER
            : ForecastTaskType.RUN_ONCE_FORECAST_SINGLE_STREAM;

        try {

            if (indexManagement.doesStateIndexExist()) {
                // If state index exist, check if latest task is running
                getAndExecuteOnLatestConfigLevelTask(config.getId(), Arrays.asList(taskType), (task) -> {
                    if (!task.isPresent() || task.get().isDone()) {
                        updateLatestFlagOfOldTasksAndCreateNewTask(config, null, true, config.getUser(), TaskState.INIT_TEST, listener);
                    } else {
                        listener.onFailure(new OpenSearchStatusException("run once is on-going", RestStatus.BAD_REQUEST));
                    }
                }, transportService, true, listener);
            } else {
                // If state index doesn't exist, create index and execute forecast.
                indexManagement.initStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", stateIndex);
                        updateLatestFlagOfOldTasksAndCreateNewTask(config, null, true, config.getUser(), TaskState.INIT_TEST, listener);
                    } else {
                        String error = String.format(Locale.ROOT, CommonMessages.CREATE_INDEX_NOT_ACKNOWLEDGED, stateIndex);
                        logger.warn(error);
                        listener.onFailure(new OpenSearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        updateLatestFlagOfOldTasksAndCreateNewTask(config, null, true, config.getUser(), TaskState.INIT_TEST, listener);
                    } else {
                        logger.error("Failed to init anomaly detection state index", e);
                        listener.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            logger.error("Failed to start detector " + config.getId(), e);
            listener.onFailure(e);
        }
    }

    @Override
    public List<ForecastTaskType> getTaskTypes(DateRange dateRange, boolean runOnce) {
        if (runOnce) {
            return ForecastTaskType.RUN_ONCE_TASK_TYPES;
        } else {
            return ForecastTaskType.REALTIME_TASK_TYPES;
        }
    }

    private <T> void resetRunOnceConfigTaskState(
        List<TimeSeriesTask> runOnceTasks,
        ExecutorFunction function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        if (ParseUtils.isNullOrEmpty(runOnceTasks)) {
            function.execute();
            return;
        }
        ForecastTask forecastTask = (ForecastTask) runOnceTasks.get(0);
        resetTaskStateAsStopped(forecastTask, function, transportService, listener);
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
        List<ForecastTask> tasks,
        Consumer<List<ForecastTask>> function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        List<TimeSeriesTask> runningRealtimeTasks = new ArrayList<>();
        List<TimeSeriesTask> runningRunOnceTasks = new ArrayList<>();

        for (TimeSeriesTask task : tasks) {
            if (!task.isHistoricalEntityTask() && !task.isDone()) {
                if (task.isRealTimeTask()) {
                    runningRealtimeTasks.add(task);
                } else if (task.isRunOnceTask()) {
                    runningRunOnceTasks.add(task);
                }
            }
        }

        resetRunOnceConfigTaskState(
            runningRunOnceTasks,
            () -> resetRealtimeConfigTaskState(runningRealtimeTasks, () -> function.accept(tasks), transportService, listener),
            transportService,
            listener
        );
    }
}
