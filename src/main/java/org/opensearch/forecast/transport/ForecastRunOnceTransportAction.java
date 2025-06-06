/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import static org.opensearch.core.rest.RestStatus.CONFLICT;
import static org.opensearch.core.rest.RestStatus.FORBIDDEN;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_FORECAST_FEATURES;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_HC_FORECASTERS;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_SINGLE_STREAM_FORECASTERS;
import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class ForecastRunOnceTransportAction extends HandledTransportAction<ForecastResultRequest, ForecastResultResponse> {

    private static final Logger LOG = LogManager.getLogger(ForecastRunOnceTransportAction.class);
    // Define constants for retry logic.
    private static final int MAX_RETRIES = 3;
    private static final long BASE_DELAY_MS = 1000; // 1 second initial delay
    private static final int POLL_FREQ = 10; // 10 seconds
    private static final int MAX_WAIT_TIMES = 100;

    private ResultProcessor<ForecastResultRequest, ForecastResult, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement, ForecastTaskManager> resultProcessor;
    private final Client client;
    private CircuitBreakerService circuitBreakerService;
    private final NodeStateManager nodeStateManager;

    private final Settings settings;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final HashRing hashRing;
    private final TransportService transportService;
    private final ForecastTaskManager taskManager;
    private final NamedXContentRegistry xContentRegistry;
    private final SecurityClientUtil clientUtil;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final FeatureManager featureManager;
    private final ForecastStats forecastStats;
    private volatile Boolean filterByEnabled;

    protected volatile Integer maxSingleStreamForecasters;
    protected volatile Integer maxHCForecasters;
    protected volatile Integer maxForecastFeatures;
    protected volatile Integer maxCategoricalFields;

    @Inject
    public ForecastRunOnceTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        Client client,
        SecurityClientUtil clientUtil,
        NodeStateManager nodeStateManager,
        FeatureManager featureManager,
        ForecastModelManager modelManager,
        HashRing hashRing,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        CircuitBreakerService circuitBreakerService,
        ForecastStats forecastStats,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager realTimeTaskManager
    ) {
        super(ForecastRunOnceAction.NAME, transportService, actionFilters, ForecastResultRequest::new);

        this.resultProcessor = null;
        this.settings = settings;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.hashRing = hashRing;
        this.transportService = transportService;
        this.taskManager = realTimeTaskManager;
        this.xContentRegistry = xContentRegistry;
        this.clientUtil = clientUtil;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.featureManager = featureManager;
        this.forecastStats = forecastStats;

        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
        this.nodeStateManager = nodeStateManager;
        filterByEnabled = ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);

        this.maxSingleStreamForecasters = MAX_SINGLE_STREAM_FORECASTERS.get(settings);
        this.maxHCForecasters = MAX_HC_FORECASTERS.get(settings);
        this.maxForecastFeatures = MAX_FORECAST_FEATURES;
        this.maxCategoricalFields = ForecastNumericSetting.maxCategoricalFields();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_SINGLE_STREAM_FORECASTERS, it -> maxSingleStreamForecasters = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_HC_FORECASTERS, it -> maxHCForecasters = it);
    }

    @Override
    protected void doExecute(Task task, ForecastResultRequest request, ActionListener<ForecastResultResponse> listener) {
        String forecastID = request.getConfigId();
        User user = ParseUtils.getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {

            resolveUserAndExecute(
                user,
                forecastID,
                filterByEnabled,
                listener,
                (forecaster) -> executeRunOnce(forecastID, request, listener),
                client,
                clusterService,
                xContentRegistry,
                Forecaster.class
            );
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(new OpenSearchStatusException("Failed to run once forecaster " + forecastID, INTERNAL_SERVER_ERROR));
        }
    }

    private void executeRunOnce(String forecastID, ForecastResultRequest request, ActionListener<ForecastResultResponse> listener) {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            listener.onFailure(new OpenSearchStatusException(ForecastCommonMessages.DISABLED_ERR_MSG, FORBIDDEN));
            return;
        }

        if (circuitBreakerService.isOpen()) {
            listener.onFailure(new OpenSearchStatusException(CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, SERVICE_UNAVAILABLE));
            return;
        }

        client.execute(ForecastRunOnceProfileAction.INSTANCE, new ForecastRunOnceProfileRequest(forecastID), ActionListener.wrap(r -> {
            if (r.isAnswerTrue()) {
                listener
                    .onFailure(
                        new OpenSearchStatusException(
                            "cannot start a new test for " + forecastID + " since current test hasn't finished.",
                            CONFLICT
                        )
                    );
            } else {
                nodeStateManager.getJob(forecastID, ActionListener.wrap(jobOptional -> {
                    if (jobOptional.isPresent() && jobOptional.get().isEnabled()) {
                        listener
                            .onFailure(
                                new OpenSearchStatusException("Cannot run once " + forecastID + " when real time job is running.", CONFLICT)
                            );
                        return;
                    }

                    triggerRunOnce(forecastID, request, listener);
                }, e -> {
                    if (e instanceof IndexNotFoundException) {
                        triggerRunOnce(forecastID, request, listener);
                    } else {
                        LOG.error(e);
                        listener
                            .onFailure(new OpenSearchStatusException("Fail to verify if job " + forecastID + " starts or not.", CONFLICT));
                    }
                }));
            }
        }, e -> {
            LOG.error(e);
            listener.onFailure(new OpenSearchStatusException("Failed to run once forecaster " + forecastID, INTERNAL_SERVER_ERROR));
        }));
    }

    private void checkIfRunOnceFinished(String forecastID, String taskId, AtomicInteger waitTimes) {
        client.execute(ForecastRunOnceProfileAction.INSTANCE, new ForecastRunOnceProfileRequest(forecastID), ActionListener.wrap(r -> {
            if (r.isAnswerTrue()) {
                handleRunOnceNotFinished(forecastID, taskId, waitTimes, r.getExceptionMsg());
            } else {
                handleRunOnceFinished(forecastID, taskId, r.getExceptionMsg());
            }
        }, e -> {
            LOG.error("Failed to profile run once of forecaster " + forecastID, e);
            handleRunOnceNotFinished(forecastID, taskId, waitTimes, ExceptionUtil.getErrorMessage(e));
        }));
    }

    private void handleRunOnceNotFinished(String forecastID, String taskId, AtomicInteger waitTimes, String exceptionMsg) {
        if (waitTimes.get() < MAX_WAIT_TIMES) {
            waitTimes.addAndGet(1);
            threadPool
                .schedule(
                    () -> checkIfRunOnceFinished(forecastID, taskId, waitTimes),
                    new TimeValue(POLL_FREQ, TimeUnit.SECONDS),
                    FORECAST_THREAD_POOL_NAME
                );
            if (!Strings.isEmpty(exceptionMsg)) {
                updateTaskError(forecastID, taskId, exceptionMsg);
            }
        } else {
            LOG.warn("Timed out run once of forecaster {}", forecastID);
            updateTaskState(forecastID, taskId, TaskState.INACTIVE);
        }
    }

    private void handleRunOnceFinished(String forecastID, String taskId, String exceptionMsg) {
        LOG.info("Run once of forecaster {} finished", forecastID);
        // run once does not need to cache config
        nodeStateManager.getConfig(forecastID, AnalysisType.FORECAST, false, ActionListener.wrap(configOptional -> {
            if (configOptional.isEmpty()) {
                updateTaskState(forecastID, taskId, TaskState.INACTIVE);
                return;
            }
            checkForecastResults(forecastID, taskId, configOptional.get(), exceptionMsg);
        }, e -> {
            LOG.error("Fail to get config", e);
            updateTaskState(forecastID, taskId, TaskState.INACTIVE);
        }));
    }

    private void checkForecastResults(String forecastID, String taskId, Config config, String exceptionMsg) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(ForecastCommonName.FORECASTER_ID_KEY, forecastID));
        ExistsQueryBuilder forecastsExistFilter = QueryBuilders.existsQuery(ForecastResult.VALUE_FIELD);
        filterQuery.must(forecastsExistFilter);
        // run-once analysis result also stored in result index, which has non-null task_id.
        filterQuery.filter(QueryBuilders.termQuery(CommonName.TASK_ID_FIELD, taskId));

        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1);

        SearchRequest request = new SearchRequest(ForecastIndexManagement.ALL_FORECAST_RESULTS_INDEX_PATTERN);
        request.source(source);
        if (config.getCustomResultIndexOrAlias() != null) {
            request.indices(config.getCustomResultIndexPattern());
        }

        performSearchWithRetry(forecastID, taskId, request, 0, exceptionMsg);
    }

    private void updateTaskError(String forecastID, String taskId, String exceptionMsg) {
        updateTask(forecastID, taskId, null, exceptionMsg);
    }

    private void updateTaskState(String forecastID, String taskId, TaskState state) {
        updateTask(forecastID, taskId, state, null);
    }

    private void updateTask(String forecastID, String taskId, TaskState state, String exceptionMsg) {
        Map<String, Object> updatedFields = new HashMap<>();
        if (state != null) {
            updatedFields.put(TimeSeriesTask.STATE_FIELD, state.name());
        }
        if (!Strings.isEmpty(exceptionMsg)) {
            updatedFields.put(TimeSeriesTask.ERROR_FIELD, exceptionMsg);
        }

        taskManager.updateTask(taskId, updatedFields, ActionListener.wrap(updateResponse -> {
            LOG.info("Updated forecaster task {} for forecaster {}: {}", taskId, forecastID, updatedFields);
        }, e -> { LOG.error("Failed to update forecaster task: {} for forecaster: {}", taskId, forecastID, e); }));
    }

    private void triggerRunOnce(String forecastID, ForecastResultRequest request, ActionListener<ForecastResultResponse> listener) {
        try {
            resultProcessor = new ForecastResultProcessor(
                ForecastSettings.FORECAST_REQUEST_TIMEOUT,
                EntityForecastResultAction.NAME,
                StatNames.FORECAST_HC_EXECUTE_REQUEST_COUNT,
                settings,
                clusterService,
                threadPool,
                hashRing,
                nodeStateManager,
                transportService,
                forecastStats,
                taskManager,
                xContentRegistry,
                client,
                clientUtil,
                indexNameExpressionResolver,
                ForecastResultResponse.class,
                featureManager,
                AnalysisType.FORECAST,
                true
            );

            ActionListener<ForecastResultResponse> wrappedListener = ActionListener.wrap(r -> {
                AtomicInteger waitTimes = new AtomicInteger(0);

                threadPool
                    .schedule(
                        () -> checkIfRunOnceFinished(forecastID, r.getTaskId(), waitTimes),
                        new TimeValue(10, TimeUnit.SECONDS),
                        TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME
                    );
                listener.onResponse(r);
            }, e -> {
                LOG.error("Failed to finish run once of forecaster " + forecastID, e);
                listener.onFailure(new OpenSearchStatusException("Failed to run once forecaster " + forecastID, INTERNAL_SERVER_ERROR));
            });

            nodeStateManager
                .getConfig(
                    forecastID,
                    AnalysisType.FORECAST,
                    false,
                    resultProcessor.onGetConfig(wrappedListener, forecastID, request, Optional.empty())
                );

            // check for status
        } catch (Exception ex) {
            ResultProcessor.handleExecuteException(ex, listener, forecastID);
        }
    }

    /**
     * Performs the search with retry logic.
     *
     * @param forecastID   The forecast ID.
     * @param taskId       The task ID.
     * @param request      The search request.
     * @param attempt      The current attempt number (start with 0).
     * @param exceptionMsg The exception message to pass in case of failure.
     */
    private void performSearchWithRetry(String forecastID, String taskId, SearchRequest request, int attempt, String exceptionMsg) {
        client.search(request, ActionListener.wrap(searchResponse -> {
            SearchHits hits = searchResponse.getHits();
            if (hits.getTotalHits().value > 0) {
                // At least one result found: mark the task as complete.
                updateTaskState(forecastID, taskId, TaskState.TEST_COMPLETE);
            } else {
                if (attempt < MAX_RETRIES) {
                    // Calculate the exponential delay: BASE_DELAY_MS * 2^(attempt)
                    // e.g. for attempts: 1s, 2s, 4s delays respectively.
                    long delayMillis = BASE_DELAY_MS * (1L << attempt);

                    LOG.info("No hits found. Retrying search in {} ms (attempt {}/{})...", delayMillis, attempt + 1, MAX_RETRIES);
                    // Schedule a retry after the calculated delay.
                    threadPool
                        .schedule(
                            () -> performSearchWithRetry(forecastID, taskId, request, attempt + 1, exceptionMsg),
                            TimeValue.timeValueMillis(delayMillis),
                            FORECAST_THREAD_POOL_NAME
                        );
                } else {
                    // After MAX_RETRIES attempts, update the task as INIT_TEST_FAILED if there is no existing state.
                    taskManager.getTask(taskId, ActionListener.wrap(r -> {
                        if (r.isPresent()) {
                            String state = r.get().getState();
                            // If there is no state, update it; otherwise, it might have been set elsewhere (e.g., by ColdStartWorker)
                            if (Strings.isEmpty(state)) {
                                updateTask(forecastID, taskId, TaskState.INIT_TEST_FAILED, exceptionMsg);
                            }
                        } else {
                            updateTask(forecastID, taskId, TaskState.INIT_TEST_FAILED, exceptionMsg);
                        }
                    }, e -> {
                        // The task may not exist.
                        updateTask(forecastID, taskId, TaskState.INIT_TEST_FAILED, ExceptionUtil.getErrorMessage(e));
                    }));
                }
            }
        }, e -> {
            // Treat search failures as transient errors.
            // Some errors, such as "all shards failed", may occur temporarily before the index is ready for search.
            // Therefore, we count the failure as part of the retry logic rather than immediately failing the task.
            LOG.error("Fail to search result on attempt {}/{}. Retrying...", attempt + 1, MAX_RETRIES, e);
            if (attempt < MAX_RETRIES) {
                long delayMillis = BASE_DELAY_MS * (1L << attempt);
                LOG.info("Retrying search in {} ms due to failure (attempt {}/{})...", delayMillis, attempt + 1, MAX_RETRIES);
                threadPool
                    .schedule(
                        () -> performSearchWithRetry(forecastID, taskId, request, attempt + 1, ExceptionUtil.getErrorMessage(e)),
                        TimeValue.timeValueMillis(delayMillis),
                        FORECAST_THREAD_POOL_NAME
                    );
            } else {
                updateTask(forecastID, taskId, TaskState.INIT_TEST_FAILED, ExceptionUtil.getErrorMessage(e));
            }
        }));
    }
}
