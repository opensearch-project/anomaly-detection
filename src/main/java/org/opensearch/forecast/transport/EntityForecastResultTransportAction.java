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
package org.opensearch.forecast.transport;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.ForecastRealTimeInferencer;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.ratelimit.ForecastCheckpointReadWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastColdEntityWorker;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastResultWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastSaveResultStrategy;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.EntityResultProcessor;
import org.opensearch.timeseries.transport.EntityResultRequest;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.RCFCaster;

/**
 * Entry-point for HC forecast workflow.  We have created multiple queues for coordinating
 * the workflow. The overrall workflow is:
 * 1. We store as many frequently used entity models in a cache as allowed by the
 *  memory limit (by default 10% heap). If an entity feature is a hit, we use the in-memory model
 *  to forecast and record results using the result write queue.
 * 2. If an entity feature is a miss, we check if there is free memory or any other
 *  entity's model can be evacuated. An in-memory entity's frequency may be lower
 *  compared to the cache miss entity. If that's the case, we replace the lower
 *  frequency entity's model with the higher frequency entity's model. To load the
 *  higher frequency entity's model, we first check if a model exists on disk by
 *  sending a checkpoint read queue request. If there is a checkpoint, we load it
 *  to memory, perform forecast, and save the result using the result write queue.
 *  Otherwise, we enqueue a cold start request to the cold start queue for model
 *  training. If training is successful, we save the learned model via the checkpoint
 *  write queue.
 * 3. We also have the cold entity queue configured for cold entities, and the model
 * training and inference are connected by serial juxtaposition to limit resource usage.
 */
public class EntityForecastResultTransportAction extends HandledTransportAction<EntityResultRequest, AcknowledgedResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityForecastResultTransportAction.class);
    private CircuitBreakerService circuitBreakerService;
    private CacheProvider<RCFCaster, ForecastPriorityCache> cache;
    private final NodeStateManager stateManager;
    private ThreadPool threadPool;
    private EntityResultProcessor<RCFCaster, ForecastResult, RCFCasterResult, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastColdStart, ForecastModelManager, ForecastPriorityCache, ForecastSaveResultStrategy, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastColdStartWorker, ForecastRealTimeInferencer, ForecastCheckpointReadWorker, ForecastColdEntityWorker> intervalDataProcessor;

    private final ForecastCacheProvider entityCache;
    private final ForecastCheckpointReadWorker checkpointReadQueue;
    private final ForecastColdEntityWorker coldEntityQueue;
    private final ForecastRealTimeInferencer inferencer;

    @Inject
    public EntityForecastResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        CircuitBreakerService adCircuitBreakerService,
        ForecastCacheProvider entityCache,
        NodeStateManager stateManager,
        ForecastIndexManagement indexUtil,
        ForecastResultWriteWorker resultWriteQueue,
        ForecastCheckpointReadWorker checkpointReadQueue,
        ForecastColdEntityWorker coldEntityQueue,
        ThreadPool threadPool,
        ForecastRealTimeInferencer inferencer
    ) {
        super(EntityForecastResultAction.NAME, transportService, actionFilters, EntityResultRequest::new);
        this.circuitBreakerService = adCircuitBreakerService;
        this.cache = entityCache;
        this.stateManager = stateManager;
        this.threadPool = threadPool;
        this.intervalDataProcessor = null;
        this.entityCache = entityCache;
        this.checkpointReadQueue = checkpointReadQueue;
        this.coldEntityQueue = coldEntityQueue;
        this.inferencer = inferencer;
    }

    @Override
    protected void doExecute(Task task, EntityResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (circuitBreakerService.isOpen()) {
            threadPool
                .executor(TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME)
                .execute(() -> cache.get().releaseMemoryForOpenCircuitBreaker());
            listener.onFailure(new LimitExceededException(request.getConfigId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String forecasterId = request.getConfigId();

            Optional<Exception> previousException = stateManager.fetchExceptionAndClear(forecasterId);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error("Previous exception of {}: {}", forecasterId, exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }

                listener = ExceptionUtil.wrapListener(listener, exception, forecasterId);
            }

            intervalDataProcessor = new EntityResultProcessor<>(
                entityCache,
                checkpointReadQueue,
                coldEntityQueue,
                inferencer,
                threadPool,
                TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME
            );

            stateManager
                .getConfig(
                    forecasterId,
                    request.getAnalysisType(),
                    // task id equals to null means it is real time and we want to cache
                    request.getTaskId() == null,
                    intervalDataProcessor.onGetConfig(listener, forecasterId, request, previousException, request.getAnalysisType())
                );
        } catch (Exception exception) {
            LOG.error("fail to get entity's forecasts", exception);
            listener.onFailure(exception);
        }
    }
}
