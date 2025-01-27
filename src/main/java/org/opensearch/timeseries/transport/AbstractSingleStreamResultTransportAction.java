/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import java.time.Instant;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.CacheBuffer;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.PriorityCache;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.RealTimeInferencer;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.CheckpointReadWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;
import org.opensearch.timeseries.ratelimit.SaveResultStrategy;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ActionListenerExecutor;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class AbstractSingleStreamResultTransportAction<RCFModelType extends ThresholdedRandomCutForest, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriterType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, CheckpointMaintainerType extends CheckpointMaintainWorker, CacheBufferType extends CacheBuffer<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType>, PriorityCacheType extends PriorityCache<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType, CacheBufferType>, CacheProviderType extends CacheProvider<RCFModelType, PriorityCacheType>, ResultType extends IndexableResult, RCFResultType extends IntermediateResult<ResultType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ResultType>, ModelManagerType extends ModelManager<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType>, CacheType extends TimeSeriesCache<RCFModelType>, SaveResultStrategyType extends SaveResultStrategy<ResultType, RCFResultType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType, CacheType, ResultType, RCFResultType, ModelManagerType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType>, InferencerType extends RealTimeInferencer<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType, ModelManagerType, SaveResultStrategyType, CacheType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType>, CheckpointReadWorkerType extends CheckpointReadWorker<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType, ModelManagerType, CacheType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType, InferencerType>, ResultWriteRequestType extends ResultWriteRequest<ResultType>>
    extends HandledTransportAction<SingleStreamResultRequest, AcknowledgedResponse> {
    private static final Logger LOG = LogManager.getLogger(AbstractSingleStreamResultTransportAction.class);
    protected CircuitBreakerService circuitBreakerService;
    protected CacheProviderType cache;
    protected final NodeStateManager stateManager;
    protected CheckpointReadWorkerType checkpointReadQueue;
    protected AnalysisType analysisType;
    private InferencerType inferencer;
    private ThreadPool threadPool;
    private String threadPoolName;

    public AbstractSingleStreamResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CircuitBreakerService circuitBreakerService,
        CacheProviderType cache,
        NodeStateManager stateManager,
        CheckpointReadWorkerType checkpointReadQueue,
        String resultAction,
        AnalysisType analysisType,
        InferencerType inferencer,
        ThreadPool threadPool,
        String threadPoolName
    ) {
        super(resultAction, transportService, actionFilters, SingleStreamResultRequest::new);
        this.circuitBreakerService = circuitBreakerService;
        this.cache = cache;
        this.stateManager = stateManager;
        this.checkpointReadQueue = checkpointReadQueue;
        this.analysisType = analysisType;
        this.inferencer = inferencer;
        this.threadPool = threadPool;
        this.threadPoolName = threadPoolName;
    }

    @Override
    protected void doExecute(Task task, SingleStreamResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (circuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getConfigId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String configId = request.getConfigId();

            Optional<Exception> previousException = stateManager.fetchExceptionAndClear(configId);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error("Previous exception of {}: {}", configId, exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }

                listener = ExceptionUtil.wrapListener(listener, exception, configId);
            }

            // task id equals to null means it is real time and we want to cache
            stateManager
                .getConfig(
                    configId,
                    analysisType,
                    request.getTaskId() == null,
                    onGetConfig(listener, configId, request, previousException)
                );
        } catch (Exception exception) {
            LOG.error("fail to get result", exception);
            listener.onFailure(exception);
        }
    }

    public ActionListener<Optional<? extends Config>> onGetConfig(
        ActionListener<AcknowledgedResponse> listener,
        String configId,
        SingleStreamResultRequest request,
        Optional<Exception> prevException
    ) {
        return ActionListenerExecutor.wrap(configOptional -> {
            if (!configOptional.isPresent()) {
                listener.onFailure(new EndRunException(configId, "Config " + configId + " is not available.", false));
                return;
            }

            Config config = configOptional.get();

            String modelId = request.getModelId();
            double[] datapoint = request.getDataPoint();
            ModelState<RCFModelType> modelState = cache.get().get(modelId, config);
            if (modelState == null) {
                // cache miss
                checkpointReadQueue
                    .put(
                        new FeatureRequest(
                            System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                            configId,
                            RequestPriority.MEDIUM,
                            request.getModelId(),
                            datapoint,
                            request.getStart(),
                            request.getTaskId()
                        )
                    );
            } else {
                inferencer
                    .process(
                        new Sample(datapoint, Instant.ofEpochMilli(request.getStart()), Instant.ofEpochMilli(request.getEnd())),
                        modelState,
                        config,
                        request.getTaskId()
                    );
            }

            // respond back
            if (prevException.isPresent()) {
                listener.onFailure(prevException.get());
            } else {
                listener.onResponse(new AcknowledgedResponse(true));
            }
        }, exception -> {
            LOG
                .error(
                    new ParameterizedMessage(
                        "fail to get single stream result for config [{}]: start: [{}], end: [{}]",
                        configId,
                        request.getStart(),
                        request.getEnd()
                    ),
                    exception
                );
            listener.onFailure(exception);
        }, threadPool.executor(threadPoolName));
    }

    public abstract ResultWriteRequestType createResultWriteRequest(Config config, ResultType result);
}
