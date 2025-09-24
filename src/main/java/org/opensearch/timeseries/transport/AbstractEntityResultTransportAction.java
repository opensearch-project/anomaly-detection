/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.timeseries.transport;

import java.time.Clock;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.CacheProvider;
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
import org.opensearch.timeseries.ml.RealTimeInferencer;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.ratelimit.CheckpointReadWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.ColdEntityWorker;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.SaveResultStrategy;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Shared transport action skeleton for high-cardinality entity workflows.
 */
public abstract class AbstractEntityResultTransportAction<RCFModelType extends ThresholdedRandomCutForest, IndexableResultType extends IndexableResult, IntermediateResultType extends IntermediateResult<IndexableResultType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ModelColdStartType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, IndexableResultType>, ModelManagerType extends ModelManager<RCFModelType, IndexableResultType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, ModelColdStartType>, CacheType extends TimeSeriesCache<RCFModelType>, SaveResultStrategyType extends SaveResultStrategy<IndexableResultType, IntermediateResultType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType, CacheType, IndexableResultType, IntermediateResultType, ModelManagerType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType>, InferencerType extends RealTimeInferencer<RCFModelType, IndexableResultType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType, ModelManagerType, SaveResultStrategyType, CacheType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType>, HCCheckpointReadWorkerType extends CheckpointReadWorker<RCFModelType, IndexableResultType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType, ModelManagerType, CacheType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType, InferencerType>, ColdEntityWorkerType extends ColdEntityWorker<RCFModelType, IndexableResultType, IndexType, IndexManagementType, CheckpointDaoType, IntermediateResultType, ModelManagerType, CheckpointWriteWorkerType, ModelColdStartType, CacheType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType, InferencerType, HCCheckpointReadWorkerType>>
    extends HandledTransportAction<EntityResultRequest, AcknowledgedResponse> {
    private static final Logger LOG = LogManager.getLogger(AbstractEntityResultTransportAction.class);

    private final CircuitBreakerService circuitBreakerService;
    private final CacheProvider<RCFModelType, CacheType> cacheProvider;
    private final NodeStateManager stateManager;
    private final ThreadPool threadPool;
    private final Clock clock;
    private final String threadPoolName;
    private final HCCheckpointReadWorkerType checkpointReadQueue;
    private final ColdEntityWorkerType coldEntityQueue;
    private final InferencerType inferencer;

    private EntityResultProcessor<RCFModelType, IndexableResultType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType, ModelManagerType, CacheType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType, InferencerType, HCCheckpointReadWorkerType, ColdEntityWorkerType> intervalDataProcessor;

    protected AbstractEntityResultTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        CircuitBreakerService circuitBreakerService,
        CacheProvider<RCFModelType, CacheType> cacheProvider,
        NodeStateManager stateManager,
        ThreadPool threadPool,
        String threadPoolName,
        HCCheckpointReadWorkerType checkpointReadQueue,
        ColdEntityWorkerType coldEntityQueue,
        InferencerType inferencer
    ) {
        super(actionName, transportService, actionFilters, EntityResultRequest::new);
        this.circuitBreakerService = circuitBreakerService;
        this.cacheProvider = cacheProvider;
        this.stateManager = stateManager;
        this.threadPool = threadPool;
        this.clock = Clock.systemUTC();
        this.threadPoolName = threadPoolName;
        this.checkpointReadQueue = checkpointReadQueue;
        this.coldEntityQueue = coldEntityQueue;
        this.inferencer = inferencer;
        this.intervalDataProcessor = null;
    }

    @Override
    protected void doExecute(Task task, EntityResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (circuitBreakerService.isOpen()) {
            threadPool.executor(threadPoolName).execute(() -> cacheProvider.get().releaseMemoryForOpenCircuitBreaker());
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

            intervalDataProcessor = new EntityResultProcessor<>(
                cacheProvider,
                checkpointReadQueue,
                coldEntityQueue,
                inferencer,
                threadPool,
                threadPoolName,
                clock
            );

            stateManager
                .getConfig(
                    configId,
                    request.getAnalysisType(),
                    // task id equals to null means it is real time and we want to cache
                    request.getTaskId() == null,
                    intervalDataProcessor.onGetConfig(listener, configId, request, previousException, request.getAnalysisType())
                );
        } catch (Exception exception) {
            LOG.error("fail to get entity's analysis result", exception);
            listener.onFailure(exception);
        }
    }
}
