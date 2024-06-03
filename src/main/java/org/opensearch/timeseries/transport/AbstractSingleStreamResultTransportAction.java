/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
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
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.CheckpointReadWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;
import org.opensearch.timeseries.ratelimit.ResultWriteWorker;
import org.opensearch.timeseries.ratelimit.SaveResultStrategy;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.transport.handler.IndexMemoryPressureAwareResultHandler;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class AbstractSingleStreamResultTransportAction<RCFModelType extends ThresholdedRandomCutForest, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriterType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, CheckpointMaintainerType extends CheckpointMaintainWorker, CacheBufferType extends CacheBuffer<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType>, PriorityCacheType extends PriorityCache<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType, CacheBufferType>, CacheProviderType extends CacheProvider<RCFModelType, PriorityCacheType>, ResultType extends IndexableResult, RCFResultType extends IntermediateResult<ResultType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType>, ModelManagerType extends ModelManager<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType>, CacheType extends TimeSeriesCache<RCFModelType>, SaveResultStrategyType extends SaveResultStrategy<ResultType, RCFResultType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType, CacheType, ResultType, RCFResultType, ModelManagerType, SaveResultStrategyType>, CheckpointReadWorkerType extends CheckpointReadWorker<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType, ModelManagerType, CacheType, SaveResultStrategyType, ColdStartWorkerType>, ResultWriteRequestType extends ResultWriteRequest<ResultType>, BatchRequestType extends ResultBulkRequest<ResultType, ResultWriteRequestType>, ResultHandlerType extends IndexMemoryPressureAwareResultHandler<BatchRequestType, ResultBulkResponse, IndexType, IndexManagementType>, ResultWriteWorkerType extends ResultWriteWorker<ResultType, ResultWriteRequestType, BatchRequestType, IndexType, IndexManagementType, ResultHandlerType>>
    extends HandledTransportAction<SingleStreamResultRequest, AcknowledgedResponse> {
    private static final Logger LOG = LogManager.getLogger(AbstractSingleStreamResultTransportAction.class);
    protected CircuitBreakerService circuitBreakerService;
    protected CacheProviderType cache;
    protected final NodeStateManager stateManager;
    protected CheckpointReadWorkerType checkpointReadQueue;
    protected ModelManagerType modelManager;
    protected IndexManagementType indexUtil;
    protected ResultWriteWorkerType resultWriteQueue;
    protected Stats stats;
    protected ColdStartWorkerType coldStartWorker;
    protected IndexType resultIndex;
    protected AnalysisType analysisType;
    private String modelCorrptionStat;

    public AbstractSingleStreamResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CircuitBreakerService circuitBreakerService,
        CacheProviderType cache,
        NodeStateManager stateManager,
        CheckpointReadWorkerType checkpointReadQueue,
        ModelManagerType modelManager,
        IndexManagementType indexUtil,
        ResultWriteWorkerType resultWriteQueue,
        Stats stats,
        ColdStartWorkerType coldStartQueue,
        String resultAction,
        IndexType resultIndex,
        AnalysisType analysisType,
        String modelCorrptionStat
    ) {
        super(resultAction, transportService, actionFilters, SingleStreamResultRequest::new);
        this.circuitBreakerService = circuitBreakerService;
        this.cache = cache;
        this.stateManager = stateManager;
        this.checkpointReadQueue = checkpointReadQueue;
        this.modelManager = modelManager;
        this.indexUtil = indexUtil;
        this.resultWriteQueue = resultWriteQueue;
        this.stats = stats;
        this.coldStartWorker = coldStartQueue;
        this.resultIndex = resultIndex;
        this.analysisType = analysisType;
        this.modelCorrptionStat = modelCorrptionStat;
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

            stateManager.getConfig(configId, analysisType, onGetConfig(listener, configId, request, previousException));
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
        return ActionListener.wrap(configOptional -> {
            if (!configOptional.isPresent()) {
                listener.onFailure(new EndRunException(configId, "Config " + configId + " is not available.", false));
                return;
            }

            Config config = configOptional.get();

            Instant executionStartTime = Instant.now();

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
                try {
                    RCFResultType result = modelManager
                        .getResult(
                            new Sample(datapoint, Instant.ofEpochMilli(request.getStart()), Instant.ofEpochMilli(request.getEnd())),
                            modelState,
                            modelId,
                            config,
                            request.getTaskId()
                        );
                    // result.getRcfScore() = 0 means the model is not initialized
                    if (result.getRcfScore() > 0) {
                        List<ResultType> indexableResults = result
                            .toIndexableResults(
                                config,
                                Instant.ofEpochMilli(request.getStart()),
                                Instant.ofEpochMilli(request.getEnd()),
                                executionStartTime,
                                Instant.now(),
                                ParseUtils.getFeatureData(datapoint, config),
                                Optional.empty(),
                                indexUtil.getSchemaVersion(resultIndex),
                                modelId,
                                null,
                                null
                            );

                        for (ResultType r : indexableResults) {
                            resultWriteQueue.put(createResultWriteRequest(config, r));
                        }
                    }
                } catch (IllegalArgumentException e) {
                    // fail to score likely due to model corruption. Re-cold start to recover.
                    LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", modelId), e);
                    stats.getStat(modelCorrptionStat).increment();
                    cache.get().removeModel(configId, modelId);
                    coldStartWorker
                        .put(
                            new FeatureRequest(
                                System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                                configId,
                                RequestPriority.MEDIUM,
                                modelId,
                                datapoint,
                                request.getStart(),
                                request.getTaskId()
                            )
                        );
                }
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
                        "fail to get entity's result for config [{}]: start: [{}], end: [{}]",
                        configId,
                        request.getStart(),
                        request.getEnd()
                    ),
                    exception
                );
            listener.onFailure(exception);
        });
    }

    public abstract ResultWriteRequestType createResultWriteRequest(Config config, ResultType result);
}
