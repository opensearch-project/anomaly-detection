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

package org.opensearch.timeseries.ratelimit;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.common.exception.EndRunException;
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
import org.opensearch.timeseries.util.ActionListenerExecutor;
import org.opensearch.timeseries.util.ExceptionUtil;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public abstract class CheckpointReadWorker<RCFModelType extends ThresholdedRandomCutForest, ResultType extends IndexableResult, RCFResultType extends IntermediateResult<ResultType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, CheckpointType, CheckpointWriteWorkerType>, ModelManagerType extends ModelManager<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointType, CheckpointWriteWorkerType, ColdStarterType>, CacheType extends TimeSeriesCache<RCFModelType>, SaveResultStrategyType extends SaveResultStrategy<ResultType, RCFResultType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointType, CheckpointWriteWorkerType, ColdStarterType, CacheType, ResultType, RCFResultType, ModelManagerType, SaveResultStrategyType>, InferencerType extends RealTimeInferencer<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointType, CheckpointWriteWorkerType, ColdStarterType, ModelManagerType, SaveResultStrategyType, CacheType, ColdStartWorkerType>>
    extends BatchWorker<FeatureRequest, MultiGetRequest, MultiGetResponse> {

    private static final Logger LOG = LogManager.getLogger(CheckpointReadWorker.class);

    protected final ModelManagerType modelManager;
    protected final CheckpointType checkpointDao;
    protected final ColdStartWorkerType coldStartWorker;
    protected final CheckpointWriteWorkerType checkpointWriteWorker;
    protected final Provider<? extends TimeSeriesCache<RCFModelType>> cacheProvider;
    protected final String checkpointIndexName;
    protected final InferencerType inferencer;

    public CheckpointReadWorker(
        String workerName,
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        String threadPoolName,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Duration executionTtl,
        ModelManagerType modelManager,
        CheckpointType checkpointDao,
        ColdStartWorkerType entityColdStartWorker,
        NodeStateManager stateManager,
        Provider<? extends TimeSeriesCache<RCFModelType>> cacheProvider,
        Duration stateTtl,
        CheckpointWriteWorkerType checkpointWriteWorker,
        Setting<Integer> concurrencySetting,
        Setting<Integer> batchSizeSetting,
        String checkpointIndexName,
        AnalysisType context,
        InferencerType inferencer
    ) {
        super(
            workerName,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            threadPoolName,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            concurrencySetting,
            executionTtl,
            batchSizeSetting,
            stateTtl,
            stateManager,
            context
        );

        this.modelManager = modelManager;
        this.checkpointDao = checkpointDao;
        this.coldStartWorker = entityColdStartWorker;
        this.cacheProvider = cacheProvider;
        this.checkpointWriteWorker = checkpointWriteWorker;
        this.checkpointIndexName = checkpointIndexName;
        this.inferencer = inferencer;
    }

    @Override
    protected void executeBatchRequest(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        checkpointDao.batchRead(request, listener);
    }

    /**
     * Convert the input list of FeatureRequest to a multi-get request.
     * RateLimitedRequestWorker.getRequests has already limited the number of
     * requests in the input list. So toBatchRequest method can take the input
     * and send the multi-get directly.
     * @return The converted multi-get request
     */
    @Override
    protected MultiGetRequest toBatchRequest(List<FeatureRequest> toProcess) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (FeatureRequest request : toProcess) {
            String modelId = request.getModelId();
            if (null == modelId) {
                continue;
            }
            multiGetRequest.add(new MultiGetRequest.Item(checkpointIndexName, modelId));
        }
        return multiGetRequest;
    }

    @Override
    protected ActionListener<MultiGetResponse> getResponseListener(List<FeatureRequest> toProcess, MultiGetRequest batchRequest) {
        return ActionListener.wrap(response -> {

            final MultiGetItemResponse[] itemResponses = response.getResponses();
            Map<String, MultiGetItemResponse> successfulRequests = new HashMap<>();

            // lazy init since we don't expect retryable requests to happen often
            Set<String> retryableRequests = null;
            Set<String> notFoundModels = null;
            boolean printedUnexpectedFailure = false;
            // contain requests that we will set the detector's exception to
            // EndRunException (stop now = false)
            Map<String, Exception> stopDetectorRequests = null;
            for (MultiGetItemResponse itemResponse : itemResponses) {
                String modelId = itemResponse.getId();
                if (itemResponse.isFailed()) {
                    final Exception failure = itemResponse.getFailure().getFailure();
                    if (failure instanceof IndexNotFoundException) {
                        for (FeatureRequest origRequest : toProcess) {
                            // If it is checkpoint index not found exception, I don't
                            // need to retry as checkpoint read is bound to fail. Just
                            // send everything to the cold start queue and return.
                            coldStartWorker.put(origRequest);
                        }
                        return;
                    } else if (ExceptionUtil.isRetryAble(failure)) {
                        if (retryableRequests == null) {
                            retryableRequests = new HashSet<>();
                        }
                        retryableRequests.add(modelId);
                    } else if (ExceptionUtil.isOverloaded(failure)) {
                        LOG.error("too many get model checkpoint requests or shard not available");
                        setCoolDownStart();
                    } else {
                        // some unexpected bug occurred or cluster is unstable (e.g., ClusterBlockException) or index is red (e.g.
                        // NoShardAvailableActionException) while fetching a checkpoint. As this might happen for a large amount
                        // of entities, we don't want to flood logs with such exception trace. Only print it once.
                        if (!printedUnexpectedFailure) {
                            LOG.error("Unexpected failure", failure);
                            printedUnexpectedFailure = true;
                        }
                        if (stopDetectorRequests == null) {
                            stopDetectorRequests = new HashMap<>();
                        }
                        stopDetectorRequests.put(modelId, failure);
                    }
                } else if (!itemResponse.getResponse().isExists()) {
                    // lazy init as we don't expect retrying happens often
                    if (notFoundModels == null) {
                        notFoundModels = new HashSet<>();
                    }
                    notFoundModels.add(modelId);
                } else {
                    successfulRequests.put(modelId, itemResponse);
                }
            }

            // deal with not found model
            if (notFoundModels != null) {
                for (FeatureRequest origRequest : toProcess) {
                    String modelId = origRequest.getModelId();
                    if (modelId != null && notFoundModels.contains(modelId)) {
                        // submit to cold start queue
                        coldStartWorker.put(origRequest);
                    }
                }
            }

            // deal with failures that we will retry for a limited amount of times
            // before stopping the detector
            // We cannot just loop over stopDetectorRequests instead of toProcess
            // because we need detector id from toProcess' elements. stopDetectorRequests only has model id.
            if (stopDetectorRequests != null) {
                for (FeatureRequest origRequest : toProcess) {
                    String modelId = origRequest.getModelId();
                    if (modelId != null && stopDetectorRequests.containsKey(modelId)) {
                        String configID = origRequest.getConfigId();
                        nodeStateManager
                            .setException(
                                configID,
                                new EndRunException(configID, CommonMessages.BUG_RESPONSE, stopDetectorRequests.get(modelId), false)
                            );
                        // once one EndRunException is set, we can break; no point setting the exception repeatedly
                        break;
                    }
                }
            }

            if (successfulRequests.isEmpty() && (retryableRequests == null || retryableRequests.isEmpty())) {
                // don't need to proceed further since no checkpoint is available
                return;
            }
            processCheckpointIteration(0, toProcess, successfulRequests, retryableRequests);
        }, exception -> {
            if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get model checkpoint requests or shard not available");
                setCoolDownStart();
            } else if (ExceptionUtil.isRetryAble(exception)) {
                // retry all of them
                putAll(toProcess);
            } else {
                LOG.error("Fail to restore models", exception);
            }
        });
    }

    protected void processCheckpointIteration(
        int i,
        List<FeatureRequest> toProcess,
        Map<String, MultiGetItemResponse> successfulRequests,
        Set<String> retryableRequests
    ) {
        if (i >= toProcess.size()) {
            return;
        }

        // whether we will process next response in callbacks
        // if false, finally will process next checkpoints
        boolean processNextInCallBack = false;
        try {
            FeatureRequest origRequest = toProcess.get(i);

            String modelId = origRequest.getModelId();
            if (null == modelId) {
                return;
            }

            String configId = origRequest.getConfigId();

            MultiGetItemResponse checkpointResponse = successfulRequests.get(modelId);

            if (checkpointResponse != null) {
                // successful requests
                ModelState<RCFModelType> modelState = checkpointDao
                    .processHCGetResponse(checkpointResponse.getResponse(), modelId, configId);

                if (null == modelState) {
                    // checkpoint is not available (e.g., too big or corrupted); cold start again
                    coldStartWorker.put(origRequest);
                    return;
                }

                nodeStateManager
                    .getConfig(
                        configId,
                        context,
                        processIterationUsingConfig(
                            origRequest,
                            i,
                            configId,
                            toProcess,
                            successfulRequests,
                            retryableRequests,
                            modelState,
                            modelId
                        )
                    );
                processNextInCallBack = true;
            } else if (retryableRequests != null && retryableRequests.contains(modelId)) {
                // failed requests
                super.put(origRequest);
            }
        } finally {
            if (false == processNextInCallBack) {
                processCheckpointIteration(i + 1, toProcess, successfulRequests, retryableRequests);
            }
        }
    }

    protected ActionListener<Optional<? extends Config>> processIterationUsingConfig(
        FeatureRequest origRequest,
        int index,
        String configId,
        List<FeatureRequest> toProcess,
        Map<String, MultiGetItemResponse> successfulRequests,
        Set<String> retryableRequests,
        ModelState<RCFModelType> restoredModelState,
        String modelId
    ) {
        return ActionListenerExecutor.wrap(configOptional -> {
            if (configOptional.isEmpty()) {
                LOG.warn(new ParameterizedMessage("Config [{}] is not available.", configId));
                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                return;
            }

            Config config = configOptional.get();

            boolean processed = inferencer
                .process(
                    new Sample(
                        origRequest.getCurrentFeature(),
                        Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
                        Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + config.getIntervalInMilliseconds())
                    ),
                    restoredModelState,
                    config,
                    origRequest.getTaskId()
                );
            if (processed) {
                // try to load to cache
                boolean loaded = cacheProvider.get().hostIfPossible(config, restoredModelState);

                if (false == loaded) {
                    // not in memory. Maybe cold entities or some other entities
                    // have filled the slot while waiting for loading checkpoints.
                    checkpointWriteWorker.write(restoredModelState, true, RequestPriority.LOW);
                }
            }

            processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
        }, exception -> {
            LOG.error(new ParameterizedMessage("fail to get checkpoint [{}]", modelId, exception));
            nodeStateManager.setException(configId, exception);
            processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
        }, threadPool.executor(threadPoolName));
    }
}
