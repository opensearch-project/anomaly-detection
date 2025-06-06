/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ratelimit;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.ExceptionUtil;

public abstract class CheckpointWriteWorker<RCFModelType, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>>
    extends BatchWorker<CheckpointWriteRequest, BulkRequest, BulkResponse> {
    private static final Logger LOG = LogManager.getLogger(CheckpointWriteWorker.class);

    protected final CheckpointDaoType checkpoint;
    protected final String indexName;
    protected final Duration checkpointInterval;

    public CheckpointWriteWorker(
        String queueName,
        long heapSize,
        int singleRequestSize,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService circuitBreakerService,
        ThreadPool threadPool,
        String threadPoolName,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Setting<Integer> concurrencySetting,
        Duration executionTtl,
        Setting<Integer> batchSizeSetting,
        Duration stateTtl,
        NodeStateManager timeSeriesNodeStateManager,
        CheckpointDaoType checkpoint,
        String indexName,
        Duration checkpointInterval,
        AnalysisType context
    ) {
        super(
            queueName,
            heapSize,
            singleRequestSize,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            circuitBreakerService,
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
            timeSeriesNodeStateManager,
            context
        );
        this.checkpoint = checkpoint;
        this.indexName = indexName;
        this.checkpointInterval = checkpointInterval;
    }

    @Override
    protected void executeBatchRequest(BulkRequest request, ActionListener<BulkResponse> listener) {
        checkpoint.batchWrite(request, listener);
    }

    @Override
    protected BulkRequest toBatchRequest(List<CheckpointWriteRequest> toProcess) {
        final BulkRequest bulkRequest = new BulkRequest();
        for (CheckpointWriteRequest request : toProcess) {
            bulkRequest.add(request.getUpdateRequest());
        }
        return bulkRequest;
    }

    @Override
    protected ActionListener<BulkResponse> getResponseListener(List<CheckpointWriteRequest> toProcess, BulkRequest batchRequest) {
        return ActionListener.wrap(response -> {
            for (BulkItemResponse r : response.getItems()) {
                if (r.getFailureMessage() != null) {
                    // maybe indicating a bug
                    // don't retry failed requests since checkpoints are too large (250KB+)
                    // Later maintenance window or cold start or cache remove will retry saving
                    LOG.error(r.getFailureMessage());
                }
            }
        }, exception -> {
            if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get AD model checkpoint requests or shard not avialble");
                setCoolDownStart();
            }

            for (CheckpointWriteRequest request : toProcess) {
                nodeStateManager.setException(request.getConfigId(), exception);
            }

            // don't retry failed requests since checkpoints are too large (250KB+)
            // Later maintenance window or cold start or cache remove will retry saving
            LOG.error("Fail to save models", exception);
        });
    }

    /**
     * Prepare bulking the input model state to the checkpoint index.
     * We don't save checkpoints within checkpointInterval again, except this
     * is a high priority request (e.g., from cold start).
     * This method will update the input state's last checkpoint time if the
     *  checkpoint is staged (ready to be written in the next batch).
     * @param modelState Model state
     * @param forceWrite whether we should write no matter what
     * @param priority how urgent the write is
     */
    public void write(ModelState<RCFModelType> modelState, boolean forceWrite, RequestPriority priority) {
        if (checkpoint.shouldSave(modelState, forceWrite, checkpointInterval, clock)) {
            String configId = modelState.getConfigId();
            String modelId = modelState.getModelId();
            if (modelId == null || configId == null) {
                return;
            }

            // run once won't write checkpoint. Safe to cache config
            nodeStateManager.getConfig(configId, context, true, onGetConfig(configId, modelId, modelState, priority));
        }
    }

    private ActionListener<Optional<? extends Config>> onGetConfig(
        String configId,
        String modelId,
        ModelState<RCFModelType> modelState,
        RequestPriority priority
    ) {
        return ActionListener.wrap(configOptional -> {
            if (false == configOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("Config [{}] is not available.", configId));
                return;
            }

            Config config = configOptional.get();
            try {
                Map<String, Object> source = checkpoint.toIndexSource(modelState);

                // the model state is bloated or we have bugs, skip
                if (source == null || source.isEmpty()) {
                    return;
                }

                modelState.setLastCheckpointTime(clock.instant());
                CheckpointWriteRequest request = new CheckpointWriteRequest(
                    System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                    configId,
                    priority,
                    // If the document does not already exist, the contents of the upsert element
                    // are inserted as a new document.
                    // If the document exists, update fields in the map
                    new UpdateRequest(indexName, modelId).docAsUpsert(true).doc(source)
                );

                put(request);
            } catch (Exception e) {
                // Example exception:
                // ConcurrentModificationException when calling toCheckpoint
                // and updating rcf model at the same time. To prevent this,
                // we need to have a deep copy of models or have a lock. Both
                // options are costly.
                // As we are gonna retry serializing either when the entity is
                // evicted out of cache or during the next maintenance period,
                // don't do anything when the exception happens.
                LOG.error(new ParameterizedMessage("Exception while serializing models for [{}]", modelId), e);
            }

        }, exception -> { LOG.error(new ParameterizedMessage("fail to get config [{}]", configId), exception); });
    }

    public void writeAll(List<ModelState<RCFModelType>> modelStates, String configId, boolean forceWrite, RequestPriority priority) {
        ActionListener<Optional<? extends Config>> onGetForAll = ActionListener.wrap(configOptional -> {
            if (false == configOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("Config [{}] is not available.", configId));
                return;
            }

            Config config = configOptional.get();
            try {
                List<CheckpointWriteRequest> allRequests = new ArrayList<>();
                for (ModelState<RCFModelType> state : modelStates) {
                    if (!checkpoint.shouldSave(state, forceWrite, checkpointInterval, clock)) {
                        continue;
                    }

                    Map<String, Object> source = checkpoint.toIndexSource(state);
                    String modelId = state.getModelId();

                    // the model state is bloated or empty (empty samples and models), skip
                    if (source == null || source.isEmpty() || Strings.isEmpty(modelId)) {
                        continue;
                    }

                    state.setLastCheckpointTime(clock.instant());
                    allRequests
                        .add(
                            new CheckpointWriteRequest(
                                System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                                configId,
                                priority,
                                // If the document does not already exist, the contents of the upsert element
                                // are inserted as a new document.
                                // If the document exists, update fields in the map
                                new UpdateRequest(indexName, modelId).docAsUpsert(true).doc(source)
                            )
                        );
                }

                putAll(allRequests);
            } catch (Exception e) {
                // Example exception:
                // ConcurrentModificationException when calling toCheckpoint
                // and updating rcf model at the same time. To prevent this,
                // we need to have a deep copy of models or have a lock. Both
                // options are costly.
                // As we are gonna retry serializing either when the entity is
                // evicted out of cache or during the next maintenance period,
                // don't do anything when the exception happens.
                LOG.info(new ParameterizedMessage("Exception while serializing models for [{}]", configId), e);
            }

        }, exception -> { LOG.error(new ParameterizedMessage("fail to get config [{}]", configId), exception); });

        // run once won't write checkpoint. Safe to cache config
        nodeStateManager.getConfig(configId, context, true, onGetForAll);
    }
}
