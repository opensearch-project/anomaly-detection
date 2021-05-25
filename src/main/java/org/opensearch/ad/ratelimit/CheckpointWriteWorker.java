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

package org.opensearch.ad.ratelimit;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

public class CheckpointWriteWorker extends BatchWorker<CheckpointWriteRequest, BulkRequest, BulkResponse> {
    private static final Logger LOG = LogManager.getLogger(CheckpointWriteWorker.class);

    private final AnomalyDetectionIndices indexUtil;
    private final CheckpointDao checkpoint;
    private final String indexName;
    private final Duration checkpointInterval;

    public CheckpointWriteWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        ADCircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        ClientUtil clientUtil,
        Duration executionTtl,
        AnomalyDetectionIndices indexUtil,
        CheckpointDao checkpoint,
        String indexName,
        Duration checkpointInterval,
        NodeStateManager stateManager,
        Duration stateTtl
    ) {
        super(
            "checkpoint-write",
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            clientUtil,
            CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
            executionTtl,
            CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
            stateTtl,
            stateManager
        );
        this.indexUtil = indexUtil;
        this.checkpoint = checkpoint;
        this.indexName = indexName;
        this.checkpointInterval = checkpointInterval;
    }

    @Override
    protected void executeBatchRequest(BulkRequest request, ActionListener<BulkResponse> listener) {
        if (indexUtil.doesCheckpointIndexExist()) {
            clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
        } else {
            indexUtil.initCheckpointIndex(ActionListener.wrap(initResponse -> {
                if (initResponse.isAcknowledged()) {
                    clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
                } else {
                    throw new RuntimeException("Creating checkpoint with mappings call not acknowledged.");
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
                } else {
                    LOG.error(String.format(Locale.ROOT, "Unexpected error creating checkpoint index"), exception);
                    listener.onFailure(exception);
                }
            }));
        }
    }

    @Override
    protected BulkRequest toBatchRequest(List<CheckpointWriteRequest> toProcess) {
        final BulkRequest bulkRequest = new BulkRequest();
        for (CheckpointWriteRequest request : toProcess) {
            bulkRequest.add(request.getIndexRequest());
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
                    // Later maintenance window or cold start will retry saving
                    LOG.error(r.getFailureMessage());
                }
            }
        }, exception -> {
            if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get AD model checkpoint requests or shard not avialble");
                setCoolDownStart();
            }

            for (CheckpointWriteRequest request : toProcess) {
                nodeStateManager.setException(request.getDetectorId(), exception);
            }

            // don't retry failed requests since checkpoints are too large (250KB+)
            // Later maintenance window or cold start will retry saving
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
    public void write(ModelState<EntityModel> modelState, boolean forceWrite, RequestPriority priority) {
        Instant instant = modelState.getLastCheckpointTime();
        if ((instant == Instant.MIN || instant.plus(checkpointInterval).isAfter(clock.instant())) && !forceWrite) {
            return;
        }

        if (modelState.getModel() != null) {
            try {
                String detectorId = modelState.getDetectorId();
                String modelId = modelState.getModelId();
                if (modelId == null || detectorId == null) {
                    return;
                }

                nodeStateManager.getAnomalyDetector(detectorId, onGetDetector(detectorId, modelId, modelState, priority));
            } catch (ConcurrentModificationException e) {
                LOG.info(new ParameterizedMessage("Concurrent modification while serializing models for [{}]", modelState), e);
            }
        }
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        String detectorId,
        String modelId,
        ModelState<EntityModel> modelState,
        RequestPriority priority
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                return;
            }

            AnomalyDetector detector = detectorOptional.get();
            try {
                Map<String, Object> source = checkpoint.toIndexSource(modelState);

                // the model state is bloated or we have bugs, skip
                if (source == null || source.isEmpty()) {
                    return;
                }

                CheckpointWriteRequest request = new CheckpointWriteRequest(
                    System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                    detectorId,
                    priority,
                    new IndexRequest(indexName).id(modelId).source(source)
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
                LOG.info(new ParameterizedMessage("Exception while serializing models for [{}]", modelId), e);
            }

        }, exception -> { LOG.error(new ParameterizedMessage("fail to get detector [{}]", detectorId), exception); });
    }

    public void writeAll(List<ModelState<EntityModel>> modelStates, String detectorId, boolean forceWrite, RequestPriority priority) {
        ActionListener<Optional<AnomalyDetector>> onGetForAll = ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                return;
            }

            AnomalyDetector detector = detectorOptional.get();
            try {
                List<CheckpointWriteRequest> allRequests = new ArrayList<>();
                for (ModelState<EntityModel> state : modelStates) {
                    Instant instant = state.getLastCheckpointTime();
                    if ((instant == Instant.MIN || instant.plus(checkpointInterval).isAfter(clock.instant())) && !forceWrite) {
                        continue;
                    }

                    Map<String, Object> source = checkpoint.toIndexSource(state);
                    String modelId = state.getModelId();

                    // the model state is bloated, skip
                    if (source == null || source.isEmpty() || Strings.isEmpty(modelId)) {
                        continue;
                    }

                    allRequests
                        .add(
                            new CheckpointWriteRequest(
                                System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                                detectorId,
                                priority,
                                new IndexRequest(indexName).id(modelId).source(source)
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
                LOG.info(new ParameterizedMessage("Exception while serializing models for [{}]", detectorId), e);
            }

        }, exception -> { LOG.error(new ParameterizedMessage("fail to get detector [{}]", detectorId), exception); });

        nodeStateManager.getAnomalyDetector(detectorId, onGetForAll);
    }
}
