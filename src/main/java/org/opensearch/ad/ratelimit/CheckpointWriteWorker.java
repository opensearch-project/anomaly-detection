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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.threadpool.ThreadPool;

public class CheckpointWriteWorker extends BatchWorker<CheckpointWriteRequest, BulkRequest, BulkResponse> {
    private static final Logger LOG = LogManager.getLogger(CheckpointWriteWorker.class);
    public static final String WORKER_NAME = "checkpoint-write";

    private final CheckpointDao checkpoint;
    private final String indexName;
    private final Duration checkpointInterval;

    public CheckpointWriteWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        SDKClusterService clusterService,
        Random random,
        ADCircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Duration executionTtl,
        CheckpointDao checkpoint,
        String indexName,
        Duration checkpointInterval,
        NodeStateManager stateManager,
        Duration stateTtl
    ) {
        super(
            WORKER_NAME,
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
            CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
            executionTtl,
            CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
            stateTtl,
            stateManager
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
                nodeStateManager.setException(request.getDetectorId(), exception);
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
    public void write(ModelState<EntityModel> modelState, boolean forceWrite, RequestPriority priority) {
        Instant instant = modelState.getLastCheckpointTime();
        if (!shouldSave(instant, forceWrite)) {
            return;
        }

        if (modelState.getModel() != null) {
            String detectorId = modelState.getDetectorId();
            String modelId = modelState.getModelId();
            if (modelId == null || detectorId == null) {
                return;
            }

            nodeStateManager.getAnomalyDetector(detectorId, onGetDetector(detectorId, modelId, modelState, priority));
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

                modelState.setLastCheckpointTime(clock.instant());
                CheckpointWriteRequest request = new CheckpointWriteRequest(
                    System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                    detectorId,
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
                    if (!shouldSave(instant, forceWrite)) {
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
                                System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                                detectorId,
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
                LOG.info(new ParameterizedMessage("Exception while serializing models for [{}]", detectorId), e);
            }

        }, exception -> { LOG.error(new ParameterizedMessage("fail to get detector [{}]", detectorId), exception); });

        nodeStateManager.getAnomalyDetector(detectorId, onGetForAll);
    }

    /**
     * Should we save the checkpoint or not
     * @param lastCheckpointTIme Last checkpoint time
     * @param forceWrite Save no matter what
     * @return true when forceWrite is true or we haven't saved checkpoint in the
     *  last checkpoint interval; false otherwise
     */
    private boolean shouldSave(Instant lastCheckpointTIme, boolean forceWrite) {
        return (lastCheckpointTIme != Instant.MIN && lastCheckpointTIme.plus(checkpointInterval).isBefore(clock.instant())) || forceWrite;
    }
}
