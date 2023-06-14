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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.util.ParseUtils;

/**
 * a queue for loading model checkpoint. The read is a multi-get query. Possible results are:
 * a). If a checkpoint is not found, we forward that request to the cold start queue.
 * b). When a request gets errors, the queue does not change its expiry time and puts
 *  that request to the end of the queue and automatically retries them before they expire.
 * c) When a checkpoint is found, we load that point to memory and score the input
 * data point and save the result if a complete model exists. Otherwise, we enqueue
 * the sample. If we can host that model in memory (e.g., there is enough memory),
 * we put the loaded model to cache. Otherwise (e.g., a cold entity), we write the
 * updated checkpoint back to disk.
 *
 */
public class CheckpointReadWorker extends BatchWorker<EntityFeatureRequest, MultiGetRequest, MultiGetResponse> {
    private static final Logger LOG = LogManager.getLogger(CheckpointReadWorker.class);
    public static final String WORKER_NAME = "checkpoint-read";
    private final ModelManager modelManager;
    private final CheckpointDao checkpointDao;
    private final EntityColdStartWorker entityColdStartQueue;
    private final ResultWriteWorker resultWriteQueue;
    private final AnomalyDetectionIndices indexUtil;
    private final CacheProvider cacheProvider;
    private final CheckpointWriteWorker checkpointWriteQueue;
    private final ADStats adStats;

    public CheckpointReadWorker(
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
        Duration executionTtl,
        ModelManager modelManager,
        CheckpointDao checkpointDao,
        EntityColdStartWorker entityColdStartQueue,
        ResultWriteWorker resultWriteQueue,
        NodeStateManager stateManager,
        AnomalyDetectionIndices indexUtil,
        CacheProvider cacheProvider,
        Duration stateTtl,
        CheckpointWriteWorker checkpointWriteQueue,
        ADStats adStats
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
            AD_CHECKPOINT_READ_QUEUE_CONCURRENCY,
            executionTtl,
            AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
            stateTtl,
            stateManager
        );

        this.modelManager = modelManager;
        this.checkpointDao = checkpointDao;
        this.entityColdStartQueue = entityColdStartQueue;
        this.resultWriteQueue = resultWriteQueue;
        this.indexUtil = indexUtil;
        this.cacheProvider = cacheProvider;
        this.checkpointWriteQueue = checkpointWriteQueue;
        this.adStats = adStats;
    }

    @Override
    protected void executeBatchRequest(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        checkpointDao.batchRead(request, listener);
    }

    /**
     * Convert the input list of EntityFeatureRequest to a multi-get request.
     * RateLimitedRequestWorker.getRequests has already limited the number of
     * requests in the input list. So toBatchRequest method can take the input
     * and send the multi-get directly.
     * @return The converted multi-get request
     */
    @Override
    protected MultiGetRequest toBatchRequest(List<EntityFeatureRequest> toProcess) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (EntityRequest request : toProcess) {
            Optional<String> modelId = request.getModelId();
            if (false == modelId.isPresent()) {
                continue;
            }
            multiGetRequest.add(new MultiGetRequest.Item(ADCommonName.CHECKPOINT_INDEX_NAME, modelId.get()));
        }
        return multiGetRequest;
    }

    @Override
    protected ActionListener<MultiGetResponse> getResponseListener(List<EntityFeatureRequest> toProcess, MultiGetRequest batchRequest) {
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
                        for (EntityRequest origRequest : toProcess) {
                            // If it is checkpoint index not found exception, I don't
                            // need to retry as checkpoint read is bound to fail. Just
                            // send everything to the cold start queue and return.
                            entityColdStartQueue.put(origRequest);
                        }
                        return;
                    } else if (ExceptionUtil.isRetryAble(failure)) {
                        if (retryableRequests == null) {
                            retryableRequests = new HashSet<>();
                        }
                        retryableRequests.add(modelId);
                    } else if (ExceptionUtil.isOverloaded(failure)) {
                        LOG.error("too many get AD model checkpoint requests or shard not available");
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
                for (EntityRequest origRequest : toProcess) {
                    Optional<String> modelId = origRequest.getModelId();
                    if (modelId.isPresent() && notFoundModels.contains(modelId.get())) {
                        // submit to cold start queue
                        entityColdStartQueue.put(origRequest);
                    }
                }
            }

            // deal with failures that we will retry for a limited amount of times
            // before stopping the detector
            // We cannot just loop over stopDetectorRequests instead of toProcess
            // because we need detector id from toProcess' elements. stopDetectorRequests only has model id.
            if (stopDetectorRequests != null) {
                for (EntityRequest origRequest : toProcess) {
                    Optional<String> modelId = origRequest.getModelId();
                    if (modelId.isPresent() && stopDetectorRequests.containsKey(modelId.get())) {
                        String adID = origRequest.detectorId;
                        nodeStateManager
                            .setException(
                                adID,
                                new EndRunException(adID, CommonMessages.BUG_RESPONSE, stopDetectorRequests.get(modelId.get()), false)
                            );
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
                LOG.error("too many get AD model checkpoint requests or shard not available");
                setCoolDownStart();
            } else if (ExceptionUtil.isRetryAble(exception)) {
                // retry all of them
                putAll(toProcess);
            } else {
                LOG.error("Fail to restore models", exception);
            }
        });
    }

    private void processCheckpointIteration(
        int i,
        List<EntityFeatureRequest> toProcess,
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
            EntityFeatureRequest origRequest = toProcess.get(i);

            Optional<String> modelIdOptional = origRequest.getModelId();
            if (false == modelIdOptional.isPresent()) {
                return;
            }

            String detectorId = origRequest.getId();
            Entity entity = origRequest.getEntity();

            String modelId = modelIdOptional.get();

            MultiGetItemResponse checkpointResponse = successfulRequests.get(modelId);

            if (checkpointResponse != null) {
                // successful requests
                Optional<Entry<EntityModel, Instant>> checkpoint = checkpointDao
                    .processGetResponse(checkpointResponse.getResponse(), modelId);

                if (false == checkpoint.isPresent()) {
                    // checkpoint is too big
                    return;
                }

                nodeStateManager
                    .getAnomalyDetector(
                        detectorId,
                        onGetDetector(
                            origRequest,
                            i,
                            detectorId,
                            toProcess,
                            successfulRequests,
                            retryableRequests,
                            checkpoint,
                            entity,
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

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        EntityFeatureRequest origRequest,
        int index,
        String detectorId,
        List<EntityFeatureRequest> toProcess,
        Map<String, MultiGetItemResponse> successfulRequests,
        Set<String> retryableRequests,
        Optional<Entry<EntityModel, Instant>> checkpoint,
        Entity entity,
        String modelId
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                return;
            }

            AnomalyDetector detector = detectorOptional.get();

            ModelState<EntityModel> modelState = modelManager
                .processEntityCheckpoint(checkpoint, entity, modelId, detectorId, detector.getShingleSize());

            ThresholdingResult result = null;
            try {
                result = modelManager
                    .getAnomalyResultForEntity(origRequest.getCurrentFeature(), modelState, modelId, entity, detector.getShingleSize());
            } catch (IllegalArgumentException e) {
                // fail to score likely due to model corruption. Re-cold start to recover.
                LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", origRequest.getModelId()), e);
                adStats.getStat(StatNames.MODEL_CORRUTPION_COUNT.getName()).increment();
                if (origRequest.getModelId().isPresent()) {
                    String entityModelId = origRequest.getModelId().get();
                    checkpointDao
                        .deleteModelCheckpoint(
                            entityModelId,
                            ActionListener
                                .wrap(
                                    r -> LOG.debug(new ParameterizedMessage("Succeeded in deleting checkpoint [{}].", entityModelId)),
                                    ex -> LOG.error(new ParameterizedMessage("Failed to delete checkpoint [{}].", entityModelId), ex)
                                )
                        );
                }

                entityColdStartQueue.put(origRequest);
                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                return;
            }

            if (result != null && result.getRcfScore() > 0) {
                AnomalyResult resultToSave = result
                    .toAnomalyResult(
                        detector,
                        Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
                        Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + detector.getIntervalInMilliseconds()),
                        Instant.now(),
                        Instant.now(),
                        ParseUtils.getFeatureData(origRequest.getCurrentFeature(), detector),
                        entity,
                        indexUtil.getSchemaVersion(ADIndex.RESULT),
                        modelId,
                        null,
                        null
                    );

                resultWriteQueue
                    .put(
                        new ResultWriteRequest(
                            origRequest.getExpirationEpochMs(),
                            detectorId,
                            result.getGrade() > 0 ? RequestPriority.HIGH : RequestPriority.MEDIUM,
                            resultToSave,
                            detector.getCustomResultIndex()
                        )
                    );
            }

            // try to load to cache
            boolean loaded = cacheProvider.get().hostIfPossible(detector, modelState);

            if (false == loaded) {
                // not in memory. Maybe cold entities or some other entities
                // have filled the slot while waiting for loading checkpoints.
                checkpointWriteQueue.write(modelState, true, RequestPriority.LOW);
            }

            processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
        }, exception -> {
            LOG.error(new ParameterizedMessage("fail to get checkpoint [{}]", modelId, exception));
            nodeStateManager.setException(detectorId, exception);
            processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
        });
    }
}
