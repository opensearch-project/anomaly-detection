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

package org.opensearch.ad.transport;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.CheckpointReadWorker;
import org.opensearch.ad.ratelimit.ColdEntityWorker;
import org.opensearch.ad.ratelimit.EntityColdStartWorker;
import org.opensearch.ad.ratelimit.EntityFeatureRequest;
import org.opensearch.ad.ratelimit.RequestPriority;
import org.opensearch.ad.ratelimit.ResultWriteRequest;
import org.opensearch.ad.ratelimit.ResultWriteWorker;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.transport.TransportService;

/**
 * Entry-point for HCAD workflow.  We have created multiple queues for coordinating
 * the workflow. The overrall workflow is:
 * 1. We store as many frequently used entity models in a cache as allowed by the
 *  memory limit (10% heap). If an entity feature is a hit, we use the in-memory model
 *  to detect anomalies and record results using the result write queue.
 * 2. If an entity feature is a miss, we check if there is free memory or any other
 *  entity's model can be evacuated. An in-memory entity's frequency may be lower
 *  compared to the cache miss entity. If that's the case, we replace the lower
 *  frequency entity's model with the higher frequency entity's model. To load the
 *  higher frequency entity's model, we first check if a model exists on disk by
 *  sending a checkpoint read queue request. If there is a checkpoint, we load it
 *  to memory, perform detection, and save the result using the result write queue.
 *  Otherwise, we enqueue a cold start request to the cold start queue for model
 *  training. If training is successful, we save the learned model via the checkpoint
 *  write queue.
 * 3. We also have the cold entity queue configured for cold entities, and the model
 * training and inference are connected by serial juxtaposition to limit resource usage.
 */
public class EntityResultTransportAction extends HandledTransportAction<EntityResultRequest, AcknowledgedResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityResultTransportAction.class);
    private ModelManager modelManager;
    private ADCircuitBreakerService adCircuitBreakerService;
    private CacheProvider cache;
    private final NodeStateManager stateManager;
    private ADIndexManagement indexUtil;
    private ResultWriteWorker resultWriteQueue;
    private CheckpointReadWorker checkpointReadQueue;
    private ColdEntityWorker coldEntityQueue;
    private ThreadPool threadPool;
    private EntityColdStartWorker entityColdStartWorker;
    private ADStats adStats;

    @Inject
    public EntityResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService,
        CacheProvider entityCache,
        NodeStateManager stateManager,
        ADIndexManagement indexUtil,
        ResultWriteWorker resultWriteQueue,
        CheckpointReadWorker checkpointReadQueue,
        ColdEntityWorker coldEntityQueue,
        ThreadPool threadPool,
        EntityColdStartWorker entityColdStartWorker,
        ADStats adStats
    ) {
        super(EntityResultAction.NAME, transportService, actionFilters, EntityResultRequest::new);
        this.modelManager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.cache = entityCache;
        this.stateManager = stateManager;
        this.indexUtil = indexUtil;
        this.resultWriteQueue = resultWriteQueue;
        this.checkpointReadQueue = checkpointReadQueue;
        this.coldEntityQueue = coldEntityQueue;
        this.threadPool = threadPool;
        this.entityColdStartWorker = entityColdStartWorker;
        this.adStats = adStats;
    }

    @Override
    protected void doExecute(Task task, EntityResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            threadPool
                .executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)
                .execute(() -> cache.get().releaseMemoryForOpenCircuitBreaker());
            listener.onFailure(new LimitExceededException(request.getId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String detectorId = request.getId();

            Optional<Exception> previousException = stateManager.fetchExceptionAndClear(detectorId);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error("Previous exception of {}: {}", detectorId, exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }

                listener = ExceptionUtil.wrapListener(listener, exception, detectorId);
            }

            stateManager.getConfig(detectorId, AnalysisType.AD, onGetDetector(listener, detectorId, request, previousException));
        } catch (Exception exception) {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        }
    }

    private ActionListener<Optional<? extends Config>> onGetDetector(
        ActionListener<AcknowledgedResponse> listener,
        String detectorId,
        EntityResultRequest request,
        Optional<Exception> prevException
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (!detectorOptional.isPresent()) {
                listener.onFailure(new EndRunException(detectorId, "AnomalyDetector is not available.", false));
                return;
            }

            AnomalyDetector detector = (AnomalyDetector) detectorOptional.get();

            if (request.getEntities() == null) {
                listener.onFailure(new EndRunException(detectorId, "Fail to get any entities from request.", false));
                return;
            }

            Instant executionStartTime = Instant.now();
            Map<Entity, double[]> cacheMissEntities = new HashMap<>();
            for (Entry<Entity, double[]> entityEntry : request.getEntities().entrySet()) {
                Entity categoricalValues = entityEntry.getKey();

                if (isEntityFromOldNodeMsg(categoricalValues)
                    && detector.getCategoryFields() != null
                    && detector.getCategoryFields().size() == 1) {
                    Map<String, String> attrValues = categoricalValues.getAttributes();
                    // handle a request from a version before OpenSearch 1.1.
                    categoricalValues = Entity
                        .createSingleAttributeEntity(detector.getCategoryFields().get(0), attrValues.get(ADCommonName.EMPTY_FIELD));
                }

                Optional<String> modelIdOptional = categoricalValues.getModelId(detectorId);
                if (false == modelIdOptional.isPresent()) {
                    continue;
                }

                String modelId = modelIdOptional.get();
                double[] datapoint = entityEntry.getValue();
                ModelState<EntityModel> entityModel = cache.get().get(modelId, detector);
                if (entityModel == null) {
                    // cache miss
                    cacheMissEntities.put(categoricalValues, datapoint);
                    continue;
                }
                try {
                    ThresholdingResult result = modelManager
                        .getAnomalyResultForEntity(datapoint, entityModel, modelId, categoricalValues, detector.getShingleSize());
                    // result.getRcfScore() = 0 means the model is not initialized
                    // result.getGrade() = 0 means it is not an anomaly
                    // So many OpenSearchRejectedExecutionException if we write no matter what
                    if (result.getRcfScore() > 0) {
                        List<AnomalyResult> resultsToSave = result
                            .toIndexableResults(
                                detector,
                                Instant.ofEpochMilli(request.getStart()),
                                Instant.ofEpochMilli(request.getEnd()),
                                executionStartTime,
                                Instant.now(),
                                ParseUtils.getFeatureData(datapoint, detector),
                                Optional.ofNullable(categoricalValues),
                                indexUtil.getSchemaVersion(ADIndex.RESULT),
                                modelId,
                                null,
                                null
                            );
                        for (AnomalyResult r : resultsToSave) {
                            resultWriteQueue
                                .put(
                                    new ResultWriteRequest(
                                        System.currentTimeMillis() + detector.getIntervalInMilliseconds(),
                                        detectorId,
                                        result.getGrade() > 0 ? RequestPriority.HIGH : RequestPriority.MEDIUM,
                                        r,
                                        detector.getCustomResultIndex()
                                    )
                                );
                        }
                    }
                } catch (IllegalArgumentException e) {
                    // fail to score likely due to model corruption. Re-cold start to recover.
                    LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", modelId), e);
                    adStats.getStat(StatNames.MODEL_CORRUTPION_COUNT.getName()).increment();
                    cache.get().removeEntityModel(detectorId, modelId);
                    entityColdStartWorker
                        .put(
                            new EntityFeatureRequest(
                                System.currentTimeMillis() + detector.getIntervalInMilliseconds(),
                                detectorId,
                                RequestPriority.MEDIUM,
                                categoricalValues,
                                datapoint,
                                request.getStart()
                            )
                        );
                }
            }

            // split hot and cold entities
            Pair<List<Entity>, List<Entity>> hotColdEntities = cache
                .get()
                .selectUpdateCandidate(cacheMissEntities.keySet(), detectorId, detector);

            List<EntityFeatureRequest> hotEntityRequests = new ArrayList<>();
            List<EntityFeatureRequest> coldEntityRequests = new ArrayList<>();

            for (Entity hotEntity : hotColdEntities.getLeft()) {
                double[] hotEntityValue = cacheMissEntities.get(hotEntity);
                if (hotEntityValue == null) {
                    LOG.error(new ParameterizedMessage("feature value should not be null: [{}]", hotEntity));
                    continue;
                }
                hotEntityRequests
                    .add(
                        new EntityFeatureRequest(
                            System.currentTimeMillis() + detector.getIntervalInMilliseconds(),
                            detectorId,
                            // hot entities has MEDIUM priority
                            RequestPriority.MEDIUM,
                            hotEntity,
                            hotEntityValue,
                            request.getStart()
                        )
                    );
            }

            for (Entity coldEntity : hotColdEntities.getRight()) {
                double[] coldEntityValue = cacheMissEntities.get(coldEntity);
                if (coldEntityValue == null) {
                    LOG.error(new ParameterizedMessage("feature value should not be null: [{}]", coldEntity));
                    continue;
                }
                coldEntityRequests
                    .add(
                        new EntityFeatureRequest(
                            System.currentTimeMillis() + detector.getIntervalInMilliseconds(),
                            detectorId,
                            // cold entities has LOW priority
                            RequestPriority.LOW,
                            coldEntity,
                            coldEntityValue,
                            request.getStart()
                        )
                    );
            }

            checkpointReadQueue.putAll(hotEntityRequests);
            coldEntityQueue.putAll(coldEntityRequests);

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
                        "fail to get entity's anomaly grade for detector [{}]: start: [{}], end: [{}]",
                        detectorId,
                        request.getStart(),
                        request.getEnd()
                    ),
                    exception
                );
            listener.onFailure(exception);
        });
    }

    /**
     * Whether the received entity comes from an node that doesn't support multi-category fields.
     * This can happen during rolling-upgrade or blue/green deployment.
     *
     * Specifically, when receiving an EntityResultRequest from an incompatible node,
     * EntityResultRequest(StreamInput in) gets an String that represents an entity.
     * But Entity class requires both an category field name and value. Since we
     * don't have access to detector config in EntityResultRequest(StreamInput in),
     * we put CommonName.EMPTY_FIELD as the placeholder.  In this method,
     * we use the same CommonName.EMPTY_FIELD to check if the deserialized entity
     * comes from an incompatible node.  If it is, we will add the field name back
     * as EntityResultTranportAction has access to the detector config object.
     *
     * @param categoricalValues deserialized Entity from inbound message.
     * @return Whether the received entity comes from an node that doesn't support multi-category fields.
     */
    private boolean isEntityFromOldNodeMsg(Entity categoricalValues) {
        Map<String, String> attrValues = categoricalValues.getAttributes();
        return (attrValues != null && attrValues.containsKey(ADCommonName.EMPTY_FIELD));
    }
}
