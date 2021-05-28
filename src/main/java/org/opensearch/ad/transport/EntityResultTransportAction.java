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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.EntityColdStarter;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.ratelimit.CheckpointReadWorker;
import org.opensearch.ad.ratelimit.ColdEntityWorker;
import org.opensearch.ad.ratelimit.EntityFeatureRequest;
import org.opensearch.ad.ratelimit.RequestPriority;
import org.opensearch.ad.ratelimit.ResultWriteRequest;
import org.opensearch.ad.ratelimit.ResultWriteWorker;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
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
    private AnomalyDetectionIndices indexUtil;
    private ResultWriteWorker resultWriteQueue;
    private CheckpointReadWorker checkpointReadQueue;
    private EntityColdStarter coldStarter;
    private ColdEntityWorker coldEntityQueue;
    private ThreadPool threadPool;

    @Inject
    public EntityResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService,
        CacheProvider entityCache,
        NodeStateManager stateManager,
        AnomalyDetectionIndices indexUtil,
        ResultWriteWorker resultWriteQueue,
        CheckpointReadWorker checkpointReadQueue,
        EntityColdStarter coldStarer,
        ColdEntityWorker coldEntityQueue,
        ThreadPool threadPool
    ) {
        super(EntityResultAction.NAME, transportService, actionFilters, EntityResultRequest::new);
        this.modelManager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.cache = entityCache;
        this.stateManager = stateManager;
        this.indexUtil = indexUtil;
        this.resultWriteQueue = resultWriteQueue;
        this.checkpointReadQueue = checkpointReadQueue;
        this.coldStarter = coldStarer;
        this.coldEntityQueue = coldEntityQueue;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, EntityResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            threadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME).execute(() -> cache.get().releaseMemoryForOpenCircuitBreaker());
            listener
                .onFailure(new LimitExceededException(request.getDetectorId(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String detectorId = request.getDetectorId();

            Optional<AnomalyDetectionException> previousException = stateManager.fetchExceptionAndClear(detectorId);

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

            stateManager.getAnomalyDetector(detectorId, onGetDetector(listener, detectorId, request, previousException));
        } catch (Exception exception) {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        }
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        ActionListener<AcknowledgedResponse> listener,
        String detectorId,
        EntityResultRequest request,
        Optional<AnomalyDetectionException> prevException
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (!detectorOptional.isPresent()) {
                listener.onFailure(new EndRunException(detectorId, "AnomalyDetector is not available.", true));
                return;
            }

            AnomalyDetector detector = detectorOptional.get();

            if (request.getEntities() == null) {
                listener.onResponse(null);
                return;
            }

            Instant executionStartTime = Instant.now();
            Map<Entity, double[]> cacheMissEntities = new HashMap<>();
            for (Entry<Entity, double[]> entityEntry : request.getEntities().entrySet()) {
                Entity categoricalValues = entityEntry.getKey();

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
                ThresholdingResult result = modelManager
                    .getAnomalyResultForEntity(datapoint, entityModel, modelId, detector, categoricalValues);
                // result.getRcfScore() = 0 means the model is not initialized
                // result.getGrade() = 0 means it is not an anomaly
                // So many OpenSearchRejectedExecutionException if we write no matter what
                if (result.getRcfScore() > 0) {
                    resultWriteQueue
                        .put(
                            new ResultWriteRequest(
                                System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                                detectorId,
                                result.getGrade() > 0 ? RequestPriority.HIGH : RequestPriority.MEDIUM,
                                new AnomalyResult(
                                    detectorId,
                                    null,
                                    result.getRcfScore(),
                                    result.getGrade(),
                                    result.getConfidence(),
                                    ParseUtils.getFeatureData(datapoint, detector),
                                    Instant.ofEpochMilli(request.getStart()),
                                    Instant.ofEpochMilli(request.getEnd()),
                                    executionStartTime,
                                    Instant.now(),
                                    null,
                                    categoricalValues,
                                    detector.getUser(),
                                    indexUtil.getSchemaVersion(ADIndex.RESULT),
                                    modelId
                                )
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
                            System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
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
                            System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
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
}
