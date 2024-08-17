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

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ADRealTimeInferencer;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ADColdEntityWorker;
import org.opensearch.ad.ratelimit.ADColdStartWorker;
import org.opensearch.ad.ratelimit.ADSaveResultStrategy;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.transport.EntityResultProcessor;
import org.opensearch.timeseries.transport.EntityResultRequest;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Entry-point for HCAD workflow. We have created multiple queues for
 * coordinating the workflow. The overrall workflow is: 1. We store as many
 * frequently used entity models in a cache as allowed by the memory limit (10%
 * heap). If an entity feature is a hit, we use the in-memory model to detect
 * anomalies and record results using the result write queue. 2. If an entity
 * feature is a miss, we check if there is free memory or any other entity's
 * model can be evacuated. An in-memory entity's frequency may be lower compared
 * to the cache miss entity. If that's the case, we replace the lower frequency
 * entity's model with the higher frequency entity's model. To load the higher
 * frequency entity's model, we first check if a model exists on disk by sending
 * a checkpoint read queue request. If there is a checkpoint, we load it to
 * memory, perform detection, and save the result using the result write queue.
 * Otherwise, we enqueue a cold start request to the cold start queue for model
 * training. If training is successful, we save the learned model via the
 * checkpoint write queue. 3. We also have the cold entity queue configured for
 * cold entities, and the model training and inference are connected by serial
 * juxtaposition to limit resource usage.
 */
public class EntityADResultTransportAction extends HandledTransportAction<EntityResultRequest, AcknowledgedResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityADResultTransportAction.class);
    private CircuitBreakerService adCircuitBreakerService;
    private CacheProvider<ThresholdedRandomCutForest, ADPriorityCache> cache;
    private final NodeStateManager stateManager;
    private ThreadPool threadPool;
    private EntityResultProcessor<ThresholdedRandomCutForest, AnomalyResult, ThresholdingResult, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADColdStart, ADModelManager, ADPriorityCache, ADSaveResultStrategy, ADColdStartWorker, ADRealTimeInferencer, ADCheckpointReadWorker, ADColdEntityWorker> intervalDataProcessor;

    private final ADCacheProvider entityCache;
    private final ADCheckpointReadWorker checkpointReadQueue;
    private final ADColdEntityWorker coldEntityQueue;
    private final ADRealTimeInferencer inferencer;

    @Inject
    public EntityADResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        CircuitBreakerService adCircuitBreakerService,
        ADCacheProvider entityCache,
        NodeStateManager stateManager,
        ADIndexManagement indexUtil,
        ADCheckpointReadWorker checkpointReadQueue,
        ADColdEntityWorker coldEntityQueue,
        ThreadPool threadPool,
        ADRealTimeInferencer inferencer
    ) {
        super(EntityADResultAction.NAME, transportService, actionFilters, EntityResultRequest::new);
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.cache = entityCache;
        this.stateManager = stateManager;
        this.threadPool = threadPool;

        this.entityCache = entityCache;
        this.checkpointReadQueue = checkpointReadQueue;
        this.coldEntityQueue = coldEntityQueue;
        this.intervalDataProcessor = null;
        this.inferencer = inferencer;
    }

    @Override
    protected void doExecute(Task task, EntityResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            threadPool
                .executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)
                .execute(() -> cache.get().releaseMemoryForOpenCircuitBreaker());
            listener.onFailure(new LimitExceededException(request.getConfigId(), CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String detectorId = request.getConfigId();

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

            this.intervalDataProcessor = new EntityResultProcessor<>(
                entityCache,
                checkpointReadQueue,
                coldEntityQueue,
                inferencer,
                threadPool,
                TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME
            );

            stateManager
                .getConfig(
                    detectorId,
                    request.getAnalysisType(),
                    intervalDataProcessor.onGetConfig(listener, detectorId, request, previousException, request.getAnalysisType())
                );
        } catch (Exception exception) {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        }
    }
}
