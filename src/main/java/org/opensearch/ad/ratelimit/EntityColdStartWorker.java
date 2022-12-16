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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.ml.EntityColdStarter;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

/**
 * A queue for HCAD model training (a.k.a. cold start). As model training is a
 * pretty expensive operation, we pull cold start requests from the queue in a
 * serial fashion. Each detector has an equal chance of being pulled. The equal
 * probability is achieved by putting model training requests for different
 * detectors into different segments and pulling requests from segments in a
 * round-robin fashion.
 *
 */
public class EntityColdStartWorker extends SingleRequestWorker<EntityRequest> {
    private static final Logger LOG = LogManager.getLogger(EntityColdStartWorker.class);
    public static final String WORKER_NAME = "cold-start";

    private final EntityColdStarter entityColdStarter;
    private final CacheProvider cacheProvider;

    public EntityColdStartWorker(
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
        EntityColdStarter entityColdStarter,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        CacheProvider cacheProvider
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
            ENTITY_COLD_START_QUEUE_CONCURRENCY,
            executionTtl,
            stateTtl,
            nodeStateManager
        );
        this.entityColdStarter = entityColdStarter;
        this.cacheProvider = cacheProvider;
    }

    @Override
    protected void executeRequest(EntityRequest coldStartRequest, ActionListener<Void> listener) {
        String detectorId = coldStartRequest.getDetectorId();

        Optional<String> modelId = coldStartRequest.getModelId();

        if (false == modelId.isPresent()) {
            String error = String.format(Locale.ROOT, "Fail to get model id for request %s", coldStartRequest);
            LOG.warn(error);
            listener.onFailure(new RuntimeException(error));
            return;
        }

        ModelState<EntityModel> modelState = new ModelState<>(
            new EntityModel(coldStartRequest.getEntity(), new ArrayDeque<>(), null),
            modelId.get(),
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        ActionListener<Void> coldStartListener = ActionListener.wrap(r -> {
            nodeStateManager.getAnomalyDetector(detectorId, ActionListener.wrap(detectorOptional -> {
                try {
                    if (!detectorOptional.isPresent()) {
                        LOG
                            .error(
                                new ParameterizedMessage(
                                    "fail to load trained model [{}] to cache due to the detector not being found.",
                                    modelState.getModelId()
                                )
                            );
                        return;
                    }
                    AnomalyDetector detector = detectorOptional.get();
                    EntityModel model = modelState.getModel();
                    // load to cache if cold start succeeds
                    if (model != null && model.getTrcf() != null) {
                        cacheProvider.get().hostIfPossible(detector, modelState);
                    }
                } finally {
                    listener.onResponse(null);
                }
            }, listener::onFailure));

        }, e -> {
            try {
                if (ExceptionUtil.isOverloaded(e)) {
                    LOG.error("OpenSearch is overloaded");
                    setCoolDownStart();
                }
                nodeStateManager.setException(detectorId, e);
            } finally {
                listener.onFailure(e);
            }
        });

        entityColdStarter.trainModel(coldStartRequest.getEntity(), detectorId, modelState, coldStartListener);
    }
}
