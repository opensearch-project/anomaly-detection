/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.caching.ADCacheBuffer;
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
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ADColdStartWorker;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.ad.ratelimit.ADSaveResultStrategy;
import org.opensearch.common.inject.Inject;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.transport.AbstractSingleStreamResultTransportAction;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ADSingleStreamResultTransportAction extends
    AbstractSingleStreamResultTransportAction<ThresholdedRandomCutForest, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADCheckpointMaintainWorker, ADCacheBuffer, ADPriorityCache, ADCacheProvider, AnomalyResult, ThresholdingResult, ADColdStart, ADModelManager, ADPriorityCache, ADSaveResultStrategy, ADColdStartWorker, ADRealTimeInferencer, ADCheckpointReadWorker, ADResultWriteRequest> {

    @Inject
    public ADSingleStreamResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CircuitBreakerService circuitBreakerService,
        ADCacheProvider cache,
        NodeStateManager stateManager,
        ADCheckpointReadWorker checkpointReadQueue,
        ADRealTimeInferencer inferencer,
        ThreadPool threadPool
    ) {
        super(
            transportService,
            actionFilters,
            circuitBreakerService,
            cache,
            stateManager,
            checkpointReadQueue,
            ADSingleStreamResultAction.NAME,
            AnalysisType.AD,
            inferencer,
            threadPool,
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME
        );
    }

    @Override
    public ADResultWriteRequest createResultWriteRequest(Config config, AnomalyResult result) {
        return new ADResultWriteRequest(
            System.currentTimeMillis() + config.getIntervalInMilliseconds(),
            config.getId(),
            RequestPriority.MEDIUM,
            result,
            config.getCustomResultIndexOrAlias()
        );
    }

}
