/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.common.inject.Inject;
import org.opensearch.forecast.caching.ForecastCacheBuffer;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.ForecastRealTimeInferencer;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastCheckpointMaintainWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointReadWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.forecast.ratelimit.ForecastSaveResultStrategy;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.transport.AbstractSingleStreamResultTransportAction;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastSingleStreamResultTransportAction extends
    AbstractSingleStreamResultTransportAction<RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastCheckpointMaintainWorker, ForecastCacheBuffer, ForecastPriorityCache, ForecastCacheProvider, ForecastResult, RCFCasterResult, ForecastColdStart, ForecastModelManager, ForecastPriorityCache, ForecastSaveResultStrategy, ForecastColdStartWorker, ForecastRealTimeInferencer, ForecastCheckpointReadWorker, ForecastResultWriteRequest> {

    private static final Logger LOG = LogManager.getLogger(ForecastSingleStreamResultTransportAction.class);

    @Inject
    public ForecastSingleStreamResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CircuitBreakerService circuitBreakerService,
        ForecastCacheProvider cache,
        NodeStateManager stateManager,
        ForecastCheckpointReadWorker checkpointReadQueue,
        ForecastRealTimeInferencer inferencer,
        ThreadPool threadPool
    ) {
        super(
            transportService,
            actionFilters,
            circuitBreakerService,
            cache,
            stateManager,
            checkpointReadQueue,
            ForecastSingleStreamResultAction.NAME,
            AnalysisType.FORECAST,
            inferencer,
            threadPool,
            TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME
        );
    }

    @Override
    public ForecastResultWriteRequest createResultWriteRequest(Config config, ForecastResult result) {
        return new ForecastResultWriteRequest(
            System.currentTimeMillis() + config.getIntervalInMilliseconds(),
            config.getId(),
            RequestPriority.MEDIUM,
            result,
            config.getCustomResultIndexOrAlias()
        );
    }
}
