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
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastCheckpointMaintainWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointReadWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.forecast.ratelimit.ForecastResultWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastSaveResultStrategy;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.transport.handler.ForecastIndexMemoryPressureAwareResultHandler;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.AbstractSingleStreamResultTransportAction;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastSingleStreamResultTransportAction extends
    AbstractSingleStreamResultTransportAction<RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastCheckpointMaintainWorker, ForecastCacheBuffer, ForecastPriorityCache, ForecastCacheProvider, ForecastResult, RCFCasterResult, ForecastColdStart, ForecastModelManager, ForecastPriorityCache, ForecastSaveResultStrategy, ForecastColdStartWorker, ForecastCheckpointReadWorker, ForecastResultWriteRequest, ForecastResultBulkRequest, ForecastIndexMemoryPressureAwareResultHandler, ForecastResultWriteWorker> {

    private static final Logger LOG = LogManager.getLogger(ForecastSingleStreamResultTransportAction.class);

    @Inject
    public ForecastSingleStreamResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CircuitBreakerService circuitBreakerService,
        ForecastCacheProvider cache,
        NodeStateManager stateManager,
        ForecastCheckpointReadWorker checkpointReadQueue,
        ForecastModelManager modelManager,
        ForecastIndexManagement indexUtil,
        ForecastResultWriteWorker resultWriteQueue,
        ForecastStats stats,
        ForecastColdStartWorker forecastColdStartQueue
    ) {
        super(
            transportService,
            actionFilters,
            circuitBreakerService,
            cache,
            stateManager,
            checkpointReadQueue,
            modelManager,
            indexUtil,
            resultWriteQueue,
            stats,
            forecastColdStartQueue,
            ForecastSingleStreamResultAction.NAME,
            ForecastIndex.RESULT,
            AnalysisType.FORECAST,
            StatNames.FORECAST_MODEL_CORRUTPION_COUNT.getName()
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
