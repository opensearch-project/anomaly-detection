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
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ADColdStartWorker;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.ad.ratelimit.ADResultWriteWorker;
import org.opensearch.ad.ratelimit.ADSaveResultStrategy;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.transport.handler.ADIndexMemoryPressureAwareResultHandler;
import org.opensearch.common.inject.Inject;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.AbstractSingleStreamResultTransportAction;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ADSingleStreamResultTransportAction extends
    AbstractSingleStreamResultTransportAction<ThresholdedRandomCutForest, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADCheckpointMaintainWorker, ADCacheBuffer, ADPriorityCache, ADCacheProvider, AnomalyResult, ThresholdingResult, ADColdStart, ADModelManager, ADPriorityCache, ADSaveResultStrategy, ADColdStartWorker, ADCheckpointReadWorker, ADResultWriteRequest, ADResultBulkRequest, ADIndexMemoryPressureAwareResultHandler, ADResultWriteWorker> {

    @Inject
    public ADSingleStreamResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        CircuitBreakerService circuitBreakerService,
        ADCacheProvider cache,
        NodeStateManager stateManager,
        ADCheckpointReadWorker checkpointReadQueue,
        ADModelManager modelManager,
        ADIndexManagement indexUtil,
        ADResultWriteWorker resultWriteQueue,
        ADStats stats,
        ADColdStartWorker adColdStartQueue
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
            adColdStartQueue,
            ADSingleStreamResultAction.NAME,
            ADIndex.RESULT,
            AnalysisType.AD,
            StatNames.AD_MODEL_CORRUTPION_COUNT.getName()
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
