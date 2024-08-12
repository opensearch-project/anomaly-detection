/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.ml.ADRealTimeInferencer;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.util.ActionListenerExecutor;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ADHCImputeTransportAction extends
    TransportNodesAction<ADHCImputeRequest, ADHCImputeNodesResponse, ADHCImputeNodeRequest, ADHCImputeNodeResponse> {
    private static final Logger LOG = LogManager.getLogger(ADHCImputeTransportAction.class);

    private ADCacheProvider cache;
    private NodeStateManager nodeStateManager;
    private ADRealTimeInferencer adInferencer;

    @Inject
    public ADHCImputeTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADCacheProvider priorityCache,
        NodeStateManager nodeStateManager,
        ADRealTimeInferencer adInferencer
    ) {
        super(
            ADHCImputeAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ADHCImputeRequest::new,
            ADHCImputeNodeRequest::new,
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
            ADHCImputeNodeResponse.class
        );
        this.cache = priorityCache;
        this.nodeStateManager = nodeStateManager;
        this.adInferencer = adInferencer;
    }

    @Override
    protected ADHCImputeNodeRequest newNodeRequest(ADHCImputeRequest request) {
        return new ADHCImputeNodeRequest(request);
    }

    @Override
    protected ADHCImputeNodeResponse newNodeResponse(StreamInput response) throws IOException {
        return new ADHCImputeNodeResponse(response);
    }

    @Override
    protected ADHCImputeNodesResponse newResponse(
        ADHCImputeRequest request,
        List<ADHCImputeNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ADHCImputeNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ADHCImputeNodeResponse nodeOperation(ADHCImputeNodeRequest nodeRequest) {
        String configId = nodeRequest.getRequest().getConfigId();
        nodeStateManager.getConfig(configId, AnalysisType.AD, ActionListenerExecutor.wrap(configOptional -> {
            if (configOptional.isEmpty()) {
                LOG.warn(String.format(Locale.ROOT, "cannot find config %s", configId));
                return;
            }
            Config config = configOptional.get();
            long windowDelayMillis = ((IntervalTimeConfiguration) config.getWindowDelay()).toDuration().toMillis();
            int featureSize = config.getEnabledFeatureIds().size();
            long dataEndMillis = nodeRequest.getRequest().getDataEndMillis();
            long dataStartMillis = nodeRequest.getRequest().getDataStartMillis();
            long executionEndTime = dataEndMillis + windowDelayMillis;
            String taskId = nodeRequest.getRequest().getTaskId();
            for (ModelState<ThresholdedRandomCutForest> modelState : cache.get().getAllModels(configId)) {
                // execution end time (when job starts execution in this interval) >= last used time => the model state is updated in
                // previous intervals
                if (executionEndTime >= modelState.getLastUsedTime().toEpochMilli()) {
                    double[] nanArray = new double[featureSize];
                    Arrays.fill(nanArray, Double.NaN);
                    adInferencer
                        .process(
                            new Sample(nanArray, Instant.ofEpochMilli(dataStartMillis), Instant.ofEpochMilli(dataEndMillis)),
                            modelState,
                            config,
                            taskId
                        );
                }
            }
        }, e -> nodeStateManager.setException(configId, e), threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)));

        Optional<Exception> previousException = nodeStateManager.fetchExceptionAndClear(configId);

        if (previousException.isPresent()) {
            return new ADHCImputeNodeResponse(clusterService.localNode(), previousException.get());
        } else {
            return new ADHCImputeNodeResponse(clusterService.localNode(), null);
        }
    }

}
