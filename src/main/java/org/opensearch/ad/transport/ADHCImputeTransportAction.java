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

/**
 * This class manages the broadcasting mechanism and entity data processing for
 * the HC detector. The system broadcasts a message after processing all records
 * in each interval to ensure that each node examines its hot models in memory
 * and determines which entity models have not received data during the current interval.
 *
 * "Hot" entities refer to those models actively loaded in memory, as opposed to
 * "cold" models, which are not loaded and remain in storage due to limited memory resources.
 *
 * Upon receiving the broadcast message, each node checks whether each hot entity
 * has received new data. If a hot entity has not received any data, the system
 * assigns a NaN value to that entity. This NaN value signals to the model that no
 * data was received, prompting it to impute the missing value based on previous data,
 * rather than using current interval data.
 *
 * The system determines which node manages which entities based on memory availability.
 * The coordinating node does not immediately know which entities are hot or cold;
 * it learns this during the pagination process. Hot entities are those that have
 * recently received data and are actively maintained in memory, while cold entities
 * remain in storage and are processed only if time permits within the interval.
 *
 * For cold entities whose models are not loaded in memory, the system does not
 * produce an anomaly result for that interval due to insufficient time or resources
 * to process them. This is particularly relevant in scenarios with short intervals,
 * such as one minute, where an underscaled cluster may cause processing delays
 * that prevent timely anomaly detection for some entities.
 */
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
