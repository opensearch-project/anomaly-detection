/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.util.ActionListenerExecutor;
import org.opensearch.timeseries.util.ModelUtil;
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
    private HashRing hashRing;

    @Inject
    public ADHCImputeTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADCacheProvider priorityCache,
        NodeStateManager nodeStateManager,
        ADRealTimeInferencer adInferencer,
        HashRing hashRing
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
        this.hashRing = hashRing;
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
        nodeStateManager.getConfig(configId, AnalysisType.AD, true, ActionListenerExecutor.wrap(configOptional -> {
            if (configOptional.isEmpty()) {
                LOG.warn(String.format(Locale.ROOT, "cannot find config %s", configId));
                return;
            }
            Config config = configOptional.get();
            long dataEndMillis = nodeRequest.getRequest().getDataEndMillis();
            long dataStartMillis = nodeRequest.getRequest().getDataStartMillis();
            String taskId = nodeRequest.getRequest().getTaskId();

            List<ModelState<ThresholdedRandomCutForest>> allModels = cache.get().getAllModels(configId);
            processImputeIteration(
                allModels.iterator(),
                config,
                config.getEnabledFeatureIds().size(),
                dataEndMillis,
                dataStartMillis,
                taskId
            );
        }, e -> nodeStateManager.setException(configId, e), threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)));

        Optional<Exception> previousException = nodeStateManager.fetchExceptionAndClear(configId);

        if (previousException.isPresent()) {
            return new ADHCImputeNodeResponse(clusterService.localNode(), previousException.get());
        } else {
            return new ADHCImputeNodeResponse(clusterService.localNode(), null);
        }
    }

    private void processImputeIteration(
        Iterator<ModelState<ThresholdedRandomCutForest>> modelStateIterator,
        Config config,
        int featureSize,
        long dataEndMillis,
        long dataStartMillis,
        String taskId
    ) {
        if (!modelStateIterator.hasNext()) {
            return;
        }
        ModelState<ThresholdedRandomCutForest> modelState = modelStateIterator.next();
        if (shouldProcessModelState(modelState, dataEndMillis, clusterService, hashRing)) {
            double[] nanArray = new double[featureSize];
            Arrays.fill(nanArray, Double.NaN);
            LOG
                .debug(
                    "Processing imputation - dataEndMillis: {}, dataStartMillis: {}, entity: {}",
                    dataEndMillis,
                    dataStartMillis,
                    modelState.getEntity().get().toString()
                );
            adInferencer
                .process(
                    new Sample(nanArray, Instant.ofEpochMilli(dataStartMillis), Instant.ofEpochMilli(dataEndMillis)),
                    modelState,
                    config,
                    taskId,
                    ActionListener.wrap(r -> {
                        processImputeIteration(modelStateIterator, config, featureSize, dataEndMillis, dataStartMillis, taskId);
                    }, e -> {
                        LOG.error("Failed to impute for model " + modelState.getModelId(), e);
                        processImputeIteration(modelStateIterator, config, featureSize, dataEndMillis, dataStartMillis, taskId);
                    })
                );
        } else {
            processImputeIteration(modelStateIterator, config, featureSize, dataEndMillis, dataStartMillis, taskId);
        }
    }

    /**
     * Determines whether the model state should be processed based on various conditions.
     *
     * Conditions checked:
     * - The model's last seen data end time is not the minimum Instant value. This means the model hasn't been initialized yet.
     * - The current data end time is greater than the model's last seen data end time,
     *   indicating that the model state was updated in previous intervals.
     * - The entity associated with the model state is present.
     * - The owning node for real-time processing of the entity, with the same local version, is present in the hash ring.
     * - The owning node for real-time processing matches the current local node.
     *
     * This method helps avoid processing model states that were already handled in previous intervals. The conditions
     * ensure that only the relevant model states are processed while accounting for scenarios where processing can occur
     * concurrently (e.g., during tests when multiple threads may operate quickly).
     *
     * @param modelState       The current state of the model.
     * @param dataEndTime      The data end time of current interval.
     * @param clusterService   The service providing information about the current cluster node.
     * @param hashRing         The hash ring used to determine the owning node for real-time processing of entities.
     * @return true if the model state should be processed; otherwise, false.
     */
    private boolean shouldProcessModelState(
        ModelState<ThresholdedRandomCutForest> modelState,
        long dataEndTime,
        ClusterService clusterService,
        HashRing hashRing
    ) {
        // Get the owning node for the entity in real-time processing from the hash ring
        Optional<DiscoveryNode> owningNode = modelState.getEntity().isPresent()
            ? hashRing.getOwningNodeWithSameLocalVersionForRealtime(modelState.getEntity().get().toString())
            : Optional.empty();

        // Check if the model state conditions are met for processing
        // We cannot use last used time as it will be updated whenever we update its priority in CacheBuffer.update when there is a
        // PriorityCache.get.
        long lastInputTimestampSecs = modelState.getModel().isPresent()
            ? ModelUtil.getLastInputTimestampSeconds(modelState.getModel().get())
            : 0;
        return lastInputTimestampSecs != 0
            // dataEndTime is in milliseconds, so we need to convert it to seconds
            && dataEndTime / 1000 > lastInputTimestampSecs
            && modelState.getEntity().isPresent()
            && owningNode.isPresent()
            && owningNode.get().getId().equals(clusterService.localNode().getId());
    }

}
