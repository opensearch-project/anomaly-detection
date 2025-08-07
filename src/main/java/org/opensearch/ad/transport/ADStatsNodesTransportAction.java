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

import java.util.Map;
import java.util.Set;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.stats.InternalStatNames;
import org.opensearch.timeseries.transport.BaseStatsNodesTransportAction;
import org.opensearch.timeseries.transport.StatsNodeResponse;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.transport.TransportService;

/**
 *  ADStatsNodesTransportAction contains the logic to extract the stats from the nodes
 */
public class ADStatsNodesTransportAction extends BaseStatsNodesTransportAction {

    private final JvmService jvmService;
    private final ADTaskManager adTaskManager;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param adStats TimeSeriesStats object
     * @param jvmService ES JVM Service
     * @param adTaskManager AD task manager
     */
    @Inject
    public ADStatsNodesTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADStats adStats,
        JvmService jvmService,
        ADTaskManager adTaskManager
    ) {
        super(threadPool, clusterService, transportService, actionFilters, adStats, ADStatsNodesAction.NAME);
        this.jvmService = jvmService;
        this.adTaskManager = adTaskManager;
    }

    @Override
    protected StatsNodeResponse createStatsNodeResponse(StatsRequest adStatsRequest) {
        Map<String, Object> statValues = super.createStatsNodeResponse(adStatsRequest).getStatsMap();
        Set<String> statsToBeRetrieved = adStatsRequest.getStatsToBeRetrieved();

        if (statsToBeRetrieved.contains(InternalStatNames.JVM_HEAP_USAGE.getName())) {
            long heapUsedPercent = jvmService.stats().getMem().getHeapUsedPercent();
            statValues.put(InternalStatNames.JVM_HEAP_USAGE.getName(), heapUsedPercent);
        }

        if (statsToBeRetrieved.contains(InternalStatNames.AD_USED_BATCH_TASK_SLOT_COUNT.getName())) {
            int usedTaskSlot = adTaskManager.getLocalAdUsedBatchTaskSlot();
            statValues.put(InternalStatNames.AD_USED_BATCH_TASK_SLOT_COUNT.getName(), usedTaskSlot);
        }

        if (statsToBeRetrieved.contains(InternalStatNames.AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName())) {
            int assignedBatchTaskSlot = adTaskManager.getLocalAdAssignedBatchTaskSlot();
            statValues.put(InternalStatNames.AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName(), assignedBatchTaskSlot);
        }

        return new StatsNodeResponse(clusterService.localNode(), statValues);
    }
}
