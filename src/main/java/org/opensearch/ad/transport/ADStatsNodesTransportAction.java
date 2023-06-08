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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.InternalStatNames;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

// TODO: https://github.com/opensearch-project/opensearch-sdk-java/issues/683 (multi node support needed for extensions.
//  Previously, the class used to extend TransportNodesAction by which request is sent to multiple nodes.
//  For extensions as of now we only have one node support. In order to test multinode feature we need to add multinode support equivalent for SDK )
/**
 *  ADStatsNodesTransportAction contains the logic to extract the stats from the nodes
 */
public class ADStatsNodesTransportAction extends TransportAction<ADStatsRequest, ADStatsNodesResponse> {
    private ADStats adStats;
    private final JvmService jvmService;
    private final ADTaskManager adTaskManager;

    private final SDKClusterService sdkClusterService;
    private ExtensionsRunner extensionsRunner;

    /**
     * Constructor
     *
     * @param sdkClusterService SDK cluster Service
     * @param actionFilters Action Filters
     * @param adStats ADStats object
     * @param jvmService ES JVM Service
     * @param adTaskManager AD task manager
     * @param taskManager Task manager
     * @param extensionsRunner extensions runner
     */
    @Inject
    public ADStatsNodesTransportAction(
        SDKClusterService sdkClusterService,
        ActionFilters actionFilters,
        ADStats adStats,
        JvmService jvmService,
        ADTaskManager adTaskManager,
        TaskManager taskManager,
        ExtensionsRunner extensionsRunner
    ) {
        super(ADStatsNodesAction.NAME, actionFilters, taskManager);
        this.adStats = adStats;
        this.jvmService = jvmService;
        this.adTaskManager = adTaskManager;
        this.sdkClusterService = sdkClusterService;
        this.extensionsRunner = extensionsRunner;
    }

    protected ADStatsNodesResponse newResponse(
        ADStatsRequest request,
        List<ADStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ADStatsNodesResponse(
            new ClusterName(extensionsRunner.getEnvironmentSettings().get("cluster.name")),
            responses,
            failures
        );
    }

    protected ADStatsNodeRequest newNodeRequest(ADStatsRequest request) {
        return new ADStatsNodeRequest(request);
    }

    protected ADStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ADStatsNodeResponse(in);
    }

    protected void doExecute(Task task, ADStatsRequest request, ActionListener<ADStatsNodesResponse> actionListener) {
        actionListener.onResponse(newResponse(request, new ArrayList<>(List.of(createADStatsNodeResponse(request))), new ArrayList<>()));
    }

    private ADStatsNodeResponse createADStatsNodeResponse(ADStatsRequest adStatsRequest) {
        Map<String, Object> statValues = new HashMap<>();
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

        for (String statName : adStats.getNodeStats().keySet()) {
            if (statsToBeRetrieved.contains(statName)) {
                statValues.put(statName, adStats.getStats().get(statName).getValue());
            }
        }

        return new ADStatsNodeResponse(sdkClusterService.localNode(), statValues);
    }
}
