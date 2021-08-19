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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.InternalStatNames;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 *  ADStatsNodesTransportAction contains the logic to extract the stats from the nodes
 */
public class ADStatsNodesTransportAction extends
    TransportNodesAction<ADStatsRequest, ADStatsNodesResponse, ADStatsNodeRequest, ADStatsNodeResponse> {

    private ADStats adStats;
    private final JvmService jvmService;
    private final ADTaskManager adTaskManager;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param adStats ADStats object
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
        super(
            ADStatsNodesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ADStatsRequest::new,
            ADStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ADStatsNodeResponse.class
        );
        this.adStats = adStats;
        this.jvmService = jvmService;
        this.adTaskManager = adTaskManager;
    }

    @Override
    protected ADStatsNodesResponse newResponse(
        ADStatsRequest request,
        List<ADStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ADStatsNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ADStatsNodeRequest newNodeRequest(ADStatsRequest request) {
        return new ADStatsNodeRequest(request);
    }

    @Override
    protected ADStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ADStatsNodeResponse(in);
    }

    @Override
    protected ADStatsNodeResponse nodeOperation(ADStatsNodeRequest request) {
        return createADStatsNodeResponse(request.getADStatsRequest());
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

        return new ADStatsNodeResponse(clusterService.localNode(), statValues);
    }
}
