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
import java.util.List;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class ADTaskProfileTransportAction extends
    TransportNodesAction<ADTaskProfileRequest, ADTaskProfileResponse, ADTaskProfileNodeRequest, ADTaskProfileNodeResponse> {

    private ADTaskManager adTaskManager;
    private HashRing hashRing;

    @Inject
    public ADTaskProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADTaskManager adTaskManager,
        HashRing hashRing
    ) {
        super(
            ADTaskProfileAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ADTaskProfileRequest::new,
            ADTaskProfileNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ADTaskProfileNodeResponse.class
        );
        this.adTaskManager = adTaskManager;
        this.hashRing = hashRing;
    }

    @Override
    protected ADTaskProfileResponse newResponse(
        ADTaskProfileRequest request,
        List<ADTaskProfileNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ADTaskProfileResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ADTaskProfileNodeRequest newNodeRequest(ADTaskProfileRequest request) {
        return new ADTaskProfileNodeRequest(request);
    }

    @Override
    protected ADTaskProfileNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ADTaskProfileNodeResponse(in);
    }

    @Override
    protected ADTaskProfileNodeResponse nodeOperation(ADTaskProfileNodeRequest request) {
        String remoteNodeId = request.getParentTask().getNodeId();
        Version remoteAdVersion = hashRing.getAdVersion(remoteNodeId);
        ADTaskProfile adTaskProfile = adTaskManager.getLocalADTaskProfilesByDetectorId(request.getId());
        return new ADTaskProfileNodeResponse(clusterService.localNode(), adTaskProfile, remoteAdVersion);
    }
}
