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
import java.util.List;

import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

// TODO: https://github.com/opensearch-project/opensearch-sdk-java/issues/683 (multi node support needed for extensions.
//  Previously, the class used to extend TransportNodesAction by which request is sent to multiple nodes.
//  For extensions as of now we only have one node support. In order to test multinode feature we need to add multinode support equivalent for SDK )
public class ADTaskProfileTransportAction extends TransportAction<ADTaskProfileRequest, ADTaskProfileResponse> {
    // TransportNodesAction<ADTaskProfileRequest, ADTaskProfileResponse, ADTaskProfileNodeRequest, ADTaskProfileNodeResponse> {

    private ADTaskManager adTaskManager;

    /* MultiNode support https://github.com/opensearch-project/opensearch-sdk-java/issues/200 */
    // private HashRing hashRing;

    private final SDKClusterService sdkClusterService;

    @Inject
    public ADTaskProfileTransportAction(
        SDKClusterService sdkClusterService,
        ActionFilters actionFilters,
        ADTaskManager adTaskManager,
        /* MultiNode support https://github.com/opensearch-project/opensearch-sdk-java/issues/200 */
        // HashRing hashRing,
        TaskManager taskManager
    ) {
        super(ADTaskProfileAction.NAME, actionFilters, taskManager);
        this.adTaskManager = adTaskManager;
        /* MultiNode support https://github.com/opensearch-project/opensearch-sdk-java/issues/200 */
        // this.hashRing = hashRing;
        this.sdkClusterService = sdkClusterService;
    }

    protected ADTaskProfileResponse newResponse(
        ADTaskProfileRequest request,
        List<ADTaskProfileNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ADTaskProfileResponse(sdkClusterService.state().getClusterName(), responses, failures);
    }

    protected ADTaskProfileNodeRequest newNodeRequest(ADTaskProfileRequest request) {
        return new ADTaskProfileNodeRequest(request);
    }

    protected ADTaskProfileNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ADTaskProfileNodeResponse(in);
    }

    @Override
    protected void doExecute(Task task, ADTaskProfileRequest request, ActionListener<ADTaskProfileResponse> actionListener) {
        /* @anomaly.detection Commented until we have extension support for hashring : https://github.com/opensearch-project/opensearch-sdk-java/issues/200
        String remoteNodeId = request.getParentTask().getNodeId();
        Version remoteAdVersion = hashRing.getAdVersion(remoteNodeId);
         */
        Version remoteAdVersion = Version.CURRENT;
        ADTaskProfile adTaskProfile = adTaskManager.getLocalADTaskProfilesByDetectorId(request.getDetectorId());
        actionListener
            .onResponse(
                newResponse(
                    request,
                    new ArrayList<>(List.of(new ADTaskProfileNodeResponse(sdkClusterService.localNode(), adTaskProfile, remoteAdVersion))),
                    new ArrayList<>()
                )
            );
    }
}
