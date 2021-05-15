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
import java.util.Locale;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelPartitioner;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Transport action to get total rcf updates from hosted models or checkpoint
 *
 */
public class RCFPollingTransportAction extends HandledTransportAction<RCFPollingRequest, RCFPollingResponse> {

    private static final Logger LOG = LogManager.getLogger(RCFPollingTransportAction.class);
    static final String NO_NODE_FOUND_MSG = "Cannot find model hosting node";
    static final String FAIL_TO_GET_RCF_UPDATE_MSG = "Cannot find hosted model or related checkpoint";

    private final TransportService transportService;
    private final ModelManager modelManager;
    private final ModelPartitioner modelPartitioner;
    private final HashRing hashRing;
    private final TransportRequestOptions option;
    private final ClusterService clusterService;

    @Inject
    public RCFPollingTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        ModelManager modelManager,
        ModelPartitioner modelPartitioner,
        HashRing hashRing,
        ClusterService clusterService
    ) {
        super(RCFPollingAction.NAME, transportService, actionFilters, RCFPollingRequest::new);
        this.transportService = transportService;
        this.modelManager = modelManager;
        this.modelPartitioner = modelPartitioner;
        this.hashRing = hashRing;
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, RCFPollingRequest request, ActionListener<RCFPollingResponse> listener) {

        String adID = request.getAdID();

        String rcfModelID = modelPartitioner.getRcfModelId(adID, 0);

        Optional<DiscoveryNode> rcfNode = hashRing.getOwningNode(rcfModelID.toString());
        if (!rcfNode.isPresent()) {
            listener.onFailure(new AnomalyDetectionException(adID, NO_NODE_FOUND_MSG));
            return;
        }

        String rcfNodeId = rcfNode.get().getId();

        DiscoveryNode localNode = clusterService.localNode();

        if (localNode.getId().equals(rcfNodeId)) {
            modelManager
                .getTotalUpdates(
                    rcfModelID,
                    adID,
                    ActionListener
                        .wrap(
                            totalUpdates -> listener.onResponse(new RCFPollingResponse(totalUpdates)),
                            e -> listener.onFailure(new AnomalyDetectionException(adID, FAIL_TO_GET_RCF_UPDATE_MSG, e))
                        )
                );
        } else {
            // redirect
            LOG.debug("Sending RCF polling request to {} for model {}", rcfNodeId, rcfModelID);

            try {
                transportService
                    .sendRequest(rcfNode.get(), RCFPollingAction.NAME, request, option, new TransportResponseHandler<RCFPollingResponse>() {

                        @Override
                        public RCFPollingResponse read(StreamInput in) throws IOException {
                            return new RCFPollingResponse(in);
                        }

                        @Override
                        public void handleResponse(RCFPollingResponse response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            listener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                    });
            } catch (Exception e) {
                LOG.error(String.format(Locale.ROOT, "Fail to poll RCF models for {}", adID), e);
                listener.onFailure(new AnomalyDetectionException(adID, FAIL_TO_GET_RCF_UPDATE_MSG, e));
            }

        }
    }
}
