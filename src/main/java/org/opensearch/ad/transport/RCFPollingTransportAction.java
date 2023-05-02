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
import java.util.Locale;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.SingleStreamModelIdMapper;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import com.google.inject.Inject;

/**
 * Transport action to get total rcf updates from hosted models or checkpoint
 *
 */
public class RCFPollingTransportAction extends TransportAction<RCFPollingRequest, RCFPollingResponse> {

    private static final Logger LOG = LogManager.getLogger(RCFPollingTransportAction.class);
    static final String NO_NODE_FOUND_MSG = "Cannot find model hosting node";
    static final String FAIL_TO_GET_RCF_UPDATE_MSG = "Cannot find hosted model or related checkpoint";

    private final TransportService transportService;
    private final ModelManager modelManager;
    // private final HashRing hashRing;
    private final TransportRequestOptions option;
    private final SDKClusterService sdkClusterService;

    @Inject
    public RCFPollingTransportAction(
        ActionFilters actionFilters,
        // Settings settings,
        ModelManager modelManager,
        // HashRing hashRing,
        TaskManager taskManager,
        SDKClusterService sdkClusterService,
        ExtensionsRunner extensionsRunner
    ) {
        super(RCFPollingAction.NAME, actionFilters, taskManager);
        this.modelManager = modelManager;
        // this.hashRing = hashRing;
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(extensionsRunner.getEnvironmentSettings()))
            .build();
        this.sdkClusterService = sdkClusterService;
        this.transportService = extensionsRunner.getSdkTransportService().getTransportService();
    }

    @Override
    protected void doExecute(Task task, RCFPollingRequest request, ActionListener<RCFPollingResponse> listener) {

        String adID = request.getAdID();

        String rcfModelID = SingleStreamModelIdMapper.getRcfModelId(adID, 0);

        /* Commenting the below piece of code as we do not have support for multinode
             https://github.com/opensearch-project/opensearch-sdk-java/issues/200
        * */
        // Optional<DiscoveryNode> rcfNode = hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(rcfModelID);

        Optional<DiscoveryNode> rcfNode = Optional.ofNullable(sdkClusterService.localNode());
        if (!rcfNode.isPresent()) {
            listener.onFailure(new AnomalyDetectionException(adID, NO_NODE_FOUND_MSG));
            return;
        }

        String rcfNodeId = rcfNode.get().getId();

        DiscoveryNode localNode = sdkClusterService.localNode();

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
        } else if (request.remoteAddress() == null) {
            // redirect if request comes from local host.
            // If a request comes from remote machine, it is already redirected.
            // One redirection should be enough.
            // We don't want a potential infinite loop due to any bug and thus give up.
            LOG.info("Sending RCF polling request to {} for model {}", rcfNodeId, rcfModelID);

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

        } else {
            LOG.error("Fail to poll rcf for model {} due to an unexpected bug.", rcfModelID);
            listener.onFailure(new AnomalyDetectionException(adID, NO_NODE_FOUND_MSG));
        }
    }
}
