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

import java.net.ConnectException;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class RCFResultTransportAction extends HandledTransportAction<RCFResultRequest, RCFResultResponse> {

    private static final Logger LOG = LogManager.getLogger(RCFResultTransportAction.class);
    private ModelManager manager;
    private ADCircuitBreakerService adCircuitBreakerService;
    private HashRing hashRing;

    @Inject
    public RCFResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService,
        HashRing hashRing
    ) {
        super(RCFResultAction.NAME, transportService, actionFilters, RCFResultRequest::new);
        this.manager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.hashRing = hashRing;
    }

    @Override
    protected void doExecute(Task task, RCFResultRequest request, ActionListener<RCFResultResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getAdID(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
            return;
        }
        Optional<DiscoveryNode> remoteNode = hashRing.getNodeByAddress(request.remoteAddress());
        if (!remoteNode.isPresent()) {
            listener.onFailure(new ConnectException("Can't find remote node by address"));
            return;
        }
        String remoteNodeId = remoteNode.get().getId();
        Version remoteAdVersion = hashRing.getAdVersion(remoteNodeId);

        try {
            LOG.info("Serve rcf request for {}", request.getModelID());
            manager
                .getTRcfResult(
                    request.getAdID(),
                    request.getModelID(),
                    request.getFeatures(),
                    ActionListener
                        .wrap(
                            result -> listener
                                .onResponse(
                                    new RCFResultResponse(
                                        result.getRcfScore(),
                                        result.getConfidence(),
                                        result.getForestSize(),
                                        result.getRelevantttribution(),
                                        result.getTotalUpdates(),
                                        result.getGrade(),
                                        remoteAdVersion,
                                        result.isStartOfAnomaly(),
                                        result.isInHighScoreRegion(),
                                        result.getRelativeIndex(),
                                        result.getPastValues(),
                                        result.getExpectedValuesList(),
                                        result.getLikelihoodOfValues(),
                                        result.getThreshold()
                                    )
                                ),
                            exception -> {
                                LOG.warn(exception);
                                listener.onFailure(exception);
                            }
                        )
                );
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

    }

}
