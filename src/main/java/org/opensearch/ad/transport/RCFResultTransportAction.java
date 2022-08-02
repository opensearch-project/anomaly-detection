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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class RCFResultTransportAction extends HandledTransportAction<RCFResultRequest, RCFResultResponse> {

    private static final Logger LOG = LogManager.getLogger(RCFResultTransportAction.class);
    private ModelManager manager;
    private ADCircuitBreakerService adCircuitBreakerService;
    private HashRing hashRing;
    private ADStats adStats;

    @Inject
    public RCFResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService,
        HashRing hashRing,
        ADStats adStats
    ) {
        super(RCFResultAction.NAME, transportService, actionFilters, RCFResultRequest::new);
        this.manager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.hashRing = hashRing;
        this.adStats = adStats;
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
                                        result.getRelevantAttribution(),
                                        result.getTotalUpdates(),
                                        result.getGrade(),
                                        remoteAdVersion,
                                        result.getRelativeIndex(),
                                        result.getPastValues(),
                                        result.getExpectedValuesList(),
                                        result.getLikelihoodOfValues(),
                                        result.getThreshold()
                                    )
                                ),
                            exception -> {
                                if (exception instanceof IllegalArgumentException) {
                                    // fail to score likely due to model corruption. Re-cold start to recover.
                                    LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", request.getAdID()), exception);
                                    adStats.getStat(StatNames.MODEL_CORRUTPION_COUNT.getName()).increment();
                                    manager
                                        .clear(
                                            request.getAdID(),
                                            ActionListener
                                                .wrap(
                                                    r -> LOG.info("Deleted model for [{}] with response [{}] ", request.getAdID(), r),
                                                    ex -> LOG.error("Fail to delete model for " + request.getAdID(), ex)
                                                )
                                        );
                                    listener.onFailure(exception);
                                } else {
                                    LOG.warn(exception);
                                    listener.onFailure(exception);
                                }
                            }
                        )
                );
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

    }

}
