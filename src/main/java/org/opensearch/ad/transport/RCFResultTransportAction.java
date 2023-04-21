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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

public class RCFResultTransportAction extends TransportAction<RCFResultRequest, RCFResultResponse> {

    private static final Logger LOG = LogManager.getLogger(RCFResultTransportAction.class);
    private ExtensionsRunner extensionsRunner;
    private ModelManager manager;
    private ADCircuitBreakerService adCircuitBreakerService;

    @Inject
    public RCFResultTransportAction(
        ExtensionsRunner extensionsRunner,
        ActionFilters actionFilters,
        TaskManager taskManager,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService
    ) {
        super(RCFResultAction.NAME, actionFilters, taskManager);
        this.extensionsRunner = extensionsRunner;
        this.manager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
    }

    @Override
    protected void doExecute(Task task, RCFResultRequest request, ActionListener<RCFResultResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getAdID(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
            return;
        }
        /* @anomaly.detection Commented until we have extension support for hashring : https://github.com/opensearch-project/opensearch-sdk-java/issues/200 
        Optional<DiscoveryNode> remoteNode = hashRing.getNodeByAddress(request.remoteAddress());
        if (!remoteNode.isPresent()) {
            listener.onFailure(new ConnectException("Can't find remote node by address"));
            return;
        }
        String remoteNodeId = remoteNode.get().getId();
        Version remoteAdVersion = hashRing.getAdVersion(remoteNodeId);
        */
        Version adVersion = Version.CURRENT;

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
                                        adVersion,
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
