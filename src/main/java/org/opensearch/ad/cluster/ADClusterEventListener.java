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

package org.opensearch.ad.cluster;

import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNodes.Delta;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.gateway.GatewayService;

public class ADClusterEventListener implements ClusterStateListener {
    private static final Logger LOG = LogManager.getLogger(ADClusterEventListener.class);
    static final String NOT_RECOVERED_MSG = "Cluster is not recovered yet.";
    static final String IN_PROGRESS_MSG = "Cluster state change in progress, return.";
    static final String NODE_CHANGED_MSG = "Cluster node changed";

    private final Semaphore inProgress;
    private HashRing hashRing;
    private final ClusterService clusterService;

    @Inject
    public ADClusterEventListener(ClusterService clusterService, HashRing hashRing) {
        this.clusterService = clusterService;
        this.clusterService.addListener(this);
        this.hashRing = hashRing;
        this.inProgress = new Semaphore(1);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            LOG.info(NOT_RECOVERED_MSG);
            return;
        }

        if (!inProgress.tryAcquire()) {
            LOG.info(IN_PROGRESS_MSG);
            return;
        }

        try {
            // Init AD version hash ring as early as possible. Some test case may fail as AD
            // version hash ring not initialized when test run.
            if (!hashRing.isHashRingInited()) {
                hashRing
                    .buildCircles(
                        ActionListener
                            .wrap(
                                r -> LOG.info("Init AD version hash ring successfully"),
                                e -> LOG.error("Failed to init AD version hash ring")
                            )
                    );
            }
            Delta delta = event.nodesDelta();

            if (delta.removed() || delta.added()) {
                LOG.info(NODE_CHANGED_MSG + ", node removed: {}, node added: {}", delta.removed(), delta.added());
                hashRing.addNodeChangeEvent();
                hashRing.buildCircles(delta, ActionListener.runAfter(ActionListener.wrap(hasRingBuildDone -> {
                    LOG.info("Hash ring build result: {}", hasRingBuildDone);
                }, e -> { LOG.error("Failed updating AD version hash ring", e); }), () -> inProgress.release()));
            } else {
                inProgress.release();
            }
        } catch (Exception ex) {
            // One possible exception is OpenSearchTimeoutException thrown when we fail
            // to put checkpoint when ModelManager stops model.
            LOG.error("Cluster state change handler has issue(s)", ex);
            inProgress.release();
        }
    }
}
