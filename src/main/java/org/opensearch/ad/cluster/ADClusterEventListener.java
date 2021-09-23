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

package org.opensearch.ad.cluster;

import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes.Delta;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.gateway.GatewayService;

public class ADClusterEventListener implements ClusterStateListener {
    private static final Logger LOG = LogManager.getLogger(ADClusterEventListener.class);
    static final String NODE_NOT_APPLIED_MSG = "AD does not use master or ultrawarm nodes";
    static final String NOT_RECOVERED_MSG = "CLuster is not recovered yet.";
    static final String IN_PROGRESS_MSG = "Cluster state change in progress, return.";
    static final String REMOVE_MODEL_MSG = "Remove model";
    static final String NODE_ADDED_MSG = "Data node added ";
    static final String NODE_REMOVED_MSG = "Data node removed ";

    private final Semaphore inProgress;
    private HashRing hashRing;
    private final ClusterService clusterService;
    private final DiscoveryNodeFilterer nodeFilter;

    @Inject
    public ADClusterEventListener(ClusterService clusterService, HashRing hashRing, DiscoveryNodeFilterer nodeFilter) {
        this.clusterService = clusterService;
        this.clusterService.addListener(this);
        this.hashRing = hashRing;
        this.inProgress = new Semaphore(1);
        this.nodeFilter = nodeFilter;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        if (!nodeFilter.isEligibleNode(event.state().nodes().getLocalNode())) {
            LOG.debug(NODE_NOT_APPLIED_MSG);
            return;
        }

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

            // Check whether it was a data node that was removed
            boolean dataNodeRemoved = false;
            for (DiscoveryNode removedNode : delta.removedNodes()) {
                if (nodeFilter.isEligibleNode(removedNode)) {
                    LOG.info(NODE_REMOVED_MSG + " {}", removedNode.getId());
                    dataNodeRemoved = true;
                    break;
                }
            }

            // Check whether it was a data node that was added
            boolean dataNodeAdded = false;
            for (DiscoveryNode addedNode : delta.addedNodes()) {
                if (nodeFilter.isEligibleNode(addedNode)) {
                    LOG.info(NODE_ADDED_MSG + " {}", addedNode.getId());
                    dataNodeAdded = true;
                    break;
                }
            }

            if (dataNodeAdded || dataNodeRemoved) {
                hashRing.addNodeChangeEvent();
                hashRing
                    .buildCircles(
                        delta,
                        ActionListener
                            .runAfter(
                                ActionListener
                                    .wrap(
                                        hasRingBuildDone -> { LOG.info("Hash ring build result: {}", hasRingBuildDone); },
                                        e -> { LOG.error("Failed updating AD version hash ring", e); }
                                    ),
                                () -> inProgress.release()
                            )
                    );
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
