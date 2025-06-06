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

package org.opensearch.timeseries.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.transport.CronAction;
import org.opensearch.timeseries.transport.CronRequest;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.Client;

public class HourlyCron implements Runnable {
    private static final Logger LOG = LogManager.getLogger(HourlyCron.class);
    public static final String SUCCEEDS_LOG_MSG = "Hourly maintenance succeeds";
    public static final String NODE_EXCEPTION_LOG_MSG = "Hourly maintenance of node has exception";
    public static final String EXCEPTION_LOG_MSG = "Hourly maintenance has exception.";
    private DiscoveryNodeFilterer nodeFilter;
    private Client client;

    public HourlyCron(Client client, DiscoveryNodeFilterer nodeFilter) {
        this.nodeFilter = nodeFilter;
        this.client = client;
    }

    @Override
    public void run() {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();

        // we also add the cancel query function here based on query text from the negative cache.

        // Length of detector id is 20. Here we create a random string as request id to get hash with
        // HashRing, then we can control some maintaining task to just run on one data node. Read
        // ADTaskManager#maintainRunningHistoricalTasks for more details.
        CronRequest modelDeleteRequest = new CronRequest(dataNodes);
        client.execute(CronAction.INSTANCE, modelDeleteRequest, ActionListener.wrap(response -> {
            if (response.hasFailures()) {
                for (FailedNodeException failedNodeException : response.failures()) {
                    LOG.warn(NODE_EXCEPTION_LOG_MSG, failedNodeException);
                }
            } else {
                LOG.info(SUCCEEDS_LOG_MSG);
            }
        }, exception -> { LOG.error(EXCEPTION_LOG_MSG, exception); }));
    }
}
