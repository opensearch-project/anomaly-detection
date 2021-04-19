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

package com.amazon.opendistroforelasticsearch.ad.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;

import com.amazon.opendistroforelasticsearch.ad.transport.CronAction;
import com.amazon.opendistroforelasticsearch.ad.transport.CronRequest;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class HourlyCron implements Runnable {
    private static final Logger LOG = LogManager.getLogger(HourlyCron.class);
    static final String SUCCEEDS_LOG_MSG = "Hourly maintenance succeeds";
    static final String NODE_EXCEPTION_LOG_MSG = "Hourly maintenance of node has exception";
    static final String EXCEPTION_LOG_MSG = "Hourly maintenance has exception.";
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
