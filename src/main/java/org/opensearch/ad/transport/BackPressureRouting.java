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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Data structure to keep track of a node's unresponsive history: a node does not reply for a
 * certain consecutive times gets muted for some time.
 */
public class BackPressureRouting {
    private static final Logger LOG = LogManager.getLogger(BackPressureRouting.class);
    private final String nodeId;
    private final Clock clock;
    private final int maxRetryForUnresponsiveNode;
    private final TimeValue mutePeriod;
    private AtomicInteger backpressureCounter;
    private long lastMuteTime;

    public BackPressureRouting(String nodeId, Clock clock, Settings settings) {
        this.nodeId = nodeId;
        this.clock = clock;
        this.backpressureCounter = new AtomicInteger(0);
        this.maxRetryForUnresponsiveNode = MAX_RETRY_FOR_UNRESPONSIVE_NODE.get(settings);
        this.mutePeriod = BACKOFF_MINUTES.get(settings);
        this.lastMuteTime = 0;
    }

    /**
     * The caller of this method does not have to keep track of when to start
     * muting. This method would mute by itself when we have accumulated enough
     * unresponsive calls.
     */
    public void addPressure() {
        int currentRetry = backpressureCounter.incrementAndGet();
        LOG.info("{} has been unresponsive for {} times", nodeId, currentRetry);
        if (currentRetry > this.maxRetryForUnresponsiveNode) {
            mute();
        }
    }

    /**
     * We call this method to decide if a node is muted or not. If yes, we can send
     * requests to the node; if not, skip sending requests.
     *
     * @return whether this node is muted or not
     */
    public boolean isMuted() {
        if (clock.millis() - lastMuteTime <= mutePeriod.getMillis()) {
            return true;
        }
        return false;
    }

    private void mute() {
        lastMuteTime = clock.millis();
    }
}
