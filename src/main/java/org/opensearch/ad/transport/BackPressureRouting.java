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

import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;

/**
 * Data structure to keep track of a node's unresponsive history: a node does not reply for a
 * certain consecutive times gets muted for some time.
 */
public class BackPressureRouting {
    private static final Logger LOG = LogManager.getLogger(BackPressureRouting.class);
    private final String nodeId;
    private final Clock clock;
    private int maxRetryForUnresponsiveNode;
    private TimeValue mutePeriod;
    private AtomicInteger backpressureCounter;
    private long lastMuteTime;

    public BackPressureRouting(String nodeId, Clock clock, int maxRetryForUnresponsiveNode, TimeValue mutePeriod) {
        this.nodeId = nodeId;
        this.clock = clock;
        this.backpressureCounter = new AtomicInteger(0);
        this.maxRetryForUnresponsiveNode = maxRetryForUnresponsiveNode;
        this.mutePeriod = mutePeriod;
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

    public int getMaxRetryForUnresponsiveNode() {
        return maxRetryForUnresponsiveNode;
    }

    /**
     * Setter for maxRetryForUnresponsiveNode
     *
     * It is up to the client to make the method thread safe.
     *
     * @param maxRetryForUnresponsiveNode the max retries before muting a node.
     */
    public void setMaxRetryForUnresponsiveNode(int maxRetryForUnresponsiveNode) {
        this.maxRetryForUnresponsiveNode = maxRetryForUnresponsiveNode;
    }

    public TimeValue getMutePeriod() {
        return mutePeriod;
    }

    public void setMutePeriod(TimeValue mutePeriod) {
        this.mutePeriod = mutePeriod;
    }
}
