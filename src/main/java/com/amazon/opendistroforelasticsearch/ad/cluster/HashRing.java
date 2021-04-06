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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class HashRing {
    private static final Logger LOG = LogManager.getLogger(HashRing.class);
    static final String REBUILD_MSG = "Rebuild hash ring";
    // In case of frequent node join/leave, hash ring has a cooldown period say 5 minute.
    // Hash ring doesn't respond to more than 1 cluster membership changes within the
    // cool-down period.
    static final String COOLDOWN_MSG = "Hash ring doesn't respond to cluster state change within the cooldown period.";

    private final int VIRTUAL_NODE_COUNT = 100;
    private final DiscoveryNodeFilterer nodeFilter;
    private TreeMap<Integer, DiscoveryNode> circle;
    private Semaphore inProgress;
    // the UTC epoch milliseconds of the most recent successful update
    private long lastUpdate;
    private final TimeValue coolDownPeriod;
    private final Clock clock;
    private AtomicBoolean membershipChangeRequied;

    public HashRing(DiscoveryNodeFilterer nodeFilter, Clock clock, Settings settings) {
        this.circle = new TreeMap<Integer, DiscoveryNode>();
        this.nodeFilter = nodeFilter;
        this.inProgress = new Semaphore(1);
        this.clock = clock;
        this.coolDownPeriod = COOLDOWN_MINUTES.get(settings);
        this.lastUpdate = 0;
        this.membershipChangeRequied = new AtomicBoolean(false);
    }

    /**
     * Rebuilds the hash ring when cluster membership change is required.
     * The build method skips a rebuilding if it has already rebuilt the hash ring within the
     *  cooldown period or the rebuilding is already in progress.
     * @return whether hash ring is rebuilt or not.
     */
    public boolean build() {
        // Check this conjunct first since most of time this conjunct evaluates
        // to false and we can skip of the following checks.
        // Hash ring can be empty because we cannot build the ring in constructor. The constructor
        // is called when the plugin is being loaded. At that time, cluster state is empty.
        if (!membershipChangeRequied.get() && !circle.isEmpty()) {
            return false;
        }

        // Check cooldown period
        if (clock.millis() - lastUpdate <= coolDownPeriod.getMillis()) {
            LOG.debug(COOLDOWN_MSG);
            return false;
        }

        // When the condition check passes, we start hash ring rebuilding.
        if (!inProgress.tryAcquire()) {
            LOG.info("Hash ring change in progress, return.");
            return false;
        }

        LOG.info(REBUILD_MSG);
        TreeMap<Integer, DiscoveryNode> newCircle = new TreeMap<>();

        try {
            for (DiscoveryNode curNode : nodeFilter.getEligibleDataNodes()) {
                for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                    newCircle.put(Murmur3HashFunction.hash(curNode.getId() + i), curNode);
                }
            }
            circle = newCircle;
            lastUpdate = clock.millis();
            membershipChangeRequied.set(false);
        } catch (Exception ex) {
            LOG.error("Hash ring cannot be rebuilt", ex);
            return false;
        } finally {
            inProgress.release();
        }
        return true;
    }

    /**
     * Compute the owning node of modelID using consistent hashing
     * @param modelId example: http-latency-rcf-1
     * @return the owning node of the modeID
     */
    public Optional<DiscoveryNode> getOwningNode(String modelId) {
        build();

        int modelHash = Murmur3HashFunction.hash(modelId);
        Map.Entry<Integer, DiscoveryNode> entry = circle.higherEntry(modelHash);

        // The method can return an empty Optional. Say two concurrent getOwningNode requests to
        // the hash ring before it's been built. The first one starts building it,
        // turning on inProgress. The second one returns from build and continues on to
        // the rest of hashing and look up while the ring is still being built and thus empty.
        // The second getOwningNode request returns an empty Optional in this case.
        return Optional.ofNullable(Optional.ofNullable(entry).orElse(circle.firstEntry())).map(x -> x.getValue());
    }

    public void recordMembershipChange() {
        membershipChangeRequied.set(true);
    }
}
