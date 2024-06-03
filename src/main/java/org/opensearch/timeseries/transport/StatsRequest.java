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

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * StatsRequest implements a request to obtain stats about the time series analytics plugin
 */
public class StatsRequest extends BaseNodesRequest<StatsRequest> {

    /**
     * Key indicating all stats should be retrieved
     */
    public static final String ALL_STATS_KEY = "_all";

    private Set<String> statsToBeRetrieved;

    public StatsRequest(StreamInput in) throws IOException {
        super(in);
        statsToBeRetrieved = in.readSet(StreamInput::readString);
    }

    /**
     * Constructor
     *
     * @param nodeIds nodeIds of nodes' stats to be retrieved
     */
    public StatsRequest(String... nodeIds) {
        super(nodeIds);
        statsToBeRetrieved = new HashSet<>();
    }

    /**
     * Constructor
     *
     * @param nodes nodes of nodes' stats to be retrieved
     */
    public StatsRequest(DiscoveryNode... nodes) {
        super(nodes);
        statsToBeRetrieved = new HashSet<>();
    }

    /**
     * Adds a stat to the set of stats to be retrieved
     *
     * @param stat name of the stat
     */
    public void addStat(String stat) {
        statsToBeRetrieved.add(stat);
    }

    /**
     * Add all stats to be retrieved
     *
     * @param statsToBeAdded set of stats to be retrieved
     */
    public void addAll(Set<String> statsToBeAdded) {
        statsToBeRetrieved.addAll(statsToBeAdded);
    }

    /**
     * Remove all stats from retrieval set
     */
    public void clear() {
        statsToBeRetrieved.clear();
    }

    /**
     * Get the set that tracks which stats should be retrieved
     *
     * @return the set that contains the stat names marked for retrieval
     */
    public Set<String> getStatsToBeRetrieved() {
        return statsToBeRetrieved;
    }

    public void readFrom(StreamInput in) throws IOException {
        statsToBeRetrieved = in.readSet(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(statsToBeRetrieved);
    }
}
