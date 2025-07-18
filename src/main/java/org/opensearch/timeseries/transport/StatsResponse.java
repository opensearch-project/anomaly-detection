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
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.model.Mergeable;

/**
 * StatsResponse contains logic to merge the node stats and cluster stats together and return them to user
 */
public class StatsResponse implements ToXContentObject, Mergeable {
    private StatsNodesResponse statsNodesResponse;
    private Map<String, Object> clusterStats;

    /**
     * Get cluster stats
     *
     * @return Map of cluster stats
     */
    public Map<String, Object> getClusterStats() {
        return clusterStats;
    }

    /**
     * Set cluster stats
     *
     * @param clusterStats Map of cluster stats
     */
    public void setClusterStats(Map<String, Object> clusterStats) {
        this.clusterStats = clusterStats;
    }

    /**
     * Get cluster stats
     *
     * @return StatsNodesResponse
     */
    public StatsNodesResponse getStatsNodesResponse() {
        return statsNodesResponse;
    }

    /**
     * Sets statsNodesResponse
     *
     * @param statsNodesResponse Stats Response from Nodes
     */
    public void setStatsNodesResponse(StatsNodesResponse statsNodesResponse) {
        this.statsNodesResponse = statsNodesResponse;
    }

    /**
     * Convert StatsResponse to XContent
     *
     * @param builder XContentBuilder
     * @return XContentBuilder
     * @throws IOException thrown on invalid input
     */
    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    public StatsResponse() {}

    public StatsResponse(StreamInput in) throws IOException {
        statsNodesResponse = new StatsNodesResponse(in);
        clusterStats = in.readMap();
    }

    public void writeTo(StreamOutput out) throws IOException {
        statsNodesResponse.writeTo(out);
        out.writeMap(clusterStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        for (Map.Entry<String, Object> clusterStat : clusterStats.entrySet()) {
            builder.field(clusterStat.getKey(), clusterStat.getValue());
        }
        statsNodesResponse.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        return xContentBuilder.endObject();
    }

    @Override
    public void merge(Mergeable other) {
        if (this == other || other == null || getClass() != other.getClass()) {
            return;
        }

        StatsResponse otherResponse = (StatsResponse) other;

        if (otherResponse.statsNodesResponse != null) {
            this.statsNodesResponse = otherResponse.statsNodesResponse;
        }

        if (otherResponse.clusterStats != null) {
            this.clusterStats = otherResponse.clusterStats;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        StatsResponse other = (StatsResponse) obj;
        return new EqualsBuilder().append(statsNodesResponse, other.statsNodesResponse).append(clusterStats, other.clusterStats).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(statsNodesResponse).append(clusterStats).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("statsNodesResponse", statsNodesResponse).append("clusterStats", clusterStats).toString();
    }
}
