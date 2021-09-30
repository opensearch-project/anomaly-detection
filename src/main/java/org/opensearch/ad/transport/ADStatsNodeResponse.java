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

import java.io.IOException;
import java.util.Map;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

/**
 * ADStatsNodeResponse
 */
public class ADStatsNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private Map<String, Object> statsMap;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException throws an IO exception if the StreamInput cannot be read from
     */
    public ADStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.statsMap = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
    }

    /**
     * Constructor
     *
     * @param node node
     * @param statsToValues Mapping of stat name to value
     */
    public ADStatsNodeResponse(DiscoveryNode node, Map<String, Object> statsToValues) {
        super(node);
        this.statsMap = statsToValues;
    }

    /**
     * Creates a new ADStatsNodeResponse object and reads in the stats from an input stream
     *
     * @param in StreamInput to read from
     * @return ADStatsNodeResponse object corresponding to the input stream
     * @throws IOException throws an IO exception if the StreamInput cannot be read from
     */
    public static ADStatsNodeResponse readStats(StreamInput in) throws IOException {

        return new ADStatsNodeResponse(in);
    }

    /**
     * getStatsMap
     *
     * @return map of stats
     */
    public Map<String, Object> getStatsMap() {
        return statsMap;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(statsMap, StreamOutput::writeString, StreamOutput::writeGenericValue);
    }

    /**
     * Converts statsMap to xContent
     *
     * @param builder XContentBuilder
     * @param params Params
     * @return XContentBuilder
     * @throws IOException thrown by builder for invalid field
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (String stat : statsMap.keySet()) {
            builder.field(stat, statsMap.get(stat));
        }

        return builder;
    }
}
