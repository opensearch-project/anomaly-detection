/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Need a separate test class as opposed to keeping it in BooleanResponseTests since
 * writeNodesTo and readNodesFrom are protected methods in class ForecastRunOnceProfileResponse.
 *
 */
public class ForecastBooleanResponseTests extends OpenSearchTestCase {
    public void testBooleanResponseReadNodesFromAndWriteNodesTo() throws IOException {
        // Arrange
        DiscoveryNode node1 = new DiscoveryNode(
            "nodeId1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "nodeId2",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        ForecastRunOnceProfileNodeResponse nodeResponse1 = new ForecastRunOnceProfileNodeResponse(node1, true, "");
        ForecastRunOnceProfileNodeResponse nodeResponse2 = new ForecastRunOnceProfileNodeResponse(node2, false, "");
        List<ForecastRunOnceProfileNodeResponse> nodes = List.of(nodeResponse1, nodeResponse2);
        ClusterName clusterName = new ClusterName("test-cluster");
        ForecastRunOnceProfileResponse response = new ForecastRunOnceProfileResponse(clusterName, nodes, Collections.emptyList());

        // Act: Write nodes to output
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeNodesTo(output, nodes);

        // Read nodes from input
        StreamInput input = output.bytes().streamInput();
        List<ForecastRunOnceProfileNodeResponse> readNodes = response.readNodesFrom(input);

        // Assert
        assertEquals(nodes.size(), readNodes.size());
        assertEquals(nodes.get(0).isAnswerTrue(), readNodes.get(0).isAnswerTrue());
        assertEquals(nodes.get(1).isAnswerTrue(), readNodes.get(1).isAnswerTrue());
    }
}
