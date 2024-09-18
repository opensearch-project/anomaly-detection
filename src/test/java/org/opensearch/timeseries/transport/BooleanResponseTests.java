/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class BooleanResponseTests extends OpenSearchTestCase {

    public void testBooleanResponseSerialization() throws IOException {
        // Arrange
        DiscoveryNode node = new DiscoveryNode(
            "nodeId",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        BooleanNodeResponse nodeResponseTrue = new BooleanNodeResponse(node, true);
        BooleanNodeResponse nodeResponseFalse = new BooleanNodeResponse(node, false);
        List<BooleanNodeResponse> nodes = List.of(nodeResponseTrue, nodeResponseFalse);
        List<FailedNodeException> failures = Collections.emptyList();
        ClusterName clusterName = new ClusterName("test-cluster");

        BooleanResponse response = new BooleanResponse(clusterName, nodes, failures);

        // Act: Serialize the response
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        // Deserialize the response
        StreamInput input = output.bytes().streamInput();
        BooleanResponse deserializedResponse = new BooleanResponse(input);

        // Assert
        assertEquals(clusterName, deserializedResponse.getClusterName());
        assertEquals(response.getNodes().size(), deserializedResponse.getNodes().size());
        assertEquals(response.failures().size(), deserializedResponse.failures().size());
        assertEquals(response.isAnswerTrue(), deserializedResponse.isAnswerTrue());
    }

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

        BooleanNodeResponse nodeResponse1 = new BooleanNodeResponse(node1, true);
        BooleanNodeResponse nodeResponse2 = new BooleanNodeResponse(node2, false);
        List<BooleanNodeResponse> nodes = List.of(nodeResponse1, nodeResponse2);
        ClusterName clusterName = new ClusterName("test-cluster");
        BooleanResponse response = new BooleanResponse(clusterName, nodes, Collections.emptyList());

        // Act: Write nodes to output
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeNodesTo(output, nodes);

        // Read nodes from input
        StreamInput input = output.bytes().streamInput();
        List<BooleanNodeResponse> readNodes = response.readNodesFrom(input);

        // Assert
        assertEquals(nodes.size(), readNodes.size());
        assertEquals(nodes.get(0).isAnswerTrue(), readNodes.get(0).isAnswerTrue());
        assertEquals(nodes.get(1).isAnswerTrue(), readNodes.get(1).isAnswerTrue());
    }

    public void testBooleanNodeResponseSerialization() throws IOException {
        // Arrange
        DiscoveryNode node = new DiscoveryNode(
            "nodeId",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        BooleanNodeResponse nodeResponse = new BooleanNodeResponse(node, true);

        // Act: Serialize the node response
        BytesStreamOutput output = new BytesStreamOutput();
        nodeResponse.writeTo(output);

        // Deserialize the node response
        StreamInput input = output.bytes().streamInput();
        BooleanNodeResponse deserializedNodeResponse = new BooleanNodeResponse(input);

        // Assert
        assertEquals(node, deserializedNodeResponse.getNode());
        assertEquals(nodeResponse.isAnswerTrue(), deserializedNodeResponse.isAnswerTrue());
    }

    public void testBooleanResponseAnswerAggregation() {
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

        BooleanNodeResponse nodeResponseTrue = new BooleanNodeResponse(node1, true);
        BooleanNodeResponse nodeResponseFalse = new BooleanNodeResponse(node2, false);
        List<BooleanNodeResponse> nodes = List.of(nodeResponseTrue, nodeResponseFalse);
        ClusterName clusterName = new ClusterName("test-cluster");

        // Act
        BooleanResponse response = new BooleanResponse(clusterName, nodes, Collections.emptyList());

        // Assert
        assertTrue(response.isAnswerTrue()); // Since at least one node responded true
    }

    public void testBooleanResponseAllFalse() {
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

        BooleanNodeResponse nodeResponse1 = new BooleanNodeResponse(node1, false);
        BooleanNodeResponse nodeResponse2 = new BooleanNodeResponse(node2, false);
        List<BooleanNodeResponse> nodes = List.of(nodeResponse1, nodeResponse2);
        ClusterName clusterName = new ClusterName("test-cluster");

        // Act
        BooleanResponse response = new BooleanResponse(clusterName, nodes, Collections.emptyList());

        // Assert
        assertFalse(response.isAnswerTrue()); // Since all nodes responded false
    }

    public void testToXContent() throws IOException {
        // Arrange
        DiscoveryNode node = new DiscoveryNode(
            "nodeId",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        BooleanNodeResponse nodeResponse = new BooleanNodeResponse(node, true);
        List<BooleanNodeResponse> nodes = Collections.singletonList(nodeResponse);
        ClusterName clusterName = new ClusterName("test-cluster");
        BooleanResponse response = new BooleanResponse(clusterName, nodes, Collections.emptyList());

        // Act
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String jsonString = builder.toString();

        // Assert
        assertTrue(jsonString.contains("\"answer\":true"));
    }
}
