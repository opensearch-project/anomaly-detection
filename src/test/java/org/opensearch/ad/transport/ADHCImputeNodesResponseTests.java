/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ADHCImputeNodesResponseTests extends OpenSearchTestCase {

    public void testADHCImputeNodesResponseSerialization() throws IOException {
        // Arrange
        DiscoveryNode node = new DiscoveryNode(
            "nodeId",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        Exception previousException = new Exception("Test exception message");

        ADHCImputeNodeResponse nodeResponse = new ADHCImputeNodeResponse(node, previousException);
        List<ADHCImputeNodeResponse> nodes = Collections.singletonList(nodeResponse);
        List<FailedNodeException> failures = Collections.emptyList();
        ClusterName clusterName = new ClusterName("test-cluster");

        ADHCImputeNodesResponse response = new ADHCImputeNodesResponse(clusterName, nodes, failures);

        // Act: Serialize the response
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        // Deserialize the response
        StreamInput input = output.bytes().streamInput();
        ADHCImputeNodesResponse deserializedResponse = new ADHCImputeNodesResponse(input);

        // Assert
        assertEquals(clusterName, deserializedResponse.getClusterName());
        assertEquals(response.getNodes().size(), deserializedResponse.getNodes().size());
        assertEquals(response.failures().size(), deserializedResponse.failures().size());

        // Check the node response
        ADHCImputeNodeResponse deserializedNodeResponse = deserializedResponse.getNodes().get(0);
        assertEquals(node, deserializedNodeResponse.getNode());
        assertNotNull(deserializedNodeResponse.getPreviousException());
        assertEquals("exception: " + previousException.getMessage(), deserializedNodeResponse.getPreviousException().getMessage());
    }

    public void testReadNodesFromAndWriteNodesTo() throws IOException {
        // Arrange
        DiscoveryNode node = new DiscoveryNode(
            "nodeId",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        Exception previousException = new Exception("Test exception message");

        ADHCImputeNodeResponse nodeResponse = new ADHCImputeNodeResponse(node, previousException);
        List<ADHCImputeNodeResponse> nodes = Collections.singletonList(nodeResponse);
        ClusterName clusterName = new ClusterName("test-cluster");
        ADHCImputeNodesResponse response = new ADHCImputeNodesResponse(clusterName, nodes, Collections.emptyList());

        // Act: Write nodes to output
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeNodesTo(output, nodes);

        // Read nodes from input
        StreamInput input = output.bytes().streamInput();
        List<ADHCImputeNodeResponse> readNodes = response.readNodesFrom(input);

        // Assert
        assertEquals(nodes.size(), readNodes.size());
        ADHCImputeNodeResponse readNodeResponse = readNodes.get(0);
        assertEquals(node, readNodeResponse.getNode());
        assertNotNull(readNodeResponse.getPreviousException());
        assertEquals("exception: " + previousException.getMessage(), readNodeResponse.getPreviousException().getMessage());
    }

    public void testADHCImputeNodeResponseSerialization() throws IOException {
        // Arrange
        DiscoveryNode node = new DiscoveryNode(
            "nodeId",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        Exception previousException = new Exception("Test exception message");

        ADHCImputeNodeResponse nodeResponse = new ADHCImputeNodeResponse(node, previousException);

        // Act: Serialize the node response
        BytesStreamOutput output = new BytesStreamOutput();
        nodeResponse.writeTo(output);

        // Deserialize the node response
        StreamInput input = output.bytes().streamInput();
        ADHCImputeNodeResponse deserializedNodeResponse = new ADHCImputeNodeResponse(input);

        // Assert
        assertEquals(node, deserializedNodeResponse.getNode());
        assertNotNull(deserializedNodeResponse.getPreviousException());
        assertEquals("exception: " + previousException.getMessage(), deserializedNodeResponse.getPreviousException().getMessage());
    }

    public void testNoExceptionSerialization() throws IOException {
        // Arrange
        DiscoveryNode node = new DiscoveryNode(
            "nodeId",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        ADHCImputeNodeResponse nodeResponse = new ADHCImputeNodeResponse(node, null);

        // Act: Serialize the node response
        BytesStreamOutput output = new BytesStreamOutput();
        nodeResponse.writeTo(output);

        // Deserialize the node response
        StreamInput input = output.bytes().streamInput();
        ADHCImputeNodeResponse deserializedNodeResponse = new ADHCImputeNodeResponse(input);

        // Assert
        assertEquals(node, deserializedNodeResponse.getNode());
        assertNull(deserializedNodeResponse.getPreviousException());
    }
}
