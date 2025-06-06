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
import org.opensearch.forecast.transport.ForecastRunOnceProfileNodeResponse;
import org.opensearch.forecast.transport.ForecastRunOnceProfileResponse;
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

        ForecastRunOnceProfileNodeResponse nodeResponseTrue = new ForecastRunOnceProfileNodeResponse(node, true, "");
        ForecastRunOnceProfileNodeResponse nodeResponseFalse = new ForecastRunOnceProfileNodeResponse(node, false, "");
        List<ForecastRunOnceProfileNodeResponse> nodes = List.of(nodeResponseTrue, nodeResponseFalse);
        List<FailedNodeException> failures = Collections.emptyList();
        ClusterName clusterName = new ClusterName("test-cluster");

        ForecastRunOnceProfileResponse response = new ForecastRunOnceProfileResponse(clusterName, nodes, failures);

        // Act: Serialize the response
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        // Deserialize the response
        StreamInput input = output.bytes().streamInput();
        ForecastRunOnceProfileResponse deserializedResponse = new ForecastRunOnceProfileResponse(input);

        // Assert
        assertEquals(clusterName, deserializedResponse.getClusterName());
        assertEquals(response.getNodes().size(), deserializedResponse.getNodes().size());
        assertEquals(response.failures().size(), deserializedResponse.failures().size());
        assertEquals(response.isAnswerTrue(), deserializedResponse.isAnswerTrue());
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

        ForecastRunOnceProfileNodeResponse nodeResponse = new ForecastRunOnceProfileNodeResponse(node, true, "");

        // Act: Serialize the node response
        BytesStreamOutput output = new BytesStreamOutput();
        nodeResponse.writeTo(output);

        // Deserialize the node response
        StreamInput input = output.bytes().streamInput();
        ForecastRunOnceProfileNodeResponse deserializedNodeResponse = new ForecastRunOnceProfileNodeResponse(input);

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

        ForecastRunOnceProfileNodeResponse nodeResponseTrue = new ForecastRunOnceProfileNodeResponse(node1, true, "");
        ForecastRunOnceProfileNodeResponse nodeResponseFalse = new ForecastRunOnceProfileNodeResponse(node2, false, "");
        List<ForecastRunOnceProfileNodeResponse> nodes = List.of(nodeResponseTrue, nodeResponseFalse);
        ClusterName clusterName = new ClusterName("test-cluster");

        // Act
        ForecastRunOnceProfileResponse response = new ForecastRunOnceProfileResponse(clusterName, nodes, Collections.emptyList());

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

        ForecastRunOnceProfileNodeResponse nodeResponse1 = new ForecastRunOnceProfileNodeResponse(node1, false, "");
        ForecastRunOnceProfileNodeResponse nodeResponse2 = new ForecastRunOnceProfileNodeResponse(node2, false, "");
        List<ForecastRunOnceProfileNodeResponse> nodes = List.of(nodeResponse1, nodeResponse2);
        ClusterName clusterName = new ClusterName("test-cluster");

        // Act
        ForecastRunOnceProfileResponse response = new ForecastRunOnceProfileResponse(clusterName, nodes, Collections.emptyList());

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

        ForecastRunOnceProfileNodeResponse nodeResponse = new ForecastRunOnceProfileNodeResponse(node, true, "");
        List<ForecastRunOnceProfileNodeResponse> nodes = Collections.singletonList(nodeResponse);
        ClusterName clusterName = new ClusterName("test-cluster");
        ForecastRunOnceProfileResponse response = new ForecastRunOnceProfileResponse(clusterName, nodes, Collections.emptyList());

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
