/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

public class CrossClusterConfigUtilsTests extends OpenSearchTestCase {

    private Client mockClient;
    private String remoteClusterName;
    private String localClusterName;

    @Before
    public void setup() {
        // Initialize the mock clients
        mockClient = mock(NodeClient.class);
        localClusterName = "localCluster";
        remoteClusterName = "remoteCluster";
    }

    public void testGetClientForClusterLocalCluster() {
        Client result = CrossClusterConfigUtils.getClientForCluster(localClusterName, mockClient, localClusterName);
        assertEquals(mockClient, result);
        verify(mockClient, never()).getRemoteClusterClient(anyString());
    }

    public void testGetClientForClusterRemoteCluster() {
        Client mockClient = mock(NodeClient.class);
        CrossClusterConfigUtils.getClientForCluster(remoteClusterName, mockClient, localClusterName);

        // Verify that getRemoteClusterClient was called once with the correct cluster name
        verify(mockClient, times(1)).getRemoteClusterClient("remoteCluster");
        when(mockClient.getRemoteClusterClient(remoteClusterName)).thenReturn(mockClient);
    }

    public void testSeparateClusterIndexesRemoteCluster() {
        List<String> indexes = Arrays.asList("remoteCluster:index1", "index2", "remoteCluster2:index2");
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.getClusterName()).thenReturn(new ClusterName("localCluster"));

        HashMap<String, List<String>> result = CrossClusterConfigUtils.separateClusterIndexes(indexes, mockClusterService);

        assertEquals(3, result.size());
        assertEquals(Arrays.asList("index1"), result.get("remoteCluster"));
        assertEquals(Arrays.asList("index2"), result.get("localCluster"));
        assertEquals(Arrays.asList("index2"), result.get("remoteCluster2"));
    }

    public void testParseClusterAndIndexName_WithClusterAndIndex() {
        String input = "clusterA:index1";
        Pair<String, String> result = CrossClusterConfigUtils.parseClusterAndIndexName(input);
        assertEquals("clusterA", result.getKey());
        assertEquals("index1", result.getValue());
    }
}
