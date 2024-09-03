package org.opensearch.timeseries.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.mockito.Mock;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

public class CrossClusterConfigUtilsTests extends OpenSearchTestCase {

    @Mock
    private Client clientMock;

    public void testGetClientForClusterLocalCluster() {
        String clusterName = "localCluster";
        Client mockClient = mock(NodeClient.class);
        String localClusterName = "localCluster";

        Client result = CrossClusterConfigUtils.getClientForCluster(clusterName, mockClient, localClusterName);

        assertEquals(mockClient, result);
    }

    public void testGetClientForClusterRemoteCluster() {
        String clusterName = "remoteCluster";
        Client mockClient = mock(NodeClient.class);
        // Client mockRemoteClient = mock(Client.class);

        when(mockClient.getRemoteClusterClient(clusterName)).thenReturn(mockClient);

        Client result = CrossClusterConfigUtils.getClientForCluster(clusterName, mockClient, "localCluster");

        assertEquals(mockClient, result);
    }

    public void testSeparateClusterIndexesRemoteCluster() {
        List<String> indexes = Arrays.asList("remoteCluster:index1", "index2");
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.getClusterName()).thenReturn(new ClusterName("localCluster"));

        HashMap<String, List<String>> result = CrossClusterConfigUtils.separateClusterIndexes(indexes, mockClusterService);

        assertEquals(2, result.size());
        assertEquals(Arrays.asList("index1"), result.get("remoteCluster"));
        assertEquals(Arrays.asList("index2"), result.get("localCluster"));
    }

    public void testParseIndexName() {
        assertEquals("index1", CrossClusterConfigUtils.parseIndexName("remoteCluster:index1"));
        assertEquals("index2", CrossClusterConfigUtils.parseIndexName("index2"));
    }

    public void testParseClusterName() {
        assertEquals("remoteCluster", CrossClusterConfigUtils.parseClusterName("remoteCluster:index1"));
        assertEquals("", CrossClusterConfigUtils.parseClusterName("index2"));
    }
}
