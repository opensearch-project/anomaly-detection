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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.util.CrossClusterConfigUtils.ResolvedIndices;
import org.opensearch.timeseries.util.CrossClusterConfigUtils.WildcardPatternGroup;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;

public class CrossClusterConfigUtilsTests extends OpenSearchTestCase {

    private Client mockClient;
    private String remoteClusterName;
    private String localClusterName;

    @Before
    public void setup() {
        // Initialize the mock clients
        mockClient = mock(NodeClient.class);
        localClusterName = "localCluster#local";
        remoteClusterName = "remoteCluster";
    }

    public void testGetClientForClusterLocalCluster() {
        Client result = CrossClusterConfigUtils.getClientForCluster(localClusterName, mockClient, "localCluster");
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
        assertEquals(Arrays.asList("index2"), result.get("localCluster#local"));
        assertEquals(Arrays.asList("index2"), result.get("remoteCluster2"));
    }

    public void testSeparateClusterIndexesWithSameRemoteClusterName() {
        List<String> indexes = Arrays.asList("opensearch:index1", "index2");
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.getClusterName()).thenReturn(new ClusterName("opensearch"));

        HashMap<String, List<String>> result = CrossClusterConfigUtils.separateClusterIndexes(indexes, mockClusterService);

        assertEquals(2, result.size());
        assertEquals(Arrays.asList("index1"), result.get("opensearch"));
        assertEquals(Arrays.asList("index2"), result.get("opensearch#local"));
    }

    public void testParseClusterAndIndexName_WithClusterAndIndex() {
        String input = "clusterA:index1";
        Pair<String, String> result = CrossClusterConfigUtils.parseClusterAndIndexName(input);
        assertEquals("clusterA", result.getKey());
        assertEquals("index1", result.getValue());
    }

    // -- wildcard cluster pattern helpers --

    public void testIsWildcardClusterPattern() {
        assertTrue(CrossClusterConfigUtils.isWildcardClusterPattern("*"));
        assertTrue(CrossClusterConfigUtils.isWildcardClusterPattern("cluster*"));
        assertTrue(CrossClusterConfigUtils.isWildcardClusterPattern("*cluster"));
        assertTrue(CrossClusterConfigUtils.isWildcardClusterPattern("*cluster*"));
        assertTrue(CrossClusterConfigUtils.isWildcardClusterPattern("a*b*c"));
        assertFalse(CrossClusterConfigUtils.isWildcardClusterPattern("cluster1"));
        assertFalse(CrossClusterConfigUtils.isWildcardClusterPattern(""));
        assertFalse(CrossClusterConfigUtils.isWildcardClusterPattern(null));
    }

    public void testContainsWildcardClusterPattern() {
        assertTrue(CrossClusterConfigUtils.containsWildcardClusterPattern(Arrays.asList("*:foo")));
        assertTrue(CrossClusterConfigUtils.containsWildcardClusterPattern(Arrays.asList("cluster*:foo", "bar")));
        assertTrue(CrossClusterConfigUtils.containsWildcardClusterPattern(Arrays.asList("local-idx", "*cluster*:foo")));
        assertFalse(CrossClusterConfigUtils.containsWildcardClusterPattern(Arrays.asList("local-idx", "remote:foo")));
        // wildcard only on index side does NOT count
        assertFalse(CrossClusterConfigUtils.containsWildcardClusterPattern(Arrays.asList("cluster1:foo*")));
        assertFalse(CrossClusterConfigUtils.containsWildcardClusterPattern(Collections.emptyList()));
    }

    public void testGlobToRegex_matchesPrefixSuffixContains() {
        assertTrue(CrossClusterConfigUtils.globToRegex("cluster*").matcher("cluster1").matches());
        assertTrue(CrossClusterConfigUtils.globToRegex("cluster*").matcher("cluster200").matches());
        assertFalse(CrossClusterConfigUtils.globToRegex("cluster*").matcher("abccluster").matches());

        assertTrue(CrossClusterConfigUtils.globToRegex("*cluster").matcher("abccluster").matches());
        assertFalse(CrossClusterConfigUtils.globToRegex("*cluster").matcher("cluster1").matches());

        assertTrue(CrossClusterConfigUtils.globToRegex("*cluster*").matcher("abcluster42").matches());
        assertTrue(CrossClusterConfigUtils.globToRegex("*cluster*").matcher("cluster1").matches());
        assertFalse(CrossClusterConfigUtils.globToRegex("*cluster*").matcher("foo").matches());

        assertTrue(CrossClusterConfigUtils.globToRegex("*").matcher("anything").matches());
        assertTrue(CrossClusterConfigUtils.globToRegex("*").matcher("").matches());
    }

    public void testGlobToRegex_escapesRegexMetacharacters() {
        // dots in remote cluster names should be treated literally, not as "any char"
        assertTrue(CrossClusterConfigUtils.globToRegex("us.east.*").matcher("us.east.prod").matches());
        assertFalse(CrossClusterConfigUtils.globToRegex("us.east.*").matcher("usXeastXprod").matches());
    }

    public void testMatchRemoteClusters() {
        Set<String> all = new LinkedHashSet<>(Arrays.asList("cluster1", "cluster2", "abccluster", "prod-us-east"));
        assertEquals(
            new LinkedHashSet<>(Arrays.asList("cluster1", "cluster2")),
            CrossClusterConfigUtils.matchRemoteClusters("cluster*", all)
        );
        assertEquals(new LinkedHashSet<>(Arrays.asList("abccluster")), CrossClusterConfigUtils.matchRemoteClusters("*cluster", all));
        assertEquals(
            new LinkedHashSet<>(Arrays.asList("cluster1", "cluster2", "abccluster")),
            CrossClusterConfigUtils.matchRemoteClusters("*cluster*", all)
        );
        assertEquals(all, CrossClusterConfigUtils.matchRemoteClusters("*", all));
        assertTrue(CrossClusterConfigUtils.matchRemoteClusters("nope*", all).isEmpty());
        assertTrue(CrossClusterConfigUtils.matchRemoteClusters("*", Collections.emptySet()).isEmpty());
    }

    // -- resolveIndices --

    public void testResolveIndices_strictOnly_localAndRemote() {
        ResolvedIndices resolved = CrossClusterConfigUtils
            .resolveIndices(
                Arrays.asList("local-idx", "remote1:idx1", "remote2:idx2"),
                "myLocal",
                new LinkedHashSet<>(Arrays.asList("remote1", "remote2", "remote3")),
                ValidationAspect.DETECTOR
            );
        assertTrue(resolved.getWildcardGroups().isEmpty());
        assertEquals(3, resolved.getStrict().size());
        assertEquals(Arrays.asList("local-idx"), resolved.getStrict().get("myLocal#local"));
        assertEquals(Arrays.asList("idx1"), resolved.getStrict().get("remote1"));
        assertEquals(Arrays.asList("idx2"), resolved.getStrict().get("remote2"));
    }

    public void testResolveIndices_wildcardExpansion() {
        ResolvedIndices resolved = CrossClusterConfigUtils
            .resolveIndices(
                Arrays.asList("*:idx1"),
                "myLocal",
                new LinkedHashSet<>(Arrays.asList("remote1", "remote2")),
                ValidationAspect.DETECTOR
            );
        assertTrue(resolved.getStrict().isEmpty());
        assertEquals(1, resolved.getWildcardGroups().size());
        WildcardPatternGroup group = resolved.getWildcardGroups().get(0);
        assertEquals("*:idx1", group.getOriginalEntry());
        assertEquals("*", group.getClusterPattern());
        assertEquals("idx1", group.getIndexPart());
        assertEquals(2, group.getExpanded().size());
        assertEquals(Arrays.asList("idx1"), group.getExpanded().get("remote1"));
        assertEquals(Arrays.asList("idx1"), group.getExpanded().get("remote2"));
    }

    public void testResolveIndices_clusterPrefixWildcard() {
        // cluster* should match cluster1, cluster2 but NOT abccluster
        ResolvedIndices resolved = CrossClusterConfigUtils
            .resolveIndices(
                Arrays.asList("cluster*:idx1"),
                "myLocal",
                new LinkedHashSet<>(Arrays.asList("cluster1", "cluster2", "abccluster")),
                ValidationAspect.DETECTOR
            );
        assertEquals(1, resolved.getWildcardGroups().size());
        WildcardPatternGroup group = resolved.getWildcardGroups().get(0);
        assertEquals(2, group.getExpanded().size());
        assertTrue(group.getExpanded().containsKey("cluster1"));
        assertTrue(group.getExpanded().containsKey("cluster2"));
        assertFalse(group.getExpanded().containsKey("abccluster"));
    }

    public void testResolveIndices_wildcardSuffixAndContains() {
        // *cluster should match only abccluster, *cluster* should match all three
        ResolvedIndices suffix = CrossClusterConfigUtils
            .resolveIndices(
                Arrays.asList("*cluster:idx1"),
                "myLocal",
                new LinkedHashSet<>(Arrays.asList("cluster1", "cluster2", "abccluster")),
                ValidationAspect.DETECTOR
            );
        assertEquals(Collections.singleton("abccluster"), suffix.getWildcardGroups().get(0).getExpanded().keySet());

        ResolvedIndices contains = CrossClusterConfigUtils
            .resolveIndices(
                Arrays.asList("*cluster*:idx1"),
                "myLocal",
                new LinkedHashSet<>(Arrays.asList("cluster1", "cluster2", "abccluster")),
                ValidationAspect.DETECTOR
            );
        assertEquals(
            new LinkedHashSet<>(Arrays.asList("cluster1", "cluster2", "abccluster")),
            contains.getWildcardGroups().get(0).getExpanded().keySet()
        );
    }

    public void testResolveIndices_wildcardAndExplicitMergeDoesNotOverwrite() {
        // *:index1 should expand to {remote1: [index1], remote2: [index1]}
        // remote1:index2 should add a strict entry for remote1 with index2
        // Combining them must NOT drop index1 from remote1.
        ResolvedIndices resolved = CrossClusterConfigUtils
            .resolveIndices(
                Arrays.asList("*:index1", "remote1:index2"),
                "myLocal",
                new LinkedHashSet<>(Arrays.asList("remote1", "remote2")),
                ValidationAspect.DETECTOR
            );

        // strict entry for remote1
        assertEquals(Arrays.asList("index2"), resolved.getStrict().get("remote1"));

        // wildcard group preserves remote1:[index1] and remote2:[index1]
        assertEquals(1, resolved.getWildcardGroups().size());
        WildcardPatternGroup group = resolved.getWildcardGroups().get(0);
        assertEquals(Arrays.asList("index1"), group.getExpanded().get("remote1"));
        assertEquals(Arrays.asList("index1"), group.getExpanded().get("remote2"));
    }

    public void testResolveIndices_multipleWildcardPatternsTargetSameClusterCombine() {
        // Two wildcard patterns *:a and *:b both expand against {remote1}. Each is its own group;
        // each group keeps its own index part.
        ResolvedIndices resolved = CrossClusterConfigUtils
            .resolveIndices(
                Arrays.asList("*:a", "*:b"),
                "myLocal",
                new LinkedHashSet<>(Arrays.asList("remote1")),
                ValidationAspect.DETECTOR
            );
        assertEquals(2, resolved.getWildcardGroups().size());
        assertEquals(Arrays.asList("a"), resolved.getWildcardGroups().get(0).getExpanded().get("remote1"));
        assertEquals(Arrays.asList("b"), resolved.getWildcardGroups().get(1).getExpanded().get("remote1"));
    }

    public void testResolveIndices_emptyIndexAfterColonFails() {
        ValidationException ex = expectThrows(
            ValidationException.class,
            () -> CrossClusterConfigUtils
                .resolveIndices(Arrays.asList("*:"), "myLocal", new LinkedHashSet<>(Arrays.asList("remote1")), ValidationAspect.DETECTOR)
        );
        assertEquals(ValidationIssueType.INDICES, ex.getType());
    }

    public void testResolveIndices_emptyIndexExplicitClusterFails() {
        ValidationException ex = expectThrows(
            ValidationException.class,
            () -> CrossClusterConfigUtils
                .resolveIndices(Arrays.asList("remote1:"), "myLocal", Collections.emptySet(), ValidationAspect.DETECTOR)
        );
        assertEquals(ValidationIssueType.INDICES, ex.getType());
    }

    public void testResolveIndices_wildcardNoRemoteClustersFails() {
        ValidationException ex = expectThrows(
            ValidationException.class,
            () -> CrossClusterConfigUtils
                .resolveIndices(Arrays.asList("*:idx"), "myLocal", Collections.emptySet(), ValidationAspect.DETECTOR)
        );
        assertEquals(ValidationIssueType.INDICES, ex.getType());
    }

    public void testResolveIndices_wildcardNoMatchingClusterFails() {
        ValidationException ex = expectThrows(
            ValidationException.class,
            () -> CrossClusterConfigUtils
                .resolveIndices(
                    Arrays.asList("nomatch*:idx"),
                    "myLocal",
                    new LinkedHashSet<>(Arrays.asList("remote1", "remote2")),
                    ValidationAspect.DETECTOR
                )
        );
        assertEquals(ValidationIssueType.INDICES, ex.getType());
    }

    public void testResolveIndices_nullOrEmptyInputFails() {
        ValidationException ex1 = expectThrows(
            ValidationException.class,
            () -> CrossClusterConfigUtils.resolveIndices(null, "myLocal", Collections.emptySet(), ValidationAspect.DETECTOR)
        );
        assertEquals(ValidationIssueType.INDICES, ex1.getType());
        ValidationException ex2 = expectThrows(
            ValidationException.class,
            () -> CrossClusterConfigUtils
                .resolveIndices(Collections.emptyList(), "myLocal", Collections.emptySet(), ValidationAspect.DETECTOR)
        );
        assertEquals(ValidationIssueType.INDICES, ex2.getType());
    }

    public void testResolveIndices_wildcardIsRemoteOnly() {
        // The local cluster matches "myLocal*" by name, but wildcards must only match registered
        // remote clusters. The local cluster should NOT be included via wildcard expansion.
        ResolvedIndices resolved = CrossClusterConfigUtils
            .resolveIndices(
                Arrays.asList("myLocal*:idx"),
                "myLocal",
                new LinkedHashSet<>(Arrays.asList("myLocal-stage", "remote1")),
                ValidationAspect.DETECTOR
            );
        assertEquals(1, resolved.getWildcardGroups().size());
        WildcardPatternGroup group = resolved.getWildcardGroups().get(0);
        // Only the matched remote cluster, never the local one.
        assertEquals(Collections.singleton("myLocal-stage"), group.getExpanded().keySet());
    }
}
