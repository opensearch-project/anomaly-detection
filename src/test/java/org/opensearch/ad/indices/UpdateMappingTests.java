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

package org.opensearch.ad.indices;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

public class UpdateMappingTests extends AbstractADTest {
    private static String resultIndexName;

    private AnomalyDetectionIndices adIndices;
    private ClusterService clusterService;
    private int numberOfNodes;
    private AdminClient adminClient;
    private ClusterState clusterState;
    private IndicesAdminClient indicesAdminClient;

    @BeforeClass
    public static void setUpBeforeClass() {
        resultIndexName = ".opendistro-anomaly-results-history-2020.06.24-000003";
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();

        Client client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        doAnswer(invocation -> {
            ActionListener<GetAliasesResponse> listener = (ActionListener<GetAliasesResponse>) invocation.getArgument(1);

            ImmutableOpenMap.Builder<String, List<AliasMetadata>> builder = ImmutableOpenMap.builder();
            List<AliasMetadata> aliasMetadata = new ArrayList<>();
            aliasMetadata.add(AliasMetadata.builder(ADIndex.RESULT.name()).build());
            builder.put(resultIndexName, aliasMetadata);

            listener.onResponse(new GetAliasesResponse(builder.build()));
            return null;
        }).when(indicesAdminClient).getAliases(any(GetAliasesRequest.class), any());

        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                                AnomalyDetectorSettings.MAX_PRIMARY_SHARDS
                            )
                    )
                )
        );

        clusterState = mock(ClusterState.class);

        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);

        IndexMetadata indexMetadata = IndexMetadata
            .builder(resultIndexName)
            .putAlias(AliasMetadata.builder(ADIndex.RESULT.getIndexName()))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> openMapBuilder = ImmutableOpenMap.builder();
        openMapBuilder.put(resultIndexName, indexMetadata);
        Metadata metadata = Metadata.builder().indices(openMapBuilder.build()).build();
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);

        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.getRoutingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(anyString())).thenReturn(true);

        Settings settings = Settings.EMPTY;
        DiscoveryNodeFilterer nodeFilter = mock(DiscoveryNodeFilterer.class);
        numberOfNodes = 2;
        when(nodeFilter.getNumberOfEligibleDataNodes()).thenReturn(numberOfNodes);
        adIndices = new AnomalyDetectionIndices(client, clusterService, threadPool, settings, nodeFilter);
    }

    public void testNoIndexToUpdate() {
        adIndices.updateMappingIfNecessary();
        verify(indicesAdminClient, never()).putMapping(any(), any());
        // for each index, we check doesAliasExists/doesIndexExists and shouldUpdateConcreteIndex
        verify(clusterService, times(10)).state();
        adIndices.updateMappingIfNecessary();
        // we will not trigger new check since we have checked all indices before
        verify(clusterService, times(10)).state();
    }

    @SuppressWarnings("serial")
    public void testUpdate() throws IOException {
        IndexMetadata indexMetadata = IndexMetadata
            .builder(resultIndexName)
            .putAlias(AliasMetadata.builder(ADIndex.RESULT.getIndexName()))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(new MappingMetadata("type", new HashMap<String, Object>() {
                {
                    put(AnomalyDetectionIndices.META, new HashMap<String, Object>() {
                        {
                            // version 1 will cause update
                            put(CommonName.SCHEMA_VERSION_FIELD, 1);
                        }
                    });
                }
            }))
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> openMapBuilder = ImmutableOpenMap.builder();
        openMapBuilder.put(resultIndexName, indexMetadata);
        Metadata metadata = Metadata.builder().indices(openMapBuilder.build()).build();
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);
        adIndices.updateMappingIfNecessary();
        verify(indicesAdminClient, times(1)).putMapping(any(), any());
    }
}
