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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
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
import java.util.Map;

import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsAction;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

public class UpdateMappingTests extends AbstractTimeSeriesTest {
    private static String resultIndexName;

    private ADIndexManagement adIndices;
    private ClusterService clusterService;
    private int numberOfNodes;
    private AdminClient adminClient;
    private ClusterState clusterState;
    private IndicesAdminClient indicesAdminClient;
    private Client client;
    private Settings settings;
    private DiscoveryNodeFilterer nodeFilter;

    @BeforeClass
    public static void setUpBeforeClass() {
        resultIndexName = ".opendistro-anomaly-results-history-2020.06.24-000003";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

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
                                AnomalyDetectorSettings.AD_MAX_PRIMARY_SHARDS
                            )
                    )
                )
        );

        clusterState = mock(ClusterState.class);

        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);

        Map<String, IndexMetadata> openMap = new HashMap<>();
        Metadata metadata = spy(Metadata.builder().indices(openMap).build());
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.hasIndex(anyString())).thenReturn(true);

        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.getRoutingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(anyString())).thenReturn(true);

        settings = Settings.EMPTY;
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        numberOfNodes = 2;
        when(nodeFilter.getNumberOfEligibleDataNodes()).thenReturn(numberOfNodes);
        adIndices = new ADIndexManagement(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
            NamedXContentRegistry.EMPTY
        );

        // simulate search config index for custom result index
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArgument(1);

            listener.onResponse(null);
            return null;
        }).when(client).search(any(), any());
    }

    public void testNoIndexToUpdate() {
        adIndices.update();
        verify(indicesAdminClient, never()).putMapping(any(), any());
        // for an index, we may check doesAliasExists/doesIndexExists for both mapping and setting
        // 5 indices * mapping/setting checks + 2 doesIndexExist in updateCustomResultIndexMapping (CUSTOM_RESULT + CUSTOM_INSIGHTS_RESULT)
        // = 12
        verify(clusterService, times(12)).state();
        adIndices.update();
        // we will not trigger new check since we have checked all indices before
        verify(clusterService, times(12)).state();
    }

    @SuppressWarnings({ "serial", "unchecked" })
    public void testUpdateMapping() throws IOException {
        doAnswer(invocation -> {
            ActionListener<GetAliasesResponse> listener = (ActionListener<GetAliasesResponse>) invocation.getArgument(1);

            Map<String, List<AliasMetadata>> builder = new HashMap<>();
            List<AliasMetadata> aliasMetadata = new ArrayList<>();
            aliasMetadata.add(AliasMetadata.builder(ADIndex.RESULT.name()).build());
            builder.put(resultIndexName, aliasMetadata);

            listener.onResponse(new GetAliasesResponse(builder));
            return null;
        }).when(indicesAdminClient).getAliases(any(GetAliasesRequest.class), any());

        IndexMetadata indexMetadata = IndexMetadata
            .builder(resultIndexName)
            .putAlias(AliasMetadata.builder(ADIndex.RESULT.getIndexName()))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(new MappingMetadata("type", new HashMap<String, Object>() {
                {
                    put(ADIndexManagement.META, new HashMap<String, Object>() {
                        {
                            // version 1 will cause update
                            put(org.opensearch.timeseries.constant.CommonName.SCHEMA_VERSION_FIELD, 1);
                        }
                    });
                }
            }))
            .build();
        Map<String, IndexMetadata> openMapBuilder = new HashMap<>();
        openMapBuilder.put(resultIndexName, indexMetadata);
        Metadata metadata = Metadata.builder().indices(openMapBuilder).build();
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);
        adIndices.update();
        verify(indicesAdminClient, times(1)).putMapping(any(), any());
    }

    // since SETTING_AUTO_EXPAND_REPLICAS is set, we won't update
    @SuppressWarnings("unchecked")
    public void testJobSettingNoUpdate() {
        Map<String, Settings> indexToSettings = new HashMap<>();
        Settings jobSettings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "1-all")
            .build();
        indexToSettings.put(ADIndex.JOB.getIndexName(), jobSettings);
        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(indexToSettings, new HashMap<>());
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = (ActionListener<GetSettingsResponse>) invocation.getArgument(2);

            listener.onResponse(getSettingsResponse);
            return null;
        }).when(client).execute(any(), any(), any());
        adIndices.update();
        verify(indicesAdminClient, never()).updateSettings(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void setUpSuccessfulGetJobSetting() {
        Map<String, Settings> indexToSettings = new HashMap<>();
        Settings jobSettings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        indexToSettings.put(ADIndex.JOB.getIndexName(), jobSettings);
        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(indexToSettings, new HashMap<>());
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = (ActionListener<GetSettingsResponse>) invocation.getArgument(2);

            listener.onResponse(getSettingsResponse);
            return null;
        }).when(client).execute(any(), any(), any());
    }

    // since SETTING_AUTO_EXPAND_REPLICAS is set, we won't update
    @SuppressWarnings("unchecked")
    public void testJobSettingUpdate() {
        setUpSuccessfulGetJobSetting();
        ArgumentCaptor<UpdateSettingsRequest> createIndexRequestCaptor = ArgumentCaptor.forClass(UpdateSettingsRequest.class);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            listener.onResponse(new AcknowledgedResponse(true));
            return null;
        }).when(indicesAdminClient).updateSettings(createIndexRequestCaptor.capture(), any());
        adIndices.update();
        verify(client, times(1)).execute(eq(GetSettingsAction.INSTANCE), any(), any());
        verify(indicesAdminClient, times(1)).updateSettings(any(), any());
        UpdateSettingsRequest request = createIndexRequestCaptor.getValue();
        assertEquals("1-10", request.settings().get(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS));

        adIndices.update();
        // won't have to do it again since we succeeded last time
        verify(client, times(1)).execute(eq(GetSettingsAction.INSTANCE), any(), any());
        verify(indicesAdminClient, times(1)).updateSettings(any(), any());
    }

    // since SETTING_NUMBER_OF_SHARDS is not there, we skip updating
    @SuppressWarnings("unchecked")
    public void testMissingPrimaryJobShards() {
        Map<String, Settings> indexToSettings = new HashMap<>();
        Settings jobSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        indexToSettings.put(ADIndex.JOB.getIndexName(), jobSettings);
        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(indexToSettings, new HashMap<>());
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = (ActionListener<GetSettingsResponse>) invocation.getArgument(2);

            listener.onResponse(getSettingsResponse);
            return null;
        }).when(client).execute(any(), any(), any());
        adIndices.update();
        verify(indicesAdminClient, never()).updateSettings(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testJobIndexNotFound() {
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> listener = (ActionListener<GetSettingsResponse>) invocation.getArgument(2);

            listener.onFailure(new IndexNotFoundException(ADIndex.JOB.getIndexName()));
            return null;
        }).when(client).execute(any(), any(), any());

        adIndices.update();
        verify(client, times(1)).execute(eq(GetSettingsAction.INSTANCE), any(), any());
        verify(indicesAdminClient, never()).updateSettings(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testFailtoUpdateJobSetting() throws InterruptedException {
        setUpSuccessfulGetJobSetting();
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArgument(1);

            listener.onFailure(new RuntimeException(ADIndex.JOB.getIndexName()));
            return null;
        }).when(indicesAdminClient).updateSettings(any(), any());

        adIndices.update();
        verify(client, times(1)).execute(eq(GetSettingsAction.INSTANCE), any(), any());
        verify(indicesAdminClient, times(1)).updateSettings(any(), any());

        // will have to do it again since last time we fail
        adIndices.update();
        verify(client, times(2)).execute(eq(GetSettingsAction.INSTANCE), any(), any());
        verify(indicesAdminClient, times(2)).updateSettings(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testTooManyUpdate() throws IOException {
        setUpSuccessfulGetJobSetting();
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArgument(2);

            listener.onFailure(new RuntimeException(ADIndex.JOB.getIndexName()));
            return null;
        }).when(indicesAdminClient).updateSettings(any(), any());

        adIndices = new ADIndexManagement(client, clusterService, threadPool, settings, nodeFilter, 1, NamedXContentRegistry.EMPTY);

        adIndices.update();
        adIndices.update();

        // even though we updated two times, since it passed the max retry limit (1), we won't retry
        verify(client, times(1)).execute(eq(GetSettingsAction.INSTANCE), any(), any());
    }
}
