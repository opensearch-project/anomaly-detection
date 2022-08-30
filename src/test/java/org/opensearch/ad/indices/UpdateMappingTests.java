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

/*
package org.opensearch.ad.indices;


public class UpdateMappingTests extends AbstractADTest {
    private static String resultIndexName;

    private AnomalyDetectionIndices adIndices;
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

    @SuppressWarnings("unchecked")
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
                                AnomalyDetectorSettings.MAX_PRIMARY_SHARDS
                            )
                    )
                )
        );

        clusterState = mock(ClusterState.class);

        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);

        ImmutableOpenMap.Builder<String, IndexMetadata> openMapBuilder = ImmutableOpenMap.builder();
        Metadata metadata = Metadata.builder().indices(openMapBuilder.build()).build();
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);

        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.getRoutingTable()).thenReturn(routingTable);
        when(routingTable.hasIndex(anyString())).thenReturn(true);

        settings = Settings.EMPTY;
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        numberOfNodes = 2;
        when(nodeFilter.getNumberOfEligibleDataNodes()).thenReturn(numberOfNodes);
        adIndices = new AnomalyDetectionIndices(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            AnomalyDetectorSettings.MAX_UPDATE_RETRY_TIMES
        );
    }

    public void testNoIndexToUpdate() {
        adIndices.update();
        verify(indicesAdminClient, never()).putMapping(any(), any());
        // for an index, we may check doesAliasExists/doesIndexExists and shouldUpdateConcreteIndex
        // 1 time for result index since alias does not exist and 2 times for other indices
        verify(clusterService, times(9)).state();
        adIndices.update();
        // we will not trigger new check since we have checked all indices before
        verify(clusterService, times(9)).state();
    }

    @SuppressWarnings({ "serial", "unchecked" })
    public void testUpdateMapping() throws IOException {
        doAnswer(invocation -> {
            ActionListener<GetAliasesResponse> listener = (ActionListener<GetAliasesResponse>) invocation.getArgument(1);

            ImmutableOpenMap.Builder<String, List<AliasMetadata>> builder = ImmutableOpenMap.builder();
            List<AliasMetadata> aliasMetadata = new ArrayList<>();
            aliasMetadata.add(AliasMetadata.builder(ADIndex.RESULT.name()).build());
            builder.put(resultIndexName, aliasMetadata);

            listener.onResponse(new GetAliasesResponse(builder.build()));
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
        adIndices.update();
        verify(indicesAdminClient, times(1)).putMapping(any(), any());
    }

    // since SETTING_AUTO_EXPAND_REPLICAS is set, we won't update
    @SuppressWarnings("unchecked")
    public void testJobSettingNoUpdate() {
        ImmutableOpenMap.Builder<String, Settings> indexToSettings = ImmutableOpenMap.builder();
        Settings jobSettings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "1-all")
            .build();
        indexToSettings.put(ADIndex.JOB.getIndexName(), jobSettings);
        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(indexToSettings.build(), ImmutableOpenMap.of());
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
        ImmutableOpenMap.Builder<String, Settings> indexToSettings = ImmutableOpenMap.builder();
        Settings jobSettings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        indexToSettings.put(ADIndex.JOB.getIndexName(), jobSettings);
        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(indexToSettings.build(), ImmutableOpenMap.of());
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
        ImmutableOpenMap.Builder<String, Settings> indexToSettings = ImmutableOpenMap.builder();
        Settings jobSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        indexToSettings.put(ADIndex.JOB.getIndexName(), jobSettings);
        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(indexToSettings.build(), ImmutableOpenMap.of());
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
    public void testFailtoUpdateJobSetting() {
        setUpSuccessfulGetJobSetting();
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArgument(2);

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
    public void testTooManyUpdate() {
        setUpSuccessfulGetJobSetting();
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArgument(2);

            listener.onFailure(new RuntimeException(ADIndex.JOB.getIndexName()));
            return null;
        }).when(indicesAdminClient).updateSettings(any(), any());

        adIndices = new AnomalyDetectionIndices(client, clusterService, threadPool, settings, nodeFilter, 1);

        adIndices.update();
        adIndices.update();

        // even though we updated two times, since it passed the max retry limit (1), we won't retry
        verify(client, times(1)).execute(eq(GetSettingsAction.INSTANCE), any(), any());
    }
}
*/
