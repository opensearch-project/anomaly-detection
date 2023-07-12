/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.indices;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.env.Environment;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public class ForecastResultIndexTests extends AbstractTimeSeriesTest {
    private ForecastIndexManagement forecastIndices;
    private IndicesAdminClient indicesClient;
    private ClusterAdminClient clusterAdminClient;
    private ClusterName clusterName;
    private ClusterState clusterState;
    private ClusterService clusterService;
    private long defaultMaxDocs;
    private int numberOfNodes;
    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        indicesClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                ForecastSettings.FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                                ForecastSettings.FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD,
                                ForecastSettings.FORECAST_RESULT_HISTORY_RETENTION_PERIOD,
                                ForecastSettings.FORECAST_MAX_PRIMARY_SHARDS
                            )
                    )
                )
        );

        clusterName = new ClusterName("test");

        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ThreadPool threadPool = mock(ThreadPool.class);
        Settings settings = Settings.EMPTY;
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        DiscoveryNodeFilterer nodeFilter = mock(DiscoveryNodeFilterer.class);
        numberOfNodes = 2;
        when(nodeFilter.getNumberOfEligibleDataNodes()).thenReturn(numberOfNodes);

        forecastIndices = new ForecastIndexManagement(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES
        );

        clusterAdminClient = mock(ClusterAdminClient.class);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        doAnswer(invocation -> {
            ClusterStateRequest clusterStateRequest = invocation.getArgument(0);
            assertEquals(ForecastIndexManagement.ALL_FORECAST_RESULTS_INDEX_PATTERN, clusterStateRequest.indices()[0]);
            @SuppressWarnings("unchecked")
            ActionListener<ClusterStateResponse> listener = (ActionListener<ClusterStateResponse>) invocation.getArgument(1);
            listener.onResponse(new ClusterStateResponse(clusterName, clusterState, true));
            return null;
        }).when(clusterAdminClient).state(any(), any());

        defaultMaxDocs = ForecastSettings.FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD.getDefault(Settings.EMPTY);

        clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);
    }

    public void testMappingSetToUpdated() throws IOException {
        try {
            doAnswer(invocation -> {
                ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocation.getArgument(1);
                listener.onResponse(new CreateIndexResponse(true, true, "blah"));
                return null;
            }).when(indicesClient).create(any(), any());

            super.setUpLog4jForJUnit(IndexManagement.class);

            ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
            forecastIndices.initDefaultResultIndexDirectly(listener);
            verify(listener, times(1)).onResponse(any(CreateIndexResponse.class));
            assertTrue(testAppender.containsMessage("mapping up-to-date"));
        } finally {
            super.tearDownLog4jForJUnit();
        }

    }

    public void testInitCustomResultIndexNoAck() {
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> createIndexListener = (ActionListener<CreateIndexResponse>) invocation.getArgument(1);
            createIndexListener.onResponse(new CreateIndexResponse(false, false, "blah"));
            return null;
        }).when(indicesClient).create(any(), any());

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        forecastIndices.initCustomResultIndexAndExecute("abc", function, listener);
        verify(listener, times(1)).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof EndRunException);
        assertTrue(
            "actual: " + value.getMessage(),
            value.getMessage().contains("Creating result index with mappings call not acknowledged")
        );

    }

    public void testInitCustomResultIndexAlreadyExist() throws IOException {
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        String indexName = "abc";

        Settings settings = Settings
            .builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        IndexMetadata indexMetaData = IndexMetadata
            .builder(indexName)
            .settings(settings)
            .putMapping(ForecastIndexManagement.getResultMappings())
            .build();
        final Map<String, IndexMetadata> indices = new HashMap<>();
        indices.put(indexName, indexMetaData);

        clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().indices(indices).build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> createIndexListener = (ActionListener<CreateIndexResponse>) invocation.getArgument(1);
            createIndexListener.onFailure(new ResourceAlreadyExistsException(new Index(indexName, indexName)));
            return null;
        }).when(indicesClient).create(any(), any());

        forecastIndices.initCustomResultIndexAndExecute(indexName, function, listener);
        verify(listener, never()).onFailure(any());
    }

    public void testInitCustomResultIndexUnknownException() throws IOException {
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        String indexName = "abc";
        String exceptionMsg = "blah";

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> createIndexListener = (ActionListener<CreateIndexResponse>) invocation.getArgument(1);
            createIndexListener.onFailure(new IllegalArgumentException(exceptionMsg));
            return null;
        }).when(indicesClient).create(any(), any());
        super.setUpLog4jForJUnit(IndexManagement.class);
        try {
            forecastIndices.initCustomResultIndexAndExecute(indexName, function, listener);
            ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
            verify(listener, times(1)).onFailure(response.capture());

            Exception value = response.getValue();
            assertTrue(value instanceof IllegalArgumentException);
            assertTrue("actual: " + value.getMessage(), value.getMessage().contains(exceptionMsg));
        } finally {
            super.tearDownLog4jForJUnit();
        }
    }
}
