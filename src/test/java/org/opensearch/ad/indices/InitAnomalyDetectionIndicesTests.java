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

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.mockito.ArgumentCaptor;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public class InitAnomalyDetectionIndicesTests extends AbstractTimeSeriesTest {
    Client client;
    ClusterService clusterService;
    ThreadPool threadPool;
    Settings settings;
    DiscoveryNodeFilterer nodeFilter;
    ADIndexManagement adIndices;
    ClusterName clusterName;
    ClusterState clusterState;
    IndicesAdminClient indicesClient;
    int numberOfHotNodes;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        client = mock(Client.class);
        indicesClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);

        numberOfHotNodes = 4;
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        when(nodeFilter.getNumberOfEligibleDataNodes()).thenReturn(numberOfHotNodes);

        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
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

        clusterName = new ClusterName("test");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices = new ADIndexManagement(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES
        );
    }

    @SuppressWarnings("unchecked")
    private void fixedPrimaryShardsIndexCreationTemplate(String index) throws IOException {
        doAnswer(invocation -> {
            CreateIndexRequest request = invocation.getArgument(0);
            assertEquals(index, request.index());

            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocation.getArgument(1);

            listener.onResponse(new CreateIndexResponse(true, true, index));
            return null;
        }).when(indicesClient).create(any(), any());

        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        if (index.equals(CommonName.CONFIG_INDEX)) {
            adIndices.initConfigIndexIfAbsent(listener);
        } else {
            adIndices.initStateIndex(listener);
        }

        ArgumentCaptor<CreateIndexResponse> captor = ArgumentCaptor.forClass(CreateIndexResponse.class);
        verify(listener).onResponse(captor.capture());
        CreateIndexResponse result = captor.getValue();
        assertEquals(index, result.index());
    }

    @SuppressWarnings("unchecked")
    private void fixedPrimaryShardsIndexNoCreationTemplate(String index, String... alias) throws IOException {
        clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);

        // RoutingTable.Builder rb = RoutingTable.builder();
        // rb.addAsNew(indexMeta(index, 1L));
        // when(clusterState.metadata()).thenReturn(rb.build());

        Metadata.Builder mb = Metadata.builder();
        // mb.put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS), true);
        mb.put(indexMeta(index, 1L, alias), true);
        when(clusterState.metadata()).thenReturn(mb.build());

        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        if (index.equals(CommonName.CONFIG_INDEX)) {
            adIndices.initConfigIndexIfAbsent(listener);
        } else {
            adIndices.initDefaultResultIndexIfAbsent(listener);
        }

        verify(indicesClient, never()).create(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void adaptivePrimaryShardsIndexCreationTemplate(String index) throws IOException {

        doAnswer(invocation -> {
            CreateIndexRequest request = invocation.getArgument(0);
            if (index.equals(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
                assertTrue(request.aliases().contains(new Alias(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS)));
            } else {
                assertEquals(index, request.index());
            }

            Settings settings = request.settings();
            if (index.equals(CommonName.JOB_INDEX)) {
                assertThat(settings.get("index.number_of_shards"), equalTo(Integer.toString(1)));
            } else {
                assertThat(settings.get("index.number_of_shards"), equalTo(Integer.toString(numberOfHotNodes)));
            }

            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocation.getArgument(1);

            listener.onResponse(new CreateIndexResponse(true, true, index));
            return null;
        }).when(indicesClient).create(any(), any());

        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        if (index.equals(CommonName.CONFIG_INDEX)) {
            adIndices.initConfigIndexIfAbsent(listener);
        } else if (index.equals(ADCommonName.DETECTION_STATE_INDEX)) {
            adIndices.initStateIndex(listener);
        } else if (index.equals(ADCommonName.CHECKPOINT_INDEX_NAME)) {
            adIndices.initCheckpointIndex(listener);
        } else if (index.equals(CommonName.JOB_INDEX)) {
            adIndices.initJobIndex(listener);
        } else {
            adIndices.initDefaultResultIndexIfAbsent(listener);
        }

        ArgumentCaptor<CreateIndexResponse> captor = ArgumentCaptor.forClass(CreateIndexResponse.class);
        verify(listener).onResponse(captor.capture());
        CreateIndexResponse result = captor.getValue();
        assertEquals(index, result.index());
    }

    public void testNotCreateDetector() throws IOException {
        fixedPrimaryShardsIndexNoCreationTemplate(CommonName.CONFIG_INDEX);
    }

    public void testNotCreateResult() throws IOException {
        fixedPrimaryShardsIndexNoCreationTemplate(CommonName.CONFIG_INDEX);
    }

    public void testCreateDetector() throws IOException {
        fixedPrimaryShardsIndexCreationTemplate(CommonName.CONFIG_INDEX);
    }

    public void testCreateState() throws IOException {
        fixedPrimaryShardsIndexCreationTemplate(ADCommonName.DETECTION_STATE_INDEX);
    }

    public void testCreateJob() throws IOException {
        adaptivePrimaryShardsIndexCreationTemplate(CommonName.JOB_INDEX);
    }

    public void testCreateResult() throws IOException {
        adaptivePrimaryShardsIndexCreationTemplate(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    public void testCreateCheckpoint() throws IOException {
        adaptivePrimaryShardsIndexCreationTemplate(ADCommonName.CHECKPOINT_INDEX_NAME);
    }
}
