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
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

public class InitAnomalyDetectionIndicesTests extends AbstractADTest {
    Client client;
    ClusterService clusterService;
    ThreadPool threadPool;
    Settings settings;
    DiscoveryNodeFilterer nodeFilter;
    AnomalyDetectionIndices adIndices;
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
                                AnomalyDetectorSettings.MAX_PRIMARY_SHARDS
                            )
                    )
                )
        );

        clusterName = new ClusterName("test");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices = new AnomalyDetectionIndices(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            AnomalyDetectorSettings.MAX_UPDATE_RETRY_TIMES
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
        if (index.equals(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {
            adIndices.initAnomalyDetectorIndexIfAbsent(listener);
        } else {
            adIndices.initDetectionStateIndex(listener);
        }

        ArgumentCaptor<CreateIndexResponse> captor = ArgumentCaptor.forClass(CreateIndexResponse.class);
        verify(listener).onResponse(captor.capture());
        CreateIndexResponse result = captor.getValue();
        assertEquals(index, result.index());
    }

    @SuppressWarnings("unchecked")
    private void fixedPrimaryShardsIndexNoCreationTemplate(String index, String alias) throws IOException {
        clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);

        RoutingTable.Builder rb = RoutingTable.builder();
        rb.addAsNew(indexMeta(index, 1L));
        when(clusterState.getRoutingTable()).thenReturn(rb.build());

        Metadata.Builder mb = Metadata.builder();
        mb.put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true);

        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        if (index.equals(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {
            adIndices.initAnomalyDetectorIndexIfAbsent(listener);
        } else {
            adIndices.initDefaultAnomalyResultIndexIfAbsent(listener);
        }

        verify(indicesClient, never()).create(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void adaptivePrimaryShardsIndexCreationTemplate(String index) throws IOException {

        doAnswer(invocation -> {
            CreateIndexRequest request = invocation.getArgument(0);
            if (index.equals(CommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
                assertTrue(request.aliases().contains(new Alias(CommonName.ANOMALY_RESULT_INDEX_ALIAS)));
            } else {
                assertEquals(index, request.index());
            }

            Settings settings = request.settings();
            // if (index.equals(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)) {
            // assertThat(settings.get("index.number_of_shards"), equalTo(Integer.toString(1)));
            // } else {
            // assertThat(settings.get("index.number_of_shards"), equalTo(Integer.toString(numberOfHotNodes)));
            // }

            ActionListener<CreateIndexResponse> listener = (ActionListener<CreateIndexResponse>) invocation.getArgument(1);

            listener.onResponse(new CreateIndexResponse(true, true, index));
            return null;
        }).when(indicesClient).create(any(), any());

        ActionListener<CreateIndexResponse> listener = mock(ActionListener.class);
        if (index.equals(AnomalyDetector.ANOMALY_DETECTORS_INDEX)) {
            adIndices.initAnomalyDetectorIndexIfAbsent(listener);
        } else if (index.equals(CommonName.DETECTION_STATE_INDEX)) {
            adIndices.initDetectionStateIndex(listener);
        } else if (index.equals(CommonName.CHECKPOINT_INDEX_NAME)) {
            adIndices.initCheckpointIndex(listener);
        }
        // else if (index.equals(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)) {
        // adIndices.initAnomalyDetectorJobIndex(listener);
        // }
        else {
            adIndices.initDefaultAnomalyResultIndexIfAbsent(listener);
        }

        ArgumentCaptor<CreateIndexResponse> captor = ArgumentCaptor.forClass(CreateIndexResponse.class);
        verify(listener).onResponse(captor.capture());
        CreateIndexResponse result = captor.getValue();
        assertEquals(index, result.index());
    }

    public void testNotCreateDetector() throws IOException {
        fixedPrimaryShardsIndexNoCreationTemplate(AnomalyDetector.ANOMALY_DETECTORS_INDEX, null);
    }

    public void testNotCreateResult() throws IOException {
        fixedPrimaryShardsIndexNoCreationTemplate(AnomalyDetector.ANOMALY_DETECTORS_INDEX, null);
    }

    public void testCreateDetector() throws IOException {
        fixedPrimaryShardsIndexCreationTemplate(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
    }

    public void testCreateState() throws IOException {
        fixedPrimaryShardsIndexCreationTemplate(CommonName.DETECTION_STATE_INDEX);
    }

    // public void testCreateJob() throws IOException {
    // adaptivePrimaryShardsIndexCreationTemplate(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX);
    // }

    public void testCreateResult() throws IOException {
        adaptivePrimaryShardsIndexCreationTemplate(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    public void testCreateCheckpoint() throws IOException {
        adaptivePrimaryShardsIndexCreationTemplate(CommonName.CHECKPOINT_INDEX_NAME);
    }
}
