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
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.rollover.Condition;
import org.opensearch.action.admin.indices.rollover.MaxDocsCondition;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.sdk.Extension;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKClusterAdminClient;
import org.opensearch.sdk.SDKClient.SDKIndicesClient;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKClusterService.SDKClusterSettings;
import org.opensearch.threadpool.ThreadPool;

public class RolloverTests extends AbstractADTest {
    private AnomalyDetectionIndices adIndices;
    private SDKIndicesClient indicesClient;
    private SDKClusterAdminClient clusterAdminClient;
    private ClusterName clusterName;
    private ClusterState clusterState;
    private SDKClusterService clusterService;
    private long defaultMaxDocs;
    private int numberOfNodes;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SDKRestClient client = mock(SDKRestClient.class);
        OpenSearchAsyncClient sdkJavaAsyncClient = mock(OpenSearchAsyncClient.class);
        indicesClient = mock(SDKIndicesClient.class);
        SDKRestClient adminClient = mock(SDKRestClient.class);
        clusterService = mock(SDKClusterService.class);

        Settings settings = Settings.EMPTY;
        List<Setting<?>> settingsList = List
            .of(
                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                AnomalyDetectorSettings.MAX_PRIMARY_SHARDS
            );
        ExtensionsRunner mockRunner = mock(ExtensionsRunner.class);
        Extension mockExtension = mock(Extension.class);
        when(mockRunner.getEnvironmentSettings()).thenReturn(settings);
        when(mockRunner.getExtension()).thenReturn(mockExtension);
        when(mockExtension.getSettings()).thenReturn(settingsList);
        SDKClusterSettings clusterSettings = new SDKClusterService(mockRunner).getClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        clusterName = new ClusterName("test");

        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        DiscoveryNodeFilterer nodeFilter = mock(DiscoveryNodeFilterer.class);
        numberOfNodes = 2;
        when(nodeFilter.getNumberOfEligibleDataNodes()).thenReturn(numberOfNodes);

        adIndices = new AnomalyDetectionIndices(
            // FIXME: Replace with SDK equivalents when re-enabling tests
            // https://github.com/opensearch-project/opensearch-sdk-java/issues/288
            client,
            sdkJavaAsyncClient,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            AnomalyDetectorSettings.MAX_UPDATE_RETRY_TIMES
        );

        clusterAdminClient = mock(SDKClusterAdminClient.class);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        // FIXME Implement state()
        // https://github.com/opensearch-project/opensearch-sdk-java/issues/354
        // doAnswer(invocation -> {
        // ClusterStateRequest clusterStateRequest = invocation.getArgument(0);
        // assertEquals(AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN, clusterStateRequest.indices()[0]);
        // @SuppressWarnings("unchecked")
        // ActionListener<ClusterStateResponse> listener = (ActionListener<ClusterStateResponse>) invocation.getArgument(1);
        // listener.onResponse(new ClusterStateResponse(clusterName, clusterState, true));
        // return null;
        // }).when(clusterAdminClient).state().state(any(), any());

        defaultMaxDocs = AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD.getDefault(Settings.EMPTY);
    }

    private void assertRolloverRequest(RolloverRequest request) {
        assertEquals(CommonName.ANOMALY_RESULT_INDEX_ALIAS, request.indices()[0]);

        Map<String, Condition<?>> conditions = request.getConditions();
        assertEquals(1, conditions.size());
        assertEquals(new MaxDocsCondition(defaultMaxDocs * numberOfNodes), conditions.get(MaxDocsCondition.NAME));

        CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
        assertEquals(AnomalyDetectionIndices.AD_RESULT_HISTORY_INDEX_PATTERN, createIndexRequest.index());
        assertTrue(createIndexRequest.mappings().contains("data_start_time"));
    }

    public void testNotRolledOver() {
        doAnswer(invocation -> {
            RolloverRequest request = invocation.getArgument(0);
            assertRolloverRequest(request);

            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArgument(1);

            listener.onResponse(new RolloverResponse(null, null, Collections.emptyMap(), request.isDryRun(), false, true, true));
            return null;
        }).when(indicesClient).rolloverIndex(any(), any());

        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true);
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        // FIXME: Implement state()
        // https://github.com/opensearch-project/opensearch-sdk-java/issues/354
        // adIndices.rolloverAndDeleteHistoryIndex();
        // verify(clusterAdminClient, never()).state(any(), any());
        // verify(indicesClient, times(1)).rolloverIndex(any(), any());
    }

    private void setUpRolloverSuccess() {
        doAnswer(invocation -> {
            RolloverRequest request = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArgument(1);

            assertEquals(CommonName.ANOMALY_RESULT_INDEX_ALIAS, request.indices()[0]);

            Map<String, Condition<?>> conditions = request.getConditions();
            assertEquals(1, conditions.size());
            assertEquals(new MaxDocsCondition(defaultMaxDocs * numberOfNodes), conditions.get(MaxDocsCondition.NAME));

            CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
            assertEquals(AnomalyDetectionIndices.AD_RESULT_HISTORY_INDEX_PATTERN, createIndexRequest.index());
            assertTrue(createIndexRequest.mappings().contains("data_start_time"));
            listener.onResponse(new RolloverResponse(null, null, Collections.emptyMap(), request.isDryRun(), true, true, true));
            return null;
        }).when(indicesClient).rolloverIndex(any(), any());
    }

    public void testRolledOverButNotDeleted() {
        setUpRolloverSuccess();

        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(
                indexMeta(
                    ".opendistro-anomaly-results-history-2020.06.24-000004",
                    Instant.now().toEpochMilli(),
                    CommonName.ANOMALY_RESULT_INDEX_ALIAS
                ),
                true
            );
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        // FIXME: Implement state()
        // https://github.com/opensearch-project/opensearch-sdk-java/issues/354
        // adIndices.rolloverAndDeleteHistoryIndex();
        // verify(clusterAdminClient, times(1)).state(any(), any());
        // verify(indicesClient, times(1)).rolloverIndex(any(), any());
        // verify(indicesClient, never()).delete(any(), any());
    }

    private void setUpTriggerDelete() {
        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000002", 1L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 2L, CommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(
                indexMeta(
                    ".opendistro-anomaly-results-history-2020.06.24-000004",
                    Instant.now().toEpochMilli(),
                    CommonName.ANOMALY_RESULT_INDEX_ALIAS
                ),
                true
            );
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);
    }

    public void testRolledOverDeleted() {
        setUpRolloverSuccess();
        setUpTriggerDelete();

        // FIXME: Implement state()
        // https://github.com/opensearch-project/opensearch-sdk-java/issues/354
        // adIndices.rolloverAndDeleteHistoryIndex();
        // verify(clusterAdminClient, times(1)).state(any(), any());
        // verify(indicesClient, times(1)).rolloverIndex(any(), any());
        // verify(indicesClient, times(1)).delete(any(), any());
    }

    public void testRetryingDelete() {
        setUpRolloverSuccess();
        setUpTriggerDelete();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArgument(1);

            // group delete not acked, trigger retry. But retry also failed.
            listener.onResponse(new AcknowledgedResponse(false));

            return null;
        }).when(indicesClient).delete(any(), any());

        // FIXME: Implement state()
        // https://github.com/opensearch-project/opensearch-sdk-java/issues/354
        // adIndices.rolloverAndDeleteHistoryIndex();
        // verify(clusterAdminClient, times(1)).state(any(), any());
        // verify(indicesClient, times(1)).rolloverIndex(any(), any());
        // 1 group delete, 1 separate retry for each index to delete
        // verify(indicesClient, times(2)).delete(any(), any());
    }
}
