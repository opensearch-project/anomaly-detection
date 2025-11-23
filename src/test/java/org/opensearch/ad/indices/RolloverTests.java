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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.timeseries.TestHelpers.createSearchResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.rollover.Condition;
import org.opensearch.action.admin.indices.rollover.MaxAgeCondition;
import org.opensearch.action.admin.indices.rollover.MaxDocsCondition;
import org.opensearch.action.admin.indices.rollover.MaxSizeCondition;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.ClusterAdminClient;
import org.opensearch.transport.client.IndicesAdminClient;

public class RolloverTests extends AbstractTimeSeriesTest {
    private ADIndexManagement adIndices;
    private IndicesAdminClient indicesClient;
    private ClusterAdminClient clusterAdminClient;
    private Client client;
    private ClusterName clusterName;
    private ClusterState clusterState;
    private ClusterService clusterService;
    private NamedXContentRegistry namedXContentRegistry;
    private long defaultMaxDocs;
    private int numberOfNodes;

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

        ThreadPool threadPool = mock(ThreadPool.class);
        Settings settings = Settings.EMPTY;
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);

        DiscoveryNodeFilterer nodeFilter = mock(DiscoveryNodeFilterer.class);
        numberOfNodes = 2;
        when(nodeFilter.getNumberOfEligibleDataNodes()).thenReturn(numberOfNodes);

        namedXContentRegistry = TestHelpers.xContentRegistry();

        adIndices = new ADIndexManagement(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
            namedXContentRegistry
        );

        clusterAdminClient = mock(ClusterAdminClient.class);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        doAnswer(invocation -> {
            ClusterStateRequest clusterStateRequest = invocation.getArgument(0);
            // Accept both system result index pattern and insights index pattern
            String requestedPattern = clusterStateRequest.indices()[0];
            assertTrue(
                requestedPattern.equals(ADIndexManagement.ALL_AD_RESULTS_INDEX_PATTERN)
                    || requestedPattern.equals(IndexManagement.getAllHistoryIndexPattern(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS))
            );
            @SuppressWarnings("unchecked")
            ActionListener<ClusterStateResponse> listener = (ActionListener<ClusterStateResponse>) invocation.getArgument(1);
            listener.onResponse(new ClusterStateResponse(clusterName, clusterState, true));
            return null;
        }).when(clusterAdminClient).state(any(), any());

        defaultMaxDocs = AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD.getDefault(Settings.EMPTY);
    }

    private void assertRolloverRequest(RolloverRequest request) {
        assertEquals(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, request.indices()[0]);

        Map<String, Condition<?>> conditions = request.getConditions();
        assertEquals(1, conditions.size());
        assertEquals(new MaxDocsCondition(defaultMaxDocs * numberOfNodes), conditions.get(MaxDocsCondition.NAME));

        CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
        assertEquals(ADIndexManagement.AD_RESULT_HISTORY_INDEX_PATTERN, createIndexRequest.index());
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
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS), true);
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices.rolloverAndDeleteHistoryIndex();
        verify(clusterAdminClient, never()).state(any(), any());
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
    }

    private void setUpRolloverSuccess() {
        doAnswer(invocation -> {
            RolloverRequest request = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArgument(1);

            assertEquals(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, request.indices()[0]);

            Map<String, Condition<?>> conditions = request.getConditions();
            assertEquals(1, conditions.size());
            assertEquals(new MaxDocsCondition(defaultMaxDocs * numberOfNodes), conditions.get(MaxDocsCondition.NAME));

            CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
            assertEquals(ADIndexManagement.AD_RESULT_HISTORY_INDEX_PATTERN, createIndexRequest.index());
            assertTrue(createIndexRequest.mappings().contains("data_start_time"));
            listener.onResponse(new RolloverResponse(null, null, Collections.emptyMap(), request.isDryRun(), true, true, true));
            return null;
        }).when(indicesClient).rolloverIndex(any(), any());
    }

    public void testRolledOverButNotDeleted() {
        setUpRolloverSuccess();

        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(
                indexMeta(
                    ".opendistro-anomaly-results-history-2020.06.24-000004",
                    Instant.now().toEpochMilli(),
                    ADCommonName.ANOMALY_RESULT_INDEX_ALIAS
                ),
                true
            );
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices.rolloverAndDeleteHistoryIndex();
        verify(clusterAdminClient, times(1)).state(any(), any());
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        verify(indicesClient, never()).delete(any(), any());
    }

    private void setUpTriggerDelete() {
        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000002", 1L, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 2L, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(
                indexMeta(
                    ".opendistro-anomaly-results-history-2020.06.24-000004",
                    Instant.now().toEpochMilli(),
                    ADCommonName.ANOMALY_RESULT_INDEX_ALIAS
                ),
                true
            );
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);
    }

    public void testRolledOverDeleted() {
        setUpRolloverSuccess();
        setUpTriggerDelete();

        adIndices.rolloverAndDeleteHistoryIndex();
        verify(clusterAdminClient, times(1)).state(any(), any());
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        verify(indicesClient, times(1)).delete(any(), any());
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

        adIndices.rolloverAndDeleteHistoryIndex();
        verify(clusterAdminClient, times(1)).state(any(), any());
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        // 1 group delete, 1 separate retry for each index to delete
        verify(indicesClient, times(2)).delete(any(), any());
    }

    public void testNoCustomResultIndexFound_RolloverDefaultResultIndex_shouldSucceed() {
        setUpRolloverSuccess();
        setUpGetConfigs_withNoCustomResultIndexAlias();

        adIndices.rolloverAndDeleteHistoryIndex();
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        verify(client, times(1)).search(any(), any());
    }

    public void testCustomResultIndexFound_RolloverCustomResultIndex_withConditions_shouldSucceed() throws IOException {
        setUpGetConfigs_withCustomResultIndexAlias();

        adIndices.rolloverAndDeleteHistoryIndex();

        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        verify(client, times(1)).search(any(), any());
    }

    private void setUpGetConfigs_withNoCustomResultIndexAlias() {
        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-detectors", 1L, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS), true);
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        String detectorString = "{\"name\":\"AhtYYGWTgqkzairTchcs\",\"description\":\"iIiAVPMyFgnFlEniLbMyfJxyoGvJAl\","
            + "\"time_field\":\"HmdFH\",\"indices\":[\"ffsBF\"],\"filter_query\":{\"bool\":{\"filter\":[{\"exists\":"
            + "{\"field\":\"value\",\"boost\":1}}],\"adjust_pure_negative\":true,\"boost\":1}},\"window_delay\":"
            + "{\"period\":{\"interval\":2,\"unit\":\"Minutes\"}},\"shingle_size\":8,\"schema_version\":-512063255,"
            + "\"feature_attributes\":[{\"feature_id\":\"OTYJs\",\"feature_name\":\"eYYCM\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"XzewX\":{\"value_count\":{\"field\":\"ok\"}}}}],\"recency_emphasis\":3342,"
            + "\"history\":62,\"last_update_time\":1717192049845,\"category_field\":[\"Tcqcb\"],\"customResultIndexOrAlias\":"
            + "\"\",\"imputation_option\":{\"method\":\"FIXED_VALUES\",\"default_fill\""
            + ":[],\"integerSensitive\":false},\"suggested_seasonality\":64,\"detection_interval\":{\"period\":"
            + "{\"interval\":5,\"unit\":\"Minutes\"}},\"detector_type\":\"MULTI_ENTITY\",\"rules\":[]}";

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHit config = SearchHit.fromXContent(TestHelpers.parser(detectorString));
            SearchHits searchHits = new SearchHits(new SearchHit[] { config }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), Float.NaN);
            InternalSearchResponse response = new InternalSearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
                null,
                null,
                false,
                null,
                1
            );
            SearchResponse searchResponse = new SearchResponse(
                response,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
    }

    private void setUpRolloverSuccessForCustomIndex() {
        doAnswer(invocation -> {
            RolloverRequest request = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArgument(1);

            assertEquals("opensearch-ad-plugin-result-", request.indices()[0]);
            Map<String, Condition<?>> conditions = request.getConditions();
            assertEquals(2, conditions.size());
            assertEquals(new MaxAgeCondition(TimeValue.timeValueDays(7)), conditions.get(MaxAgeCondition.NAME));
            assertEquals(new MaxSizeCondition(new ByteSizeValue(51200, ByteSizeUnit.MB)), conditions.get(MaxSizeCondition.NAME));

            CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
            assertEquals("<opensearch-ad-plugin-result--history-{now/d}-1>", createIndexRequest.index());
            assertTrue(createIndexRequest.mappings().contains("data_start_time"));
            listener.onResponse(new RolloverResponse(null, null, Collections.emptyMap(), request.isDryRun(), true, true, true));
            return null;
        }).when(indicesClient).rolloverIndex(any(), any());
    }

    private void setUpGetConfigs_withCustomResultIndexAlias() throws IOException {
        IndexMetadata defaultResultIndex = IndexMetadata
            .builder(".opendistro-anomaly-detectors")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexMetadata customResultIndex = IndexMetadata
            .builder("opensearch-ad-plugin-result-test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(ADCommonName.CUSTOM_RESULT_INDEX_PREFIX).writeIndex(true).build())
            .build();

        clusterState = ClusterState
            .builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(defaultResultIndex, false).put(customResultIndex, false).build())
            .build();

        when(clusterService.state()).thenReturn(clusterState);

        String detectorStringWithCustomResultIndex =
            "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\","
                + "\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],\"feature_attributes\":[{\"feature_id\":\"lxYRN\","
                + "\"feature_name\":\"eqSeU\",\"feature_enabled\":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],"
                + "\"detection_interval\":{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},"
                + "\"window_delay\":{\"period\":{\"interval\":973,\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,"
                + "\"ui_metadata\":{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
                + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028,"
                + "\"result_index\":\"opensearch-ad-plugin-result-\",\"result_index_min_size\":51200,\"result_index_min_age\":7}";

        AnomalyDetector parsedDetector = AnomalyDetector
            .parse(TestHelpers.parser(detectorStringWithCustomResultIndex), "id", 1L, null, null);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            setUpRolloverSuccessForCustomIndex();
            listener.onResponse(createSearchResponse(parsedDetector));
            return null;
        }).when(client).search(any(), any());

    }

    /**
     * Test that insights index rollover is included in the main rollover process.
     */
    public void testRolloverAndDeleteHistoryIndex_includesInsightsIndex() {
        // Set up flexible rollover that accepts both system and insights indices
        doAnswer(invocation -> {
            RolloverRequest request = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArgument(1);

            String alias = request.indices()[0];
            // Accept both system result index and insights index
            assertTrue(alias.equals(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS) || alias.equals(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS));

            listener.onResponse(new RolloverResponse(null, null, Collections.emptyMap(), request.isDryRun(), true, true, true));
            return null;
        }).when(indicesClient).rolloverIndex(any(), any());

        setUpGetConfigs_withNoCustomResultIndexAlias();

        // Add insights index to metadata
        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(indexMeta("opensearch-ad-plugin-insights-history-2025.10.30-000001", 1L, ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS), true);
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices.rolloverAndDeleteHistoryIndex();

        // Should rollover both system result index and insights index
        verify(indicesClient, times(2)).rolloverIndex(any(), any());
        // Note: search is not called because config index doesn't actually exist in test setup
    }

    /**
     * Test that insights index uses correct rollover pattern.
     */
    public void testInsightsIndexRolloverPattern() {
        setUpGetConfigs_withNoCustomResultIndexAlias();

        // Mock rollover to verify insights index pattern
        doAnswer(invocation -> {
            RolloverRequest request = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<RolloverResponse> listener = (ActionListener<RolloverResponse>) invocation.getArgument(1);

            String alias = request.indices()[0];
            if (alias.equals(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS)) {
                // Verify insights index rollover request
                CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
                String expectedPattern = String
                    .format(java.util.Locale.ROOT, "<%s-history-{now/d}-1>", ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS);
                assertEquals(expectedPattern, createIndexRequest.index());
                // Just verify that mappings are present (not empty)
                assertFalse(createIndexRequest.mappings().isEmpty());
            }

            listener.onResponse(new RolloverResponse(null, null, Collections.emptyMap(), request.isDryRun(), true, true, true));
            return null;
        }).when(indicesClient).rolloverIndex(any(), any());

        Metadata.Builder metaBuilder = Metadata
            .builder()
            .put(indexMeta(".opendistro-anomaly-results-history-2020.06.24-000003", 1L, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS), true)
            .put(indexMeta("opensearch-ad-plugin-insights-history-2025.10.30-000001", 1L, ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS), true);
        clusterState = ClusterState.builder(clusterName).metadata(metaBuilder.build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        adIndices.rolloverAndDeleteHistoryIndex();

        // Both system result and insights indices will be rolled over
        verify(indicesClient, times(2)).rolloverIndex(any(), any());
    }
}
