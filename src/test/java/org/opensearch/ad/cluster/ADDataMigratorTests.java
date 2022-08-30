/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *//*
    
    
    package org.opensearch.ad.cluster;
    
    import static org.mockito.ArgumentMatchers.any;
    import static org.mockito.ArgumentMatchers.anyBoolean;
    import static org.mockito.Mockito.doAnswer;
    import static org.mockito.Mockito.doReturn;
    import static org.mockito.Mockito.mock;
    import static org.mockito.Mockito.never;
    import static org.mockito.Mockito.spy;
    import static org.mockito.Mockito.times;
    import static org.mockito.Mockito.verify;
    import static org.mockito.Mockito.when;
    import static org.opensearch.ad.constant.CommonName.DETECTION_STATE_INDEX;
    
    import org.apache.lucene.search.TotalHits;
    import org.junit.Before;
    import org.junit.Ignore;
    import org.opensearch.ResourceAlreadyExistsException;
    import org.opensearch.action.ActionListener;
    import org.opensearch.action.NoShardAvailableActionException;
    import org.opensearch.action.admin.indices.create.CreateIndexResponse;
    import org.opensearch.action.get.GetResponse;
    import org.opensearch.action.index.IndexResponse;
    import org.opensearch.action.search.SearchResponse;
    import org.opensearch.action.search.ShardSearchFailure;
    import org.opensearch.ad.ADUnitTestCase;
    import org.opensearch.ad.TestHelpers;
    import org.opensearch.ad.indices.AnomalyDetectionIndices;
    import org.opensearch.client.Client;
    import org.opensearch.cluster.node.DiscoveryNode;
    import org.opensearch.cluster.service.ClusterService;
    import org.opensearch.common.xcontent.NamedXContentRegistry;
    import org.opensearch.common.xcontent.XContentParser;
    import org.opensearch.index.IndexNotFoundException;
    import org.opensearch.index.shard.ShardId;
    import org.opensearch.search.SearchHit;
    import org.opensearch.search.SearchHits;
    import org.opensearch.search.aggregations.InternalAggregations;
    import org.opensearch.search.internal.InternalSearchResponse;
    
    public class ADDataMigratorTests extends ADUnitTestCase {
     private Client client;
     private ClusterService clusterService;
     private NamedXContentRegistry namedXContentRegistry;
     private AnomalyDetectionIndices detectionIndices;
    //    private ADDataMigrator adDataMigrator;
     private String detectorId;
     private String taskId;
     private String detectorContent;
     private String jobContent;
     private String indexResponseContent;
     private String internalError;
    
     @Override
     @Before
     public void setUp() throws Exception {
         super.setUp();
         client = mock(Client.class);
         clusterService = mock(ClusterService.class);
         namedXContentRegistry = TestHelpers.xContentRegistry();
         detectionIndices = mock(AnomalyDetectionIndices.class);
         detectorId = randomAlphaOfLength(10);
         taskId = randomAlphaOfLength(10);
         detectorContent = "{\"_index\":\".opendistro-anomaly-detectors\",\"_type\":\"_doc\",\"_id\":\""
             + detectorId
             + "\",\"_version\":1,\"_seq_no\":1,\"_primary_term\":51,\"found\":true,\"_source\":{\"name\":\"old_r3\","
             + "\"description\":\"nab_ec2_cpu_utilization_24ae8d\",\"time_field\":\"timestamp\",\"indices\":"
             + "[\"nab_ec2_cpu_utilization_24ae8d\"],\"filter_query\":{\"match_all\":{\"boost\":1}},"
             + "\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"window_delay\":"
             + "{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"shingle_size\":8,\"schema_version\":0,"
             + "\"feature_attributes\":[{\"feature_id\":\"-nTqeXsBxGq4rqj0VvQy\",\"feature_name\":\"F1\","
             + "\"feature_enabled\":true,\"aggregation_query\":{\"f_1\":{\"sum\":{\"field\":\"value\"}}}}],"
             + "\"last_update_time\":1629838005821,\"detector_type\":\"REALTIME_SINGLE_ENTITY\"}}";
         jobContent = "{\"_index\":\".opendistro-anomaly-detector-jobs\",\"_type\":\"_doc\",\"_id\":\""
             + detectorId
             + "\",\"_score\":1,\"_source\":{\"name\":\""
             + detectorId
             + "\",\"schedule\":{\"interval\":{\"start_time\":1629838017881,\"period\":1,\"unit\":\"Minutes\"}},"
             + "\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"enabled\":true,"
             + "\"enabled_time\":1629838017881,\"last_update_time\":1629841634355,\"lock_duration_seconds\":60,"
             + "\"disabled_time\":1629841634355}}";
         indexResponseContent = "{\"_index\":\".opendistro-anomaly-detection-state\",\"_type\":\"_doc\",\"_id\":\""
             + taskId
             + "\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},"
             + "\"_seq_no\":0,\"_primary_term\":1}";
         internalError = "{\"_index\":\".opendistro-anomaly-detection-state\",\"_type\":\"_doc\",\"_id\":"
             + "\""
             + detectorId
             + "\",\"_version\":1,\"_seq_no\":10,\"_primary_term\":2,\"found\":true,"
             + "\"_source\":{\"last_update_time\":1629860362885,\"error\":\"test error\"}}";
    
         adDataMigrator = spy(new ADDataMigrator(client, clusterService, namedXContentRegistry, detectionIndices));
     }
    
     public void testMigrateDataWithNullJobResponse() {
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(true);
    
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).when(client).search(any(), any());
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, never()).backfillRealtimeTask(any(), anyBoolean());
     }
    
     public void testMigrateDataWithInitingDetectionStateIndexFailure() {
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(false);
    
         doAnswer(invocation -> {
             ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
             listener.onFailure(new RuntimeException("test"));
             return null;
         }).when(detectionIndices).initDetectionStateIndex(any());
    
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).when(client).search(any(), any());
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, never()).migrateDetectorInternalStateToRealtimeTask();
     }
    
     public void testMigrateDataWithInitingDetectionStateIndexAlreadyExists() {
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(false);
    
         doAnswer(invocation -> {
             ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
             listener.onFailure(new ResourceAlreadyExistsException("test"));
             return null;
         }).when(detectionIndices).initDetectionStateIndex(any());
    
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).when(client).search(any(), any());
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, times(1)).migrateDetectorInternalStateToRealtimeTask();
     }
    
     public void testMigrateDataWithInitingDetectionStateIndexNotAcknowledged() {
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(false);
    
         doAnswer(invocation -> {
             ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
             listener.onResponse(new CreateIndexResponse(false, false, DETECTION_STATE_INDEX));
             return null;
         }).when(detectionIndices).initDetectionStateIndex(any());
    
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).when(client).search(any(), any());
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, never()).migrateDetectorInternalStateToRealtimeTask();
     }
    
     public void testMigrateDataWithInitingDetectionStateIndexAcknowledged() {
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(false);
    
         doAnswer(invocation -> {
             ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
             listener.onResponse(new CreateIndexResponse(true, false, DETECTION_STATE_INDEX));
             return null;
         }).when(detectionIndices).initDetectionStateIndex(any());
    
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).when(client).search(any(), any());
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, times(1)).migrateDetectorInternalStateToRealtimeTask();
     }
    
     public void testMigrateDataWithEmptyJobResponse() {
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(true);
    
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
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
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, never()).backfillRealtimeTask(any(), anyBoolean());
     }
    
     public void testMigrateDataWithNormalJobResponseButMissingDetector() {
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(true);
    
         doAnswer(invocation -> {
             // Return correct AD job when search job index
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             String detectorId = randomAlphaOfLength(10);
             SearchHit job = SearchHit.fromXContent(TestHelpers.parser(jobContent));
             SearchHits searchHits = new SearchHits(new SearchHit[] { job }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), Float.NaN);
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
         }).doAnswer(invocation -> {
             // Return null when search realtime tasks
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).when(client).search(any(), any());
    
         // Return null when get detector and internal error from index.
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).when(client).get(any(), any());
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, times(2)).backfillRealtimeTask(any(), anyBoolean());
         verify(client, never()).index(any(), any());
     }
    
     public void testMigrateDataWithNormalJobResponseAndExistingDetector() {
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(true);
    
         String detectorId = randomAlphaOfLength(10);
         doAnswer(invocation -> {
             // Return correct AD job when search job index
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             SearchHit job1 = SearchHit.fromXContent(TestHelpers.parser(jobContent));
             SearchHits searchHits = new SearchHits(new SearchHit[] { job1 }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), Float.NaN);
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
         }).doAnswer(invocation -> {
             // Return null when search realtime tasks
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).when(client).search(any(), any());
    
         doAnswer(invocation -> {
             // Return null when get detector internal error from index.
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).doAnswer(invocation -> {
             // Return correct detector when get detector index.
             ActionListener<GetResponse> listener = invocation.getArgument(1);
             XContentParser parser = TestHelpers.parser(detectorContent, false);
             GetResponse getResponse = GetResponse.fromXContent(parser);
             listener.onResponse(getResponse);
             return null;
         }).when(client).get(any(), any());
    
         doAnswer(invocation -> {
             ActionListener<IndexResponse> listener = invocation.getArgument(1);
             String taskId = randomAlphaOfLength(5);
             IndexResponse indexResponse = IndexResponse.fromXContent(TestHelpers.parser(indexResponseContent, false));
             listener.onResponse(indexResponse);
             return null;
         }).when(client).index(any(), any());
         DiscoveryNode localNode = createNode("localNodeId");
         doReturn(localNode).when(clusterService).localNode();
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, times(2)).backfillRealtimeTask(any(), anyBoolean());
         verify(client, times(1)).index(any(), any());
     }
    
     public void testMigrateDataWithNormalJobResponse_ExistingDetector_ExistingInternalError() {
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(true);
    
         String detectorId = randomAlphaOfLength(10);
         doAnswer(invocation -> {
             // Return correct AD job when search job index
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             SearchHit job1 = SearchHit.fromXContent(TestHelpers.parser(jobContent));
             SearchHits searchHits = new SearchHits(new SearchHit[] { job1 }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), Float.NaN);
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
         }).doAnswer(invocation -> {
             // Return null when search realtime tasks
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onResponse(null);
             return null;
         }).when(client).search(any(), any());
    
         doAnswer(invocation -> {
             // Return null when get detector internal error from index.
             ActionListener<GetResponse> listener = invocation.getArgument(1);
             XContentParser parser = TestHelpers.parser(internalError, false);
             GetResponse getResponse = GetResponse.fromXContent(parser);
             listener.onResponse(getResponse);
             return null;
         }).doAnswer(invocation -> {
             // Return correct detector when get detector index.
             ActionListener<GetResponse> listener = invocation.getArgument(1);
             XContentParser parser = TestHelpers.parser(detectorContent, false);
             GetResponse getResponse = GetResponse.fromXContent(parser);
             listener.onResponse(getResponse);
             return null;
         }).when(client).get(any(), any());
    
         doAnswer(invocation -> {
             ActionListener<IndexResponse> listener = invocation.getArgument(1);
             String taskId = randomAlphaOfLength(5);
             IndexResponse indexResponse = IndexResponse.fromXContent(TestHelpers.parser(indexResponseContent, false));
             listener.onResponse(indexResponse);
             return null;
         }).when(client).index(any(), any());
         DiscoveryNode localNode = createNode("localNodeId");
         doReturn(localNode).when(clusterService).localNode();
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, times(2)).backfillRealtimeTask(any(), anyBoolean());
         verify(client, times(1)).index(any(), any());
     }
    
     public void testMigrateDataTwice() {
         adDataMigrator.migrateData();
         adDataMigrator.migrateData();
         verify(detectionIndices, times(1)).doesAnomalyDetectorJobIndexExist();
     }
    
     public void testMigrateDataWithNoAvailableShardsException() {
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener
                 .onFailure(
                     new NoShardAvailableActionException(ShardId.fromString("[.opendistro-anomaly-detector-jobs][1]"), "all shards failed")
                 );
             return null;
         }).when(client).search(any(), any());
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(true);
    
         adDataMigrator.migrateData();
         assertFalse(adDataMigrator.isMigrated());
     }
    
     public void testMigrateDataWithIndexNotFoundException() {
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onFailure(new IndexNotFoundException(ANOMALY_DETECTOR_JOB_INDEX));
             return null;
         }).when(client).search(any(), any());
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(true);
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, never()).backfillRealtimeTask(any(), anyBoolean());
         assertTrue(adDataMigrator.isMigrated());
     }
    
     public void testMigrateDataWithUnknownException() {
         doAnswer(invocation -> {
             ActionListener<SearchResponse> listener = invocation.getArgument(1);
             listener.onFailure(new RuntimeException("test unknown exception"));
             return null;
         }).when(client).search(any(), any());
         when(detectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);
         when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(true);
    
         adDataMigrator.migrateData();
         verify(adDataMigrator, never()).backfillRealtimeTask(any(), anyBoolean());
         assertTrue(adDataMigrator.isMigrated());
     }
    }
    */
