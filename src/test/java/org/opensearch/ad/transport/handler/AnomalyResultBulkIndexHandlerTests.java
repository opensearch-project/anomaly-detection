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

package org.opensearch.ad.transport.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.constant.CommonName.ANOMALY_RESULT_INDEX_ALIAS;

import java.io.IOException;
import java.time.Clock;

import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.ad.util.Throttler;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.threadpool.ThreadPool;

import com.google.common.collect.ImmutableList;

public class AnomalyResultBulkIndexHandlerTests extends ADUnitTestCase {

    private AnomalyResultBulkIndexHandler bulkIndexHandler;
    private Client client;
    private IndexUtils indexUtils;
    private ActionListener<BulkResponse> listener;
    private AnomalyDetectionIndices anomalyDetectionIndices;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        client = mock(Client.class);
        Settings settings = Settings.EMPTY;
        Clock clock = mock(Clock.class);
        Throttler throttler = new Throttler(clock);
        ThreadPool threadpool = mock(ThreadPool.class);
        ClientUtil clientUtil = new ClientUtil(Settings.EMPTY, client, throttler, threadpool);
        indexUtils = mock(IndexUtils.class);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        bulkIndexHandler = new AnomalyResultBulkIndexHandler(
            client,
            settings,
            threadPool,
            clientUtil,
            indexUtils,
            clusterService,
            anomalyDetectionIndices
        );
        listener = spy(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });
    }

    public void testNullAnomalyResults() {
        bulkIndexHandler.bulkIndexAnomalyResult(null, null, listener);
        verify(listener, times(1)).onResponse(null);
        verify(anomalyDetectionIndices, never()).doesAnomalyDetectorIndexExist();
    }

    public void testAnomalyResultBulkIndexHandler_IndexNotExist() {
        when(anomalyDetectionIndices.doesIndexExist("testIndex")).thenReturn(false);
        AnomalyResult anomalyResult = mock(AnomalyResult.class);
        when(anomalyResult.getDetectorId()).thenReturn("testId");

        bulkIndexHandler.bulkIndexAnomalyResult("testIndex", ImmutableList.of(anomalyResult), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Can't find result index testIndex", exceptionCaptor.getValue().getMessage());
    }

    public void testAnomalyResultBulkIndexHandler_InValidResultIndexMapping() {
        when(anomalyDetectionIndices.doesIndexExist("testIndex")).thenReturn(true);
        when(anomalyDetectionIndices.isValidResultIndexMapping("testIndex")).thenReturn(false);
        AnomalyResult anomalyResult = mock(AnomalyResult.class);
        when(anomalyResult.getDetectorId()).thenReturn("testId");

        bulkIndexHandler.bulkIndexAnomalyResult("testIndex", ImmutableList.of(anomalyResult), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("wrong index mapping of custom AD result index", exceptionCaptor.getValue().getMessage());
    }

    public void testAnomalyResultBulkIndexHandler_FailBulkIndexAnomaly() throws IOException {
        when(anomalyDetectionIndices.doesIndexExist("testIndex")).thenReturn(true);
        when(anomalyDetectionIndices.isValidResultIndexMapping("testIndex")).thenReturn(true);
        AnomalyResult anomalyResult = mock(AnomalyResult.class);
        when(anomalyResult.getDetectorId()).thenReturn("testId");
        when(anomalyResult.toXContent(any(), any())).thenThrow(new RuntimeException());

        bulkIndexHandler.bulkIndexAnomalyResult("testIndex", ImmutableList.of(anomalyResult), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to prepare request to bulk index anomaly results", exceptionCaptor.getValue().getMessage());
    }

    public void testCreateADResultIndexNotAcknowledged() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ANOMALY_RESULT_INDEX_ALIAS));
            return null;
        }).when(anomalyDetectionIndices).initDefaultAnomalyResultIndexDirectly(any());
        bulkIndexHandler.bulkIndexAnomalyResult(null, ImmutableList.of(mock(AnomalyResult.class)), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Creating anomaly result index with mappings call not acknowledged", exceptionCaptor.getValue().getMessage());
    }

    public void testWrongAnomalyResult() {
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client, BulkAction.INSTANCE);
        doReturn(bulkRequestBuilder).when(client).prepareBulk();
        doReturn(true).when(anomalyDetectionIndices).doesDefaultAnomalyResultIndexExist();
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            BulkItemResponse[] bulkItemResponses = new BulkItemResponse[2];
            String indexName = ANOMALY_RESULT_INDEX_ALIAS;
            String type = "_doc";
            String idPrefix = "id";
            String uuid = "uuid";
            int shardIntId = 0;
            ShardId shardId = new ShardId(new Index(indexName, uuid), shardIntId);
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(
                ANOMALY_RESULT_INDEX_ALIAS,
                randomAlphaOfLength(5),
                new VersionConflictEngineException(new ShardId(ANOMALY_RESULT_INDEX_ALIAS, "", 1), "id", "test")
            );
            bulkItemResponses[0] = new BulkItemResponse(0, randomFrom(DocWriteRequest.OpType.values()), failure);
            bulkItemResponses[1] = new BulkItemResponse(
                1,
                randomFrom(DocWriteRequest.OpType.values()),
                new IndexResponse(shardId, idPrefix + 1, 1, 1, randomInt(), true)
            );
            BulkResponse bulkResponse = new BulkResponse(bulkItemResponses, 10);
            listener.onResponse(bulkResponse);
            return null;
        }).when(client).bulk(any(), any());
        bulkIndexHandler
            .bulkIndexAnomalyResult(null, ImmutableList.of(wrongAnomalyResult(), TestHelpers.randomAnomalyDetectResult()), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("VersionConflictEngineException"));
    }

    public void testBulkSaveException() {
        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        doReturn(bulkRequestBuilder).when(client).prepareBulk();
        doReturn(true).when(anomalyDetectionIndices).doesDefaultAnomalyResultIndexExist();

        String testError = randomAlphaOfLength(5);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException(testError));
            return null;
        }).when(client).bulk(any(), any());

        bulkIndexHandler.bulkIndexAnomalyResult(null, ImmutableList.of(TestHelpers.randomAnomalyDetectResult()), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(testError, exceptionCaptor.getValue().getMessage());
    }

    private AnomalyResult wrongAnomalyResult() {
        return new AnomalyResult(
            randomAlphaOfLength(5),
            null,
            randomDouble(),
            randomDouble(),
            randomDouble(),
            null,
            null,
            null,
            null,
            null,
            randomAlphaOfLength(5),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            randomDoubleBetween(1.1, 10.0, true)
        );
    }
}
