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
import static org.opensearch.ad.constant.CommonName.ANOMALY_RESULT_INDEX_ALIAS;

import java.io.IOException;
import java.time.Clock;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.ad.util.Throttler;
import org.opensearch.ad.util.ThrowingConsumerWrapper;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
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
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initDetectionStateIndex),
            anomalyDetectionIndices::doesDetectorStateIndexExist,
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
        bulkIndexHandler.bulkIndexAnomalyResult(null, listener);
        verify(listener, times(1)).onResponse(null);
        verify(anomalyDetectionIndices, never()).doesAnomalyDetectorIndexExist();
    }

    public void testCreateADResultIndexNotAcknowledged() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ANOMALY_RESULT_INDEX_ALIAS));
            return null;
        }).when(anomalyDetectionIndices).initAnomalyResultIndexDirectly(any());
        bulkIndexHandler.bulkIndexAnomalyResult(ImmutableList.of(mock(AnomalyResult.class)), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Creating anomaly result index with mappings call not acknowledged", exceptionCaptor.getValue().getMessage());
    }

    public void testWrongAnomalyResult() {
        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        doReturn(bulkRequestBuilder).when(client).prepareBulk();
        doReturn(true).when(anomalyDetectionIndices).doesAnomalyResultIndexExist();
        bulkIndexHandler.bulkIndexAnomalyResult(ImmutableList.of(wrongAnomalyResult(), TestHelpers.randomAnomalyDetectResult()), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to prepare request to bulk index anomaly results", exceptionCaptor.getValue().getMessage());
    }

    public void testBulkSaveException() {
        BulkRequestBuilder bulkRequestBuilder = mock(BulkRequestBuilder.class);
        doReturn(bulkRequestBuilder).when(client).prepareBulk();
        doReturn(true).when(anomalyDetectionIndices).doesAnomalyResultIndexExist();

        String testError = randomAlphaOfLength(5);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException(testError));
            return null;
        }).when(client).bulk(any(), any());

        bulkIndexHandler.bulkIndexAnomalyResult(ImmutableList.of(TestHelpers.randomAnomalyDetectResult()), listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(testError, exceptionCaptor.getValue().getMessage());
    }

    private AnomalyResult wrongAnomalyResult() {
        return new AnomalyResult(
            randomAlphaOfLength(5),
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
            null
        );
    }
}
