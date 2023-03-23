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

package org.opensearch.ad.ratelimit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.ad.transport.ADResultBulkResponse;
import org.opensearch.ad.transport.handler.MultiEntityResultHandler;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKClusterService.SDKClusterSettings;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.threadpool.ThreadPool;

public class ResultWriteWorkerTests extends AbstractRateLimitingTest {
    ResultWriteWorker resultWriteQueue;
    SDKClusterService clusterService;
    MultiEntityResultHandler resultHandler;
    AnomalyResult detectResult;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        clusterService = mock(SDKClusterService.class);
        SDKClusterSettings clusterSettings = clusterService.new SDKClusterSettings(
            Settings.EMPTY, Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.RESULT_WRITE_QUEUE_CONCURRENCY,
                                AnomalyDetectorSettings.RESULT_WRITE_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        threadPool = mock(ThreadPool.class);
        setUpADThreadPool(threadPool);

        resultHandler = mock(MultiEntityResultHandler.class);
        SDKNamedXContentRegistry mockSdkXContentRegistry = mock(SDKNamedXContentRegistry.class);
        when(mockSdkXContentRegistry.getRegistry()).thenReturn(xContentRegistry());

        resultWriteQueue = new ResultWriteWorker(
            Integer.MAX_VALUE,
            AnomalyDetectorSettings.RESULT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            new Random(42),
            mock(ADCircuitBreakerService.class),
            threadPool,
            Settings.EMPTY,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            clock,
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            AnomalyDetectorSettings.QUEUE_MAINTENANCE,
            resultHandler,
            mockSdkXContentRegistry,
            nodeStateManager,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE
        );

        detectResult = TestHelpers.randomHCADAnomalyDetectResult(0.8, Double.NaN, null);
    }

    public void testRegular() {
        List<IndexRequest> retryRequests = new ArrayList<>();

        ADResultBulkResponse resp = new ADResultBulkResponse(retryRequests);

        ADResultBulkRequest request = new ADResultBulkRequest();
        ResultWriteRequest resultWriteRequest = new ResultWriteRequest(
            Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            detectResult,
            null
        );
        request.add(resultWriteRequest);

        doAnswer(invocation -> {
            ActionListener<ADResultBulkResponse> listener = invocation.getArgument(1);
            listener.onResponse(resp);
            return null;
        }).when(resultHandler).flush(any(), any());

        resultWriteQueue.put(new ResultWriteRequest(Long.MAX_VALUE, detectorId, RequestPriority.MEDIUM, detectResult, null));

        // the request results one flush
        verify(resultHandler, times(1)).flush(any(), any());
    }

    public void testSingleRetryRequest() throws IOException {
        List<IndexRequest> retryRequests = new ArrayList<>();
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS)
                .source(detectResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            retryRequests.add(indexRequest);
        }

        ADResultBulkResponse resp = new ADResultBulkResponse(retryRequests);

        ADResultBulkRequest request = new ADResultBulkRequest();
        ResultWriteRequest resultWriteRequest = new ResultWriteRequest(
            Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            detectResult,
            null
        );
        request.add(resultWriteRequest);

        final AtomicBoolean retried = new AtomicBoolean();
        doAnswer(invocation -> {
            ActionListener<ADResultBulkResponse> listener = invocation.getArgument(1);
            if (retried.get()) {
                listener.onResponse(new ADResultBulkResponse());
            } else {
                retried.set(true);
                listener.onResponse(resp);
            }
            return null;
        }).when(resultHandler).flush(any(), any());

        resultWriteQueue.put(new ResultWriteRequest(Long.MAX_VALUE, detectorId, RequestPriority.MEDIUM, detectResult, null));

        // one flush from the original request; and one due to retry
        verify(resultHandler, times(2)).flush(any(), any());
    }

    public void testRetryException() {
        final AtomicBoolean retried = new AtomicBoolean();
        doAnswer(invocation -> {
            ActionListener<ADResultBulkResponse> listener = invocation.getArgument(1);
            if (retried.get()) {
                listener.onResponse(new ADResultBulkResponse());
            } else {
                retried.set(true);
                listener.onFailure(new OpenSearchStatusException("blah", RestStatus.REQUEST_TIMEOUT));
            }

            return null;
        }).when(resultHandler).flush(any(), any());

        resultWriteQueue.put(new ResultWriteRequest(Long.MAX_VALUE, detectorId, RequestPriority.MEDIUM, detectResult, null));
        // one flush from the original request; and one due to retry
        verify(resultHandler, times(2)).flush(any(), any());
        verify(nodeStateManager, times(1)).setException(eq(detectorId), any(OpenSearchStatusException.class));
    }

    public void testOverloaded() {
        doAnswer(invocation -> {
            ActionListener<ADResultBulkResponse> listener = invocation.getArgument(1);
            listener.onFailure(new OpenSearchRejectedExecutionException("blah", true));

            return null;
        }).when(resultHandler).flush(any(), any());

        resultWriteQueue.put(new ResultWriteRequest(Long.MAX_VALUE, detectorId, RequestPriority.MEDIUM, detectResult, null));
        // one flush from the original request; and one due to retry
        verify(resultHandler, times(1)).flush(any(), any());
        verify(nodeStateManager, times(1)).setException(eq(detectorId), any(OpenSearchRejectedExecutionException.class));
    }
}
