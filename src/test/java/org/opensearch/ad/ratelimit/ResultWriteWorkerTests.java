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
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.ad.transport.handler.ADIndexMemoryPressureAwareResultHandler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.ResultBulkResponse;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class ResultWriteWorkerTests extends AbstractRateLimitingTest {
    ADResultWriteWorker resultWriteQueue;
    ClusterService clusterService;
    ADIndexMemoryPressureAwareResultHandler resultHandler;
    AnomalyResult detectResult;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_CONCURRENCY,
                                AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        threadPool = mock(ThreadPool.class);
        setUpADThreadPool(threadPool);

        resultHandler = mock(ADIndexMemoryPressureAwareResultHandler.class);

        resultWriteQueue = new ADResultWriteWorker(
            Integer.MAX_VALUE,
            TimeSeriesSettings.RESULT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            new Random(42),
            mock(CircuitBreakerService.class),
            threadPool,
            Settings.EMPTY,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            clock,
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            resultHandler,
            xContentRegistry(),
            nodeStateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE
        );

        detectResult = TestHelpers.randomHCADAnomalyDetectResult(0.8, Double.NaN, null);
    }

    public void testRegular() {
        List<IndexRequest> retryRequests = new ArrayList<>();

        ResultBulkResponse resp = new ResultBulkResponse(retryRequests);

        ADResultBulkRequest request = new ADResultBulkRequest();
        ADResultWriteRequest resultWriteRequest = new ADResultWriteRequest(
            Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            detectResult,
            null,
            false
        );
        request.add(resultWriteRequest);

        doAnswer(invocation -> {
            ActionListener<ResultBulkResponse> listener = invocation.getArgument(1);
            listener.onResponse(resp);
            return null;
        }).when(resultHandler).flush(any(), any());

        resultWriteQueue.put(new ADResultWriteRequest(Long.MAX_VALUE, detectorId, RequestPriority.MEDIUM, detectResult, null, false));

        // the request results one flush
        verify(resultHandler, times(1)).flush(any(), any());
    }

    public void testSingleRetryRequest() throws IOException {
        List<IndexRequest> retryRequests = new ArrayList<>();
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS)
                .source(detectResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            retryRequests.add(indexRequest);
        }

        ResultBulkResponse resp = new ResultBulkResponse(retryRequests);

        ADResultBulkRequest request = new ADResultBulkRequest();
        ADResultWriteRequest resultWriteRequest = new ADResultWriteRequest(
            Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            detectResult,
            null,
            false
        );
        request.add(resultWriteRequest);

        final AtomicBoolean retried = new AtomicBoolean();
        doAnswer(invocation -> {
            ActionListener<ResultBulkResponse> listener = invocation.getArgument(1);
            if (retried.get()) {
                listener.onResponse(new ResultBulkResponse());
            } else {
                retried.set(true);
                listener.onResponse(resp);
            }
            return null;
        }).when(resultHandler).flush(any(), any());

        resultWriteQueue.put(new ADResultWriteRequest(Long.MAX_VALUE, detectorId, RequestPriority.MEDIUM, detectResult, null, false));

        // one flush from the original request; and one due to retry
        verify(resultHandler, times(2)).flush(any(), any());
    }

    public void testRetryException() {
        final AtomicBoolean retried = new AtomicBoolean();
        doAnswer(invocation -> {
            ActionListener<ResultBulkResponse> listener = invocation.getArgument(1);
            if (retried.get()) {
                listener.onResponse(new ResultBulkResponse());
            } else {
                retried.set(true);
                listener.onFailure(new OpenSearchStatusException("blah", RestStatus.REQUEST_TIMEOUT));
            }

            return null;
        }).when(resultHandler).flush(any(), any());

        resultWriteQueue.put(new ADResultWriteRequest(Long.MAX_VALUE, detectorId, RequestPriority.MEDIUM, detectResult, null, false));
        // one flush from the original request; and one due to retry
        verify(resultHandler, times(2)).flush(any(), any());
        verify(nodeStateManager, times(1)).setException(eq(detectorId), any(OpenSearchStatusException.class));
    }

    public void testOverloaded() {
        doAnswer(invocation -> {
            ActionListener<ResultBulkResponse> listener = invocation.getArgument(1);
            listener.onFailure(new OpenSearchRejectedExecutionException("blah", true));

            return null;
        }).when(resultHandler).flush(any(), any());

        resultWriteQueue.put(new ADResultWriteRequest(Long.MAX_VALUE, detectorId, RequestPriority.MEDIUM, detectResult, null, false));
        // one flush from the original request; and one due to retry
        verify(resultHandler, times(1)).flush(any(), any());
        verify(nodeStateManager, times(1)).setException(eq(detectorId), any(OpenSearchRejectedExecutionException.class));
    }
}
