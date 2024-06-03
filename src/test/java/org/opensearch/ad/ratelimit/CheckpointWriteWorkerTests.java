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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkItemResponse.Failure;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class CheckpointWriteWorkerTests extends AbstractRateLimitingTest {
    ADCheckpointWriteWorker worker;

    ADCheckpointDao checkpoint;
    ClusterService clusterService;

    ModelState<ThresholdedRandomCutForest> state;

    @Override
    @SuppressWarnings("unchecked")
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
                                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
                                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        checkpoint = mock(ADCheckpointDao.class);
        Map<String, Object> checkpointMap = new HashMap<>();
        checkpointMap.put(CommonName.FIELD_MODEL, "a");
        when(checkpoint.toIndexSource(any())).thenReturn(checkpointMap);
        when(checkpoint.shouldSave(any(), anyBoolean(), any(), any())).thenReturn(true);

        // Integer.MAX_VALUE makes a huge heap
        worker = new ADCheckpointWriteWorker(
            Integer.MAX_VALUE,
            TimeSeriesSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
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
            checkpoint,
            ADCommonName.CHECKPOINT_INDEX_NAME,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            nodeStateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE
        );

        state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().build());
    }

    public void testTriggerSave() {
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            BulkItemResponse[] responses = new BulkItemResponse[1];
            ShardId shardId = new ShardId(new Index("index_name", "uuid"), 0);
            responses[0] = new BulkItemResponse(
                0,
                randomFrom(DocWriteRequest.OpType.values()),
                new IndexResponse(shardId, "id", 1, 1, 1, true)
            );
            listener.onResponse(new BulkResponse(responses, 1));

            return null;
        }).when(checkpoint).batchWrite(any(), any());

        worker.write(state, true, RequestPriority.MEDIUM);

        verify(checkpoint, times(1)).batchWrite(any(), any());
    }

    public void testTriggerSaveAll() {
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            BulkItemResponse[] responses = new BulkItemResponse[1];
            ShardId shardId = new ShardId(new Index("index_name", "uuid"), 0);
            responses[0] = new BulkItemResponse(
                0,
                randomFrom(DocWriteRequest.OpType.values()),
                new IndexResponse(shardId, "id", 1, 1, 1, true)
            );
            listener.onResponse(new BulkResponse(responses, 1));

            return null;
        }).when(checkpoint).batchWrite(any(), any());

        List<ModelState<ThresholdedRandomCutForest>> states = new ArrayList<>();
        states.add(state);
        worker.writeAll(states, detectorId, true, RequestPriority.MEDIUM);

        verify(checkpoint, times(1)).batchWrite(any(), any());
    }

    /**
     * Test that when more requests are coming than concurrency allowed, queues will be
     * auto-flushed given enough time.
     * @throws InterruptedException when thread.sleep gets interrupted
     */
    public void testTriggerAutoFlush() throws InterruptedException {
        final CountDownLatch processingLatch = new CountDownLatch(1);

        ExecutorService executorService = mock(ExecutorService.class);

        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = () -> {
                try {
                    processingLatch.await(100, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOG.error(e);
                    assertTrue("Unexpected exception", false);
                }
                Runnable toInvoke = invocation.getArgument(0);
                toInvoke.run();
            };
            // start a new thread so it won't block main test thread's execution
            new Thread(runnable).start();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        // make sure permits are released and the next request probe starts
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(checkpoint).batchWrite(any(), any());

        // Integer.MAX_VALUE makes a huge heap
        // create a worker to use mockThreadPool
        worker = new ADCheckpointWriteWorker(
            Integer.MAX_VALUE,
            TimeSeriesSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            new Random(42),
            mock(CircuitBreakerService.class),
            mockThreadPool,
            Settings.EMPTY,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            clock,
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            checkpoint,
            ADCommonName.CHECKPOINT_INDEX_NAME,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            nodeStateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE
        );

        // our concurrency is 2, so first 2 requests cause two batches. And the
        // remaining 1 stays in the queue until the 2 concurrent runs finish.
        // first 2 batch account for one checkpoint.batchWrite; the remaining one
        // calls checkpoint.batchWrite
        // CHECKPOINT_WRITE_QUEUE_BATCH_SIZE is the largest batch size
        int numberOfRequests = 2 * AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE.getDefault(Settings.EMPTY) + 1;
        for (int i = 0; i < numberOfRequests; i++) {
            ModelState<ThresholdedRandomCutForest> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().build());
            worker.write(state, true, RequestPriority.MEDIUM);
        }

        // Here, we allow the first 2 pulling batch from queue operations to start.
        processingLatch.countDown();

        // wait until queues get emptied
        int waitIntervals = 20;
        while (!worker.isQueueEmpty() && waitIntervals-- >= 0) {
            Thread.sleep(500);
        }

        assertTrue(worker.isQueueEmpty());
        // of requests cause at least one batch.
        verify(checkpoint, times(3)).batchWrite(any(), any());
    }

    public void testOverloaded() {
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            listener.onFailure(new OpenSearchRejectedExecutionException("blah", true));

            return null;
        }).when(checkpoint).batchWrite(any(), any());

        worker.write(state, true, RequestPriority.MEDIUM);

        verify(checkpoint, times(1)).batchWrite(any(), any());
        verify(nodeStateManager, times(1)).setException(eq(state.getConfigId()), any(OpenSearchRejectedExecutionException.class));
    }

    public void testRetryException() {
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            listener.onFailure(new OpenSearchStatusException("blah", RestStatus.REQUEST_TIMEOUT));

            return null;
        }).when(checkpoint).batchWrite(any(), any());

        worker.write(state, true, RequestPriority.MEDIUM);
        // we don't retry checkpoint write
        verify(checkpoint, times(1)).batchWrite(any(), any());
        verify(nodeStateManager, times(1)).setException(eq(state.getConfigId()), any(OpenSearchStatusException.class));
    }

    /**
     * Test that we don'd retry failed request
     */
    public void testFailedRequest() {
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            BulkItemResponse[] responses = new BulkItemResponse[1];
            ShardId shardId = new ShardId(new Index("index_name", "uuid"), 0);
            responses[0] = new BulkItemResponse(
                0,
                randomFrom(DocWriteRequest.OpType.values()),
                new Failure(shardId.getIndexName(), "id1", new VersionConflictEngineException(shardId, "id1", "blah"))
            );
            listener.onResponse(new BulkResponse(responses, 1));

            return null;
        }).when(checkpoint).batchWrite(any(), any());

        worker.write(state, true, RequestPriority.MEDIUM);
        // we don't retry checkpoint write
        verify(checkpoint, times(1)).batchWrite(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testEmptyTimeStamp() {
        ModelState<ThresholdedRandomCutForest> state = mock(ModelState.class);
        when(state.getLastCheckpointTime()).thenReturn(Instant.MIN);
        worker.write(state, false, RequestPriority.MEDIUM);

        verify(checkpoint, never()).batchWrite(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testTooSoonToSaveSingleWrite() {
        ModelState<ThresholdedRandomCutForest> state = mock(ModelState.class);
        when(state.getLastCheckpointTime()).thenReturn(Instant.now());
        worker.write(state, false, RequestPriority.MEDIUM);

        verify(checkpoint, never()).batchWrite(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testTooSoonToSaveWriteAll() {
        ModelState<ThresholdedRandomCutForest> state = mock(ModelState.class);
        when(state.getLastCheckpointTime()).thenReturn(Instant.now());

        List<ModelState<ThresholdedRandomCutForest>> states = new ArrayList<>();
        states.add(state);

        worker.writeAll(states, detectorId, false, RequestPriority.MEDIUM);

        verify(checkpoint, never()).batchWrite(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testEmptyModel() {
        ModelState<ThresholdedRandomCutForest> state = mock(ModelState.class);
        when(state.getLastCheckpointTime()).thenReturn(Instant.now());
        when(state.getModel()).thenReturn(null);
        worker.write(state, true, RequestPriority.MEDIUM);

        verify(checkpoint, never()).batchWrite(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testEmptyModelId() {
        ModelState<ThresholdedRandomCutForest> state = mock(ModelState.class);
        when(state.getLastCheckpointTime()).thenReturn(Instant.now());
        ThresholdedRandomCutForest model = mock(ThresholdedRandomCutForest.class);
        when(state.getModel()).thenReturn(Optional.of(model));
        when(state.getConfigId()).thenReturn("1");
        when(state.getModelId()).thenReturn(null);
        worker.write(state, true, RequestPriority.MEDIUM);

        verify(checkpoint, never()).batchWrite(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testEmptyDetectorId() {
        ModelState<ThresholdedRandomCutForest> state = mock(ModelState.class);
        when(state.getLastCheckpointTime()).thenReturn(Instant.now());
        ThresholdedRandomCutForest model = mock(ThresholdedRandomCutForest.class);
        when(state.getModel()).thenReturn(Optional.of(model));
        when(state.getConfigId()).thenReturn(null);
        when(state.getModelId()).thenReturn("a");
        worker.write(state, true, RequestPriority.MEDIUM);

        verify(checkpoint, never()).batchWrite(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testDetectorNotAvailableSingleWrite() {
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.empty());
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

        worker.write(state, true, RequestPriority.MEDIUM);
        verify(checkpoint, never()).batchWrite(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testDetectorNotAvailableWriteAll() {
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.empty());
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

        List<ModelState<ThresholdedRandomCutForest>> states = new ArrayList<>();
        states.add(state);
        worker.writeAll(states, detectorId, true, RequestPriority.MEDIUM);
        verify(checkpoint, never()).batchWrite(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testDetectorFetchException() {
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

        worker.write(state, true, RequestPriority.MEDIUM);
        verify(checkpoint, never()).batchWrite(any(), any());
    }

    public void testCheckpointNullSource() throws IOException {
        when(checkpoint.toIndexSource(any())).thenReturn(null);
        worker.write(state, true, RequestPriority.MEDIUM);
        verify(checkpoint, never()).batchWrite(any(), any());
    }

    public void testCheckpointEmptySource() throws IOException {
        Map<String, Object> checkpointMap = new HashMap<>();
        when(checkpoint.toIndexSource(any())).thenReturn(checkpointMap);
        worker.write(state, true, RequestPriority.MEDIUM);
        verify(checkpoint, never()).batchWrite(any(), any());
    }

    public void testConcurrentModificationException() throws IOException {
        doThrow(ConcurrentModificationException.class).when(checkpoint).toIndexSource(any());
        worker.write(state, true, RequestPriority.MEDIUM);
        verify(checkpoint, never()).batchWrite(any(), any());
    }
}
