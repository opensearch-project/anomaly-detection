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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.mockito.Mockito;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADInferencer;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.threadpool.ThreadPoolStats;
import org.opensearch.threadpool.ThreadPoolStats.Stats;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.fasterxml.jackson.core.JsonParseException;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class CheckpointReadWorkerTests extends AbstractRateLimitingTest {
    ADCheckpointReadWorker worker;

    ADCheckpointDao checkpoint;
    ClusterService clusterService;

    ModelState<ThresholdedRandomCutForest> state;

    ADCheckpointWriteWorker checkpointWriteQueue;
    ADModelManager modelManager;
    ADColdStartWorker coldstartQueue;
    ADSaveResultStrategy resultWriteStrategy;
    ADIndexManagement anomalyDetectionIndices;
    ADCacheProvider cacheProvider;
    ADPriorityCache entityCache;
    FeatureRequest request, request2, request3;
    ClusterSettings clusterSettings;
    ADStats adStats;
    ADInferencer inferencer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_CONCURRENCY,
                                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        checkpoint = mock(ADCheckpointDao.class);

        when(checkpoint.processHCGetResponse(any(), anyString(), anyString())).thenReturn(state);

        checkpointWriteQueue = mock(ADCheckpointWriteWorker.class);

        modelManager = mock(ADModelManager.class);
        when(modelManager.getResult(any(), any(), anyString(), any(), anyString())).thenReturn(new ThresholdingResult(0, 1, 0.7));

        coldstartQueue = mock(ADColdStartWorker.class);
        resultWriteStrategy = mock(ADSaveResultStrategy.class);
        anomalyDetectionIndices = mock(ADIndexManagement.class);

        cacheProvider = new ADCacheProvider();
        entityCache = mock(ADPriorityCache.class);
        cacheProvider.set(entityCache);
        when(entityCache.hostIfPossible(any(), any())).thenReturn(true);

        Map<String, TimeSeriesStat<?>> statsMap = new HashMap<String, TimeSeriesStat<?>>() {
            {
                put(StatNames.AD_MODEL_CORRUTPION_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()));
            }
        };

        adStats = new ADStats(statsMap);
        inferencer = new ADInferencer(modelManager, adStats, checkpoint, coldstartQueue, resultWriteStrategy, cacheProvider, threadPool);

        // Integer.MAX_VALUE makes a huge heap
        worker = new ADCheckpointReadWorker(
            Integer.MAX_VALUE,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
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
            modelManager,
            checkpoint,
            coldstartQueue,
            nodeStateManager,
            cacheProvider,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            inferencer
        );

        request = new FeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, new double[] { 0 }, 0, entity, null);
        request2 = new FeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, new double[] { 0 }, 0, entity2, null);
        request3 = new FeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, new double[] { 0 }, 0, entity3, null);
    }

    static class RegularSetUpConfig {
        private final boolean canHostModel;
        private final boolean fullModel;

        RegularSetUpConfig(Builder builder) {
            this.canHostModel = builder.canHostModel;
            this.fullModel = builder.fullModel;
        }

        public static class Builder {
            boolean canHostModel = true;
            boolean fullModel = true;

            Builder canHostModel(boolean canHostModel) {
                this.canHostModel = canHostModel;
                return this;
            }

            Builder fullModel(boolean fullModel) {
                this.fullModel = fullModel;
                return this;
            }

            public RegularSetUpConfig build() {
                return new RegularSetUpConfig(this);
            }
        }
    }

    private void regularTestSetUp(RegularSetUpConfig config) {
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[1];
            items[0] = new MultiGetItemResponse(
                new GetResponse(
                    new GetResult(ADCommonName.CHECKPOINT_INDEX_NAME, entity.getModelId(detectorId).get(), 1, 1, 0, true, null, null, null)
                ),
                null
            );
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        when(entityCache.hostIfPossible(any(), any())).thenReturn(config.canHostModel);

        state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(config.fullModel).build());
        when(checkpoint.processHCGetResponse(any(), anyString(), anyString())).thenReturn(state);

        // anyString won't match null. That's why we use any() at position 4 instead of anyString.
        if (config.fullModel) {
            when(modelManager.getResult(any(), any(), anyString(), any(), any())).thenReturn(new ThresholdingResult(0, 1, 1));
        } else {
            when(modelManager.getResult(any(), any(), anyString(), any(), any())).thenReturn(new ThresholdingResult(0, 0, 0));
        }

        List<FeatureRequest> requests = new ArrayList<>();
        requests.add(request);
        worker.putAll(requests);
    }

    public void testRegular() {
        regularTestSetUp(new RegularSetUpConfig.Builder().build());

        verify(resultWriteStrategy, times(1)).saveResult(any(), any(), any(), any(), anyString(), any(), any(), any());
        verify(checkpointWriteQueue, never()).write(any(), anyBoolean(), any());
    }

    public void testCannotLoadModel() {
        regularTestSetUp(new RegularSetUpConfig.Builder().canHostModel(false).build());

        verify(resultWriteStrategy, times(1)).saveResult(any(), any(), any(), any(), anyString(), any(), any(), any());
        verify(checkpointWriteQueue, times(1)).write(any(), anyBoolean(), any());
    }

    public void testNoFullModel() {
        regularTestSetUp(new RegularSetUpConfig.Builder().fullModel(false).build());
        // even though saveResult is called, the actual won't happen as the rcf score is 0
        // we have the guard condition at the beginning of saveResult method.
        verify(resultWriteStrategy, times(1)).saveResult(any(), any(), any(), any(), anyString(), any(), any(), any());
        verify(checkpointWriteQueue, never()).write(any(), anyBoolean(), any());
    }

    public void testIndexNotFound() {
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[1];
            items[0] = new MultiGetItemResponse(
                null,
                new MultiGetResponse.Failure(
                    ADCommonName.CHECKPOINT_INDEX_NAME,
                    entity.getModelId(detectorId).get(),
                    new IndexNotFoundException(ADCommonName.CHECKPOINT_INDEX_NAME)
                )
            );
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        worker.put(request);
        verify(coldstartQueue, times(1)).put(any());
        verify(entityCache, never()).hostIfPossible(any(), any());
    }

    public void testAllDocNotFound() {
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[2];
            items[0] = new MultiGetItemResponse(
                new GetResponse(
                    new GetResult(
                        ADCommonName.CHECKPOINT_INDEX_NAME,
                        entity.getModelId(detectorId).get(),
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                        0,
                        false,
                        null,
                        null,
                        null
                    )
                ),
                null
            );
            items[1] = new MultiGetItemResponse(
                new GetResponse(
                    new GetResult(
                        ADCommonName.CHECKPOINT_INDEX_NAME,
                        entity2.getModelId(detectorId).get(),
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                        0,
                        false,
                        null,
                        null,
                        null
                    )
                ),
                null
            );
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        List<FeatureRequest> requests = new ArrayList<>();
        requests.add(request);
        requests.add(request2);
        worker.putAll(requests);

        verify(coldstartQueue, times(2)).put(any());
        verify(entityCache, never()).hostIfPossible(any(), any());
    }

    public void testSingleDocNotFound() {
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[2];
            items[0] = new MultiGetItemResponse(
                new GetResponse(
                    new GetResult(ADCommonName.CHECKPOINT_INDEX_NAME, entity.getModelId(detectorId).get(), 1, 1, 0, true, null, null, null)
                ),
                null
            );
            items[1] = new MultiGetItemResponse(
                new GetResponse(
                    new GetResult(
                        ADCommonName.CHECKPOINT_INDEX_NAME,
                        entity2.getModelId(detectorId).get(),
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                        0,
                        false,
                        null,
                        null,
                        null
                    )
                ),
                null
            );
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        List<FeatureRequest> requests = new ArrayList<>();
        requests.add(request);
        requests.add(request2);
        worker.putAll(requests);
        verify(coldstartQueue, times(1)).put(any());
        verify(entityCache, times(1)).hostIfPossible(any(), any());
    }

    public void testTimeout() {
        AtomicBoolean retried = new AtomicBoolean();
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[2];
            if (!retried.get()) {
                items[0] = new MultiGetItemResponse(
                    null,
                    new MultiGetResponse.Failure(
                        ADCommonName.CHECKPOINT_INDEX_NAME,
                        entity.getModelId(detectorId).get(),
                        new OpenSearchStatusException("blah", RestStatus.REQUEST_TIMEOUT)
                    )
                );
                items[1] = new MultiGetItemResponse(
                    null,
                    new MultiGetResponse.Failure(
                        ADCommonName.CHECKPOINT_INDEX_NAME,
                        entity2.getModelId(detectorId).get(),
                        new OpenSearchStatusException("blah", RestStatus.CONFLICT)
                    )
                );
                retried.set(true);
            } else {
                items[0] = new MultiGetItemResponse(
                    new GetResponse(
                        new GetResult(
                            ADCommonName.CHECKPOINT_INDEX_NAME,
                            entity.getModelId(detectorId).get(),
                            1,
                            1,
                            0,
                            true,
                            null,
                            null,
                            null
                        )
                    ),
                    null
                );
                items[1] = new MultiGetItemResponse(
                    new GetResponse(
                        new GetResult(
                            ADCommonName.CHECKPOINT_INDEX_NAME,
                            entity2.getModelId(detectorId).get(),
                            1,
                            1,
                            0,
                            true,
                            null,
                            null,
                            null
                        )
                    ),
                    null
                );
            }

            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        List<FeatureRequest> requests = new ArrayList<>();
        requests.add(request);
        requests.add(request2);
        worker.putAll(requests);
        // two retried requests and the original putAll trigger 3 batchRead in total.
        // It is possible the two retries requests get combined into one batchRead
        verify(checkpoint, Mockito.atLeast(2)).batchRead(any(), any());
        assertTrue(retried.get());
    }

    public void testOverloadedExceptionFromResponse() {
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[1];
            items[0] = new MultiGetItemResponse(
                null,
                new MultiGetResponse.Failure(
                    ADCommonName.CHECKPOINT_INDEX_NAME,
                    entity.getModelId(detectorId).get(),
                    new OpenSearchRejectedExecutionException("blah")
                )
            );
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        worker.put(request);
        verify(coldstartQueue, never()).put(any());
        verify(entityCache, never()).hostIfPossible(any(), any());
        worker.put(request);
        // the 2nd put won't trigger batchRead as we are in cool down mode
        verify(checkpoint, times(1)).batchRead(any(), any());
    }

    public void testOverloadedExceptionFromFailure() {
        doAnswer(invocation -> {
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new OpenSearchRejectedExecutionException("blah"));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        worker.put(request);
        verify(coldstartQueue, never()).put(any());
        verify(entityCache, never()).hostIfPossible(any(), any());
        worker.put(request);
        // the 2nd put won't trigger batchRead as we are in cool down mode
        verify(checkpoint, times(1)).batchRead(any(), any());
    }

    public void testUnexpectedException() {
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[1];
            items[0] = new MultiGetItemResponse(
                null,
                new MultiGetResponse.Failure(
                    ADCommonName.CHECKPOINT_INDEX_NAME,
                    entity.getModelId(detectorId).get(),
                    new IllegalArgumentException("blah")
                )
            );
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        worker.put(request);
        verify(coldstartQueue, never()).put(any());
        verify(entityCache, never()).hostIfPossible(any(), any());
    }

    public void testRetryableException() {
        AtomicBoolean retried = new AtomicBoolean();
        doAnswer(invocation -> {
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            if (retried.get()) {
                // not retryable
                listener.onFailure(new JsonParseException(null, "blah"));
            } else {
                // retryable
                retried.set(true);
                listener.onFailure(new OpenSearchException("blah"));
            }

            return null;
        }).when(checkpoint).batchRead(any(), any());

        worker.put(request);
        verify(coldstartQueue, never()).put(any());
        verify(entityCache, never()).hostIfPossible(any(), any());
        assertTrue(retried.get());
    }

    public void testRemoveUnusedQueues() {
        // do nothing when putting a request to keep queues not empty
        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);

        worker = new ADCheckpointReadWorker(
            Integer.MAX_VALUE,
            1,
            AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
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
            modelManager,
            checkpoint,
            coldstartQueue,
            nodeStateManager,
            cacheProvider,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            inferencer
        );

        regularTestSetUp(new RegularSetUpConfig.Builder().build());

        assertTrue(!worker.isQueueEmpty());
        assertEquals(ADCheckpointReadWorker.WORKER_NAME, worker.getWorkerName());

        // make RequestQueue.expired return true
        when(clock.instant()).thenReturn(Instant.now().plusSeconds(TimeSeriesSettings.HOURLY_MAINTENANCE.getSeconds() + 1));

        // removed the expired queue
        worker.maintenance();

        assertTrue(worker.isQueueEmpty());
    }

    private void maintenanceSetup() {
        // do nothing when putting a request to keep queues not empty
        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        when(threadPool.stats()).thenReturn(new ThreadPoolStats(new ArrayList<Stats>()));
    }

    public void testSettingUpdatable() {
        maintenanceSetup();

        // can host two requests in the queue
        worker = new ADCheckpointReadWorker(
            2000,
            1,
            AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
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
            modelManager,
            checkpoint,
            coldstartQueue,
            nodeStateManager,
            cacheProvider,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            inferencer
        );

        List<FeatureRequest> requests = new ArrayList<>();
        requests.add(request);
        requests.add(request2);
        worker.putAll(requests);
        // size not exceeded, thus no effect
        worker.maintenance();
        assertTrue(!worker.isQueueEmpty());

        Settings newSettings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT.getKey(), "0.0001")
            .build();
        Settings.Builder target = Settings.builder();
        clusterSettings.updateDynamicSettings(newSettings, target, Settings.builder(), "test");
        clusterSettings.applySettings(target.build());
        // size not exceeded after changing setting
        worker.maintenance();
        assertTrue(worker.isQueueEmpty());
    }

    public void testOpenCircuitBreaker() {
        maintenanceSetup();

        CircuitBreakerService breaker = mock(CircuitBreakerService.class);
        when(breaker.isOpen()).thenReturn(true);

        worker = new ADCheckpointReadWorker(
            Integer.MAX_VALUE,
            1,
            AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            new Random(42),
            breaker,
            threadPool,
            Settings.EMPTY,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            clock,
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            modelManager,
            checkpoint,
            coldstartQueue,
            nodeStateManager,
            cacheProvider,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            inferencer
        );

        List<FeatureRequest> requests = new ArrayList<>();
        requests.add(request);
        requests.add(request2);
        worker.putAll(requests);

        // due to open circuit breaker, removed one request
        worker.maintenance();
        assertTrue(!worker.isQueueEmpty());

        // one request per batch
        Settings newSettings = Settings.builder().put(AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE.getKey(), "1").build();
        Settings.Builder target = Settings.builder();
        clusterSettings.updateDynamicSettings(newSettings, target, Settings.builder(), "test");
        clusterSettings.applySettings(target.build());

        // enable executing requests
        setUpADThreadPool(threadPool);

        // listener returns response back and trigger calls to process extra requests
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[1];
            items[0] = new MultiGetItemResponse(
                new GetResponse(
                    new GetResult(ADCommonName.CHECKPOINT_INDEX_NAME, entity.getModelId(detectorId).get(), 1, 1, 0, true, null, null, null)
                ),
                null
            );
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        // trigger request execution
        worker.put(request3);
        assertTrue(worker.isQueueEmpty());

        // two requests in the queue trigger two batches
        verify(checkpoint, times(2)).batchRead(any(), any());
    }

    public void testChangePriority() {
        assertEquals(RequestPriority.MEDIUM, request.getPriority());
        RequestPriority newPriority = RequestPriority.HIGH;
        request.setPriority(newPriority);
        assertEquals(newPriority, request.getPriority());
    }

    public void testDetectorId() {
        assertEquals(detectorId, request.getConfigId());
        String newDetectorId = "456";
        request.setDetectorId(newDetectorId);
        assertEquals(newDetectorId, request.getConfigId());
    }

    @SuppressWarnings("unchecked")
    public void testHostException() throws IOException {
        String detectorId2 = "456";
        Entity entity4 = Entity.createSingleAttributeEntity(categoryField, "value4");
        FeatureRequest request4 = new FeatureRequest(
            Integer.MAX_VALUE,
            detectorId2,
            RequestPriority.MEDIUM,
            new double[] { 0 },
            0,
            entity4,
            null
        );

        AnomalyDetector detector2 = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId2, Arrays.asList(categoryField));

        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(detector2));
            return null;
        }).when(nodeStateManager).getConfig(eq(detectorId2), eq(AnalysisType.AD), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(eq(detectorId), eq(AnalysisType.AD), any(ActionListener.class));

        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[2];
            items[0] = new MultiGetItemResponse(
                new GetResponse(
                    new GetResult(ADCommonName.CHECKPOINT_INDEX_NAME, entity.getModelId(detectorId).get(), 1, 1, 0, true, null, null, null)
                ),
                null
            );
            items[1] = new MultiGetItemResponse(
                new GetResponse(
                    new GetResult(
                        ADCommonName.CHECKPOINT_INDEX_NAME,
                        entity4.getModelId(detectorId2).get(),
                        1,
                        1,
                        0,
                        true,
                        null,
                        null,
                        null
                    )
                ),
                null
            );
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        doThrow(LimitExceededException.class).when(entityCache).hostIfPossible(eq(detector2), any());

        List<FeatureRequest> requests = new ArrayList<>();
        requests.add(request);
        requests.add(request4);
        worker.putAll(requests);
        verify(coldstartQueue, never()).put(any());
        verify(entityCache, times(2)).hostIfPossible(any(), any());

        verify(nodeStateManager, times(1)).setException(eq(detectorId2), any(LimitExceededException.class));
        verify(nodeStateManager, never()).setException(eq(detectorId), any(LimitExceededException.class));
    }

    public void testFailToScore() {
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[1];
            items[0] = new MultiGetItemResponse(
                new GetResponse(
                    new GetResult(ADCommonName.CHECKPOINT_INDEX_NAME, entity.getModelId(detectorId).get(), 1, 1, 0, true, null, null, null)
                ),
                null
            );
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        when(checkpoint.processHCGetResponse(any(), anyString(), anyString())).thenReturn(state);
        // anyString won't match null. That's why we use any() at position 2 instead of anyString.
        doThrow(new IllegalArgumentException()).when(modelManager).getResult(any(), any(), anyString(), any(), any());

        List<FeatureRequest> requests = new ArrayList<>();
        requests.add(request);
        worker.putAll(requests);

        verify(modelManager, times(1)).getResult(any(), any(), anyString(), any(), any());
        verify(resultWriteStrategy, never()).saveResult(any(), any(), any(), any(), anyString(), any(), any(), any());
        verify(checkpointWriteQueue, never()).write(any(), anyBoolean(), any());
        verify(coldstartQueue, times(1)).put(any());
        Object val = adStats.getStat(StatNames.AD_MODEL_CORRUTPION_COUNT.getName()).getValue();
        assertEquals(1L, ((Long) val).longValue());
    }
}
