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

import static java.util.AbstractMap.SimpleImmutableEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.mockito.Mockito;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.RestStatus;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

import com.fasterxml.jackson.core.JsonParseException;

public class CheckpointReadWorkerTests extends AbstractRateLimitingTest {
    CheckpointReadWorker worker;

    CheckpointDao checkpoint;
    ClusterService clusterService;

    ModelState<EntityModel> state;

    CheckpointWriteWorker checkpointWriteQueue;
    ModelManager modelManager;
    EntityColdStartWorker coldstartQueue;
    ResultWriteWorker resultWriteQueue;
    AnomalyDetectionIndices anomalyDetectionIndices;
    CacheProvider cacheProvider;
    Entity entity, entity2;
    EntityCache entityCache;
    EntityFeatureRequest request, request2;

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
                                AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_CONCURRENCY,
                                AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        checkpoint = mock(CheckpointDao.class);

        Map.Entry<EntityModel, Instant> entry = new SimpleImmutableEntry<EntityModel, Instant>(state.getModel(), Instant.now());
        when(checkpoint.processGetResponse(any(), anyString())).thenReturn(Optional.of(entry));

        checkpointWriteQueue = mock(CheckpointWriteWorker.class);

        modelManager = mock(ModelManager.class);
        when(modelManager.processEntityCheckpoint(any(), any(), anyString(), anyString())).thenReturn(state);
        when(modelManager.score(any(), anyString(), any())).thenReturn(new ThresholdingResult(0, 1, 0.7));

        coldstartQueue = mock(EntityColdStartWorker.class);
        resultWriteQueue = mock(ResultWriteWorker.class);
        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);

        cacheProvider = mock(CacheProvider.class);
        entityCache = mock(EntityCache.class);
        when(cacheProvider.get()).thenReturn(entityCache);
        when(entityCache.hostIfPossible(any(), any())).thenReturn(true);

        // Integer.MAX_VALUE makes a huge heap
        worker = new CheckpointReadWorker(
            Integer.MAX_VALUE,
            AnomalyDetectorSettings.ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
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
            modelManager,
            checkpoint,
            coldstartQueue,
            resultWriteQueue,
            nodeStateManager,
            anomalyDetectionIndices,
            cacheProvider,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue
        );

        entity = Entity.createSingleAttributeEntity(detectorId, categoryField, "value");
        entity2 = Entity.createSingleAttributeEntity(detectorId, categoryField, "value2");

        request = new EntityFeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, entity, new double[] { 0 }, 0);
        request2 = new EntityFeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, entity2, new double[] { 0 }, 0);
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
                    new GetResult(
                        CommonName.CHECKPOINT_INDEX_NAME,
                        "_doc",
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
            ActionListener<MultiGetResponse> listener = invocation.getArgument(1);
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(checkpoint).batchRead(any(), any());

        when(entityCache.hostIfPossible(any(), any())).thenReturn(config.canHostModel);

        state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(config.fullModel).build());
        when(modelManager.processEntityCheckpoint(any(), any(), anyString(), anyString())).thenReturn(state);

        List<EntityFeatureRequest> requests = new ArrayList<>();
        requests.add(request);
        worker.putAll(requests);
    }

    public void testRegular() {
        regularTestSetUp(new RegularSetUpConfig.Builder().build());

        verify(resultWriteQueue, times(1)).put(any());
        verify(checkpointWriteQueue, never()).write(any(), anyBoolean(), any());
    }

    public void testCannotLoadModel() {
        regularTestSetUp(new RegularSetUpConfig.Builder().canHostModel(false).build());

        verify(resultWriteQueue, times(1)).put(any());
        verify(checkpointWriteQueue, times(1)).write(any(), anyBoolean(), any());
    }

    public void testNoFullModel() {
        regularTestSetUp(new RegularSetUpConfig.Builder().fullModel(false).build());
        verify(resultWriteQueue, never()).put(any());
        verify(checkpointWriteQueue, never()).write(any(), anyBoolean(), any());
    }

    public void testIndexNotFound() {
        doAnswer(invocation -> {
            MultiGetItemResponse[] items = new MultiGetItemResponse[1];
            items[0] = new MultiGetItemResponse(
                null,
                new MultiGetResponse.Failure(
                    CommonName.CHECKPOINT_INDEX_NAME,
                    "_doc",
                    entity.getModelId(detectorId).get(),
                    new IndexNotFoundException(CommonName.CHECKPOINT_INDEX_NAME)
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
                        CommonName.CHECKPOINT_INDEX_NAME,
                        "_doc",
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
                        CommonName.CHECKPOINT_INDEX_NAME,
                        "_doc",
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

        List<EntityFeatureRequest> requests = new ArrayList<>();
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
                    new GetResult(
                        CommonName.CHECKPOINT_INDEX_NAME,
                        "_doc",
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
                        CommonName.CHECKPOINT_INDEX_NAME,
                        "_doc",
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

        List<EntityFeatureRequest> requests = new ArrayList<>();
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
                        CommonName.CHECKPOINT_INDEX_NAME,
                        "_doc",
                        entity.getModelId(detectorId).get(),
                        new OpenSearchStatusException("blah", RestStatus.REQUEST_TIMEOUT)
                    )
                );
                items[1] = new MultiGetItemResponse(
                    null,
                    new MultiGetResponse.Failure(
                        CommonName.CHECKPOINT_INDEX_NAME,
                        "_doc",
                        entity2.getModelId(detectorId).get(),
                        new OpenSearchStatusException("blah", RestStatus.CONFLICT)
                    )
                );
                retried.set(true);
            } else {
                items[0] = new MultiGetItemResponse(
                    new GetResponse(
                        new GetResult(
                            CommonName.CHECKPOINT_INDEX_NAME,
                            "_doc",
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
                            CommonName.CHECKPOINT_INDEX_NAME,
                            "_doc",
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

        List<EntityFeatureRequest> requests = new ArrayList<>();
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
                    CommonName.CHECKPOINT_INDEX_NAME,
                    "_doc",
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
                    CommonName.CHECKPOINT_INDEX_NAME,
                    "_doc",
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
}
