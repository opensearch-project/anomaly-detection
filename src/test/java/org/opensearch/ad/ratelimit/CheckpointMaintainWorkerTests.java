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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class CheckpointMaintainWorkerTests extends AbstractRateLimitingTest {
    ClusterService clusterService;
    CheckpointMaintainWorker cpMaintainWorker;
    CheckpointWriteWorker writeWorker;
    CheckpointMaintainRequest request;
    CheckpointMaintainRequest request2;
    List<CheckpointMaintainRequest> requests;
    CheckpointDao checkpointDao;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE.getKey(), 1).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_SECS,
                                AnomalyDetectorSettings.CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        writeWorker = mock(CheckpointWriteWorker.class);

        CacheProvider cache = mock(CacheProvider.class);
        checkpointDao = mock(CheckpointDao.class);
        String indexName = CommonName.CHECKPOINT_INDEX_NAME;
        Duration checkpointInterval = AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ;
        EntityCache entityCache = mock(EntityCache.class);
        when(cache.get()).thenReturn(entityCache);
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        when(entityCache.getForMaintainance(anyString(), anyString())).thenReturn(Optional.of(state));
        CheckPointMaintainRequestAdapter adapter = new CheckPointMaintainRequestAdapter(
            cache,
            checkpointDao,
            indexName,
            checkpointInterval,
            clock
        );

        // Integer.MAX_VALUE makes a huge heap
        cpMaintainWorker = new CheckpointMaintainWorker(
            Integer.MAX_VALUE,
            AnomalyDetectorSettings.ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            new Random(42),
            mock(ADCircuitBreakerService.class),
            threadPool,
            settings,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            clock,
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            writeWorker,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            nodeStateManager,
            adapter
        );

        request = new CheckpointMaintainRequest(Integer.MAX_VALUE, detectorId, RequestPriority.LOW, entity.getModelId(detectorId).get());
        request2 = new CheckpointMaintainRequest(Integer.MAX_VALUE, detectorId, RequestPriority.LOW, entity2.getModelId(detectorId).get());

        requests = new ArrayList<>();
        requests.add(request);
        requests.add(request2);

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();

            TimeValue value = invocation.getArgument(1);
            // since we have only 1 request each time
            long expectedExecutionPerRequestMilli = 1000 * AnomalyDetectorSettings.EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_SECS
                .getDefault(Settings.EMPTY);
            long delay = value.getMillis();
            assertTrue(delay >= expectedExecutionPerRequestMilli);
            assertTrue(delay <= expectedExecutionPerRequestMilli * 2);
            return null;
        }).when(threadPool).schedule(any(), any(), any());
    }

    public void testPutRequests() throws IOException {
        when(checkpointDao.shouldSave(any(), anyBoolean(), any(), any())).thenReturn(true);
        Map<String, Object> content = new HashMap<String, Object>();
        content.put("a", "b");
        when(checkpointDao.toIndexSource(any())).thenReturn(content);

        cpMaintainWorker.putAll(requests);

        verify(writeWorker, times(2)).putAll(any());
        verify(threadPool, times(2)).schedule(any(), any(), any());
    }

    public void testFailtoPut() throws IOException {
        when(checkpointDao.shouldSave(any(), anyBoolean(), any(), any())).thenReturn(false);

        cpMaintainWorker.putAll(requests);

        verify(writeWorker, never()).putAll(any());
        verify(threadPool, never()).schedule(any(), any(), any());
    }
}
