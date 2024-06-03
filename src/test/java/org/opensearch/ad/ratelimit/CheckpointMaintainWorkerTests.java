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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ratelimit.CheckPointMaintainRequestAdapter;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class CheckpointMaintainWorkerTests extends AbstractRateLimitingTest {
    ClusterService clusterService;
    ADCheckpointMaintainWorker cpMaintainWorker;
    ADCheckpointWriteWorker writeWorker;
    CheckpointMaintainRequest request;
    CheckpointMaintainRequest request2;
    List<CheckpointMaintainRequest> requests;
    ADCheckpointDao checkpointDao;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE.getKey(), 1).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS,
                                AnomalyDetectorSettings.AD_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
                                AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        writeWorker = mock(ADCheckpointWriteWorker.class);

        ADCacheProvider adCacheProvider = new ADCacheProvider();

        ADPriorityCache cache = mock(ADPriorityCache.class);
        checkpointDao = mock(ADCheckpointDao.class);
        String indexName = ADCommonName.CHECKPOINT_INDEX_NAME;
        Setting<TimeValue> checkpointInterval = AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ;

        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        when(cache.getForMaintainance(anyString(), anyString())).thenReturn(Optional.of(state));
        adCacheProvider.set(cache);
        CheckPointMaintainRequestAdapter<ThresholdedRandomCutForest, ADIndex, ADIndexManagement, ADCheckpointDao, ADPriorityCache> adapter =
            new CheckPointMaintainRequestAdapter<>(
                checkpointDao,
                indexName,
                checkpointInterval,
                clock,
                clusterService,
                settings,
                adCacheProvider
            );

        // Integer.MAX_VALUE makes a huge heap
        cpMaintainWorker = new ADCheckpointMaintainWorker(
            Integer.MAX_VALUE,
            TimeSeriesSettings.CHECKPOINT_MAINTAIN_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            new Random(42),
            mock(CircuitBreakerService.class),
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            clock,
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            writeWorker,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            nodeStateManager,
            adapter::convert
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
            long expectedExecutionPerRequestMilli = AnomalyDetectorSettings.AD_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS
                .getDefault(Settings.EMPTY);
            long delay = value.getMillis();
            assertTrue(delay == expectedExecutionPerRequestMilli);
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
