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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

public class ColdEntityWorkerTests extends AbstractRateLimitingTest {
    ClusterService clusterService;
    ADColdEntityWorker coldWorker;
    ADCheckpointReadWorker readWorker;
    FeatureRequest request, request2, mediumRequest;
    List<FeatureRequest> requests;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE.getKey(), 1).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS,
                                AnomalyDetectorSettings.AD_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        readWorker = mock(ADCheckpointReadWorker.class);

        // Integer.MAX_VALUE makes a huge heap
        coldWorker = new ADColdEntityWorker(
            Integer.MAX_VALUE,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
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
            readWorker,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            nodeStateManager
        );

        request = new FeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.LOW, new double[] { 0 }, 0, entity, null);
        request2 = new FeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.LOW, new double[] { 0 }, 0, entity2, null);
        mediumRequest = new FeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, new double[] { 0 }, 0, entity2, null);

        requests = new ArrayList<>();
        requests.add(request);
        requests.add(request2);
        requests.add(mediumRequest);

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();

            TimeValue value = invocation.getArgument(1);
            // since we have only 1 request each time
            long expectedExecutionPerRequestMilli = AnomalyDetectorSettings.AD_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS
                .getDefault(Settings.EMPTY);
            long delay = value.getMillis();
            assertTrue(delay == expectedExecutionPerRequestMilli);
            return null;
        }).when(threadPool).schedule(any(), any(), any());
    }

    public void testPutRequests() {
        coldWorker.putAll(requests);

        verify(readWorker, times(3)).putAll(any());
        verify(threadPool, times(3)).schedule(any(), any(), any());
    }

    /**
     * We will log a line and continue trying despite exception
     */
    public void testCheckpointReadPutException() {
        doThrow(RuntimeException.class).when(readWorker).putAll(any());
        coldWorker.putAll(requests);
        verify(readWorker, times(3)).putAll(any());
        verify(threadPool, never()).schedule(any(), any(), any());
    }

    /**
     * First, invalidRequest gets pulled out and we re-pull; Then we have schedule exception.
     * Will not schedule others anymore.
     */
    public void testScheduleException() {
        doThrow(RuntimeException.class).when(threadPool).schedule(any(), any(), any());
        coldWorker.putAll(requests);
        verify(readWorker, times(1)).putAll(any());
        verify(threadPool, times(1)).schedule(any(), any(), any());
    }

    public void testDelay() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS,
                                AnomalyDetectorSettings.AD_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        // Integer.MAX_VALUE makes a huge heap
        coldWorker = new ADColdEntityWorker(
            Integer.MAX_VALUE,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
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
            readWorker,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            nodeStateManager
        );

        coldWorker.putAll(requests);

        verify(readWorker, times(1)).putAll(any());
        verify(threadPool, never()).schedule(any(), any(), any());
    }
}
