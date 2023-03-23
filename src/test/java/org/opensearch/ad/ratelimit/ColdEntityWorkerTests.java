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

import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKClusterService.SDKClusterSettings;

public class ColdEntityWorkerTests extends AbstractRateLimitingTest {
    SDKClusterService clusterService;
    ColdEntityWorker coldWorker;
    CheckpointReadWorker readWorker;
    EntityFeatureRequest request, request2, invalidRequest;
    List<EntityFeatureRequest> requests;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(SDKClusterService.class);
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_BATCH_SIZE.getKey(), 1).build();
        SDKClusterSettings clusterSettings = clusterService.new SDKClusterSettings(
            settings, Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS,
                                AnomalyDetectorSettings.COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        readWorker = mock(CheckpointReadWorker.class);

        // Integer.MAX_VALUE makes a huge heap
        coldWorker = new ColdEntityWorker(
            Integer.MAX_VALUE,
            AnomalyDetectorSettings.ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
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
            readWorker,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            nodeStateManager
        );

        request = new EntityFeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.LOW, entity, new double[] { 0 }, 0);
        request2 = new EntityFeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.LOW, entity2, new double[] { 0 }, 0);
        invalidRequest = new EntityFeatureRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, entity2, new double[] { 0 }, 0);

        requests = new ArrayList<>();
        requests.add(request);
        requests.add(request2);
        requests.add(invalidRequest);

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();

            TimeValue value = invocation.getArgument(1);
            // since we have only 1 request each time
            long expectedExecutionPerRequestMilli = 1000 * AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS
                .getDefault(Settings.EMPTY);
            long delay = value.getMillis();
            assertTrue(delay >= expectedExecutionPerRequestMilli);
            assertTrue(delay <= expectedExecutionPerRequestMilli * 2);
            return null;
        }).when(threadPool).schedule(any(), any(), any());
    }

    public void testPutRequests() {
        coldWorker.putAll(requests);

        verify(readWorker, times(2)).putAll(any());
        verify(threadPool, times(2)).schedule(any(), any(), any());
    }

    /**
     * We will log a line and continue trying despite exception
     */
    public void testCheckpointReadPutException() {
        doThrow(RuntimeException.class).when(readWorker).putAll(any());
        coldWorker.putAll(requests);
        verify(readWorker, times(2)).putAll(any());
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
        SDKClusterSettings clusterSettings = clusterService.new SDKClusterSettings(
            Settings.EMPTY, Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS,
                                AnomalyDetectorSettings.COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                                AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_BATCH_SIZE
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        // Integer.MAX_VALUE makes a huge heap
        coldWorker = new ColdEntityWorker(
            Integer.MAX_VALUE,
            AnomalyDetectorSettings.ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
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
            readWorker,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            nodeStateManager
        );

        coldWorker.putAll(requests);

        verify(readWorker, times(1)).putAll(any());
        verify(threadPool, never()).schedule(any(), any(), any());
    }
}
