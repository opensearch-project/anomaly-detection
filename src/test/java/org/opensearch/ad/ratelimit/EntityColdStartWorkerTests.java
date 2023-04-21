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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.ml.EntityColdStarter;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKClusterService.SDKClusterSettings;

public class EntityColdStartWorkerTests extends AbstractRateLimitingTest {
    SDKClusterService clusterService;
    EntityColdStartWorker worker;
    EntityColdStarter entityColdStarter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(SDKClusterService.class);
        SDKClusterSettings clusterSettings = spy(
            clusterService.new SDKClusterSettings(
                Settings.EMPTY, Collections
                    .unmodifiableSet(
                        new HashSet<>(
                            Arrays
                                .asList(
                                    AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
                                    AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_CONCURRENCY
                                )
                        )
                    )
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        doNothing().when(clusterSettings).addSettingsUpdateConsumer(any(Setting.class), any(Consumer.class));

        entityColdStarter = mock(EntityColdStarter.class);

        // Integer.MAX_VALUE makes a huge heap
        worker = new EntityColdStartWorker(
            Integer.MAX_VALUE,
            AnomalyDetectorSettings.ENTITY_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
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
            entityColdStarter,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            nodeStateManager
        );
    }

    public void testEmptyModelId() {
        EntityRequest request = mock(EntityRequest.class);
        when(request.getPriority()).thenReturn(RequestPriority.LOW);
        when(request.getModelId()).thenReturn(Optional.empty());
        worker.put(request);
        verify(entityColdStarter, never()).trainModel(any(), anyString(), any(), any());
        verify(request, times(1)).getModelId();
    }

    public void testOverloaded() {
        EntityRequest request = new EntityRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, entity);

        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(3);
            listener.onFailure(new OpenSearchRejectedExecutionException("blah", true));

            return null;
        }).when(entityColdStarter).trainModel(any(), anyString(), any(), any());

        worker.put(request);

        verify(entityColdStarter, times(1)).trainModel(any(), anyString(), any(), any());
        verify(nodeStateManager, times(1)).setException(eq(detectorId), any(OpenSearchRejectedExecutionException.class));

        // 2nd put request won't trigger anything as we are in cooldown mode
        worker.put(request);
        verify(entityColdStarter, times(1)).trainModel(any(), anyString(), any(), any());
    }

    public void testException() {
        EntityRequest request = new EntityRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, entity);

        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(3);
            listener.onFailure(new OpenSearchStatusException("blah", RestStatus.REQUEST_TIMEOUT));

            return null;
        }).when(entityColdStarter).trainModel(any(), anyString(), any(), any());

        worker.put(request);

        verify(entityColdStarter, times(1)).trainModel(any(), anyString(), any(), any());
        verify(nodeStateManager, times(1)).setException(eq(detectorId), any(OpenSearchStatusException.class));

        // 2nd put request triggers another setException
        worker.put(request);
        verify(entityColdStarter, times(2)).trainModel(any(), anyString(), any(), any());
        verify(nodeStateManager, times(2)).setException(eq(detectorId), any(OpenSearchStatusException.class));
    }
}
