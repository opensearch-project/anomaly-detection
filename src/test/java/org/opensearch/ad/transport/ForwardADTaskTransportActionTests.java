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

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.model.ADTaskAction.CANCEL;
import static org.opensearch.ad.model.ADTaskAction.CHECK_AVAILABLE_TASK_SLOTS;
import static org.opensearch.ad.model.ADTaskAction.CLEAN_STALE_RUNNING_ENTITIES;
import static org.opensearch.ad.model.ADTaskAction.NEXT_ENTITY;
import static org.opensearch.ad.model.ADTaskAction.PUSH_BACK_ENTITY;
import static org.opensearch.ad.model.ADTaskAction.SCALE_ENTITY_TASK_SLOTS;

import java.io.IOException;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

public class ForwardADTaskTransportActionTests extends ADUnitTestCase {
    private ActionFilters actionFilters;
    private TransportService transportService;
    private ADTaskManager adTaskManager;
    private ADTaskCacheManager adTaskCacheManager;
    private FeatureManager featureManager;
    private NodeStateManager stateManager;
    private ForwardADTaskTransportAction forwardADTaskTransportAction;
    private Task task;
    private ActionListener<AnomalyDetectorJobResponse> listener;

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        actionFilters = mock(ActionFilters.class);
        transportService = mock(TransportService.class);
        adTaskManager = mock(ADTaskManager.class);
        adTaskCacheManager = mock(ADTaskCacheManager.class);
        featureManager = mock(FeatureManager.class);
        stateManager = mock(NodeStateManager.class);
        forwardADTaskTransportAction = new ForwardADTaskTransportAction(
            actionFilters,
            transportService,
            adTaskManager,
            adTaskCacheManager,
            featureManager,
            stateManager
        );

        task = mock(Task.class);
        listener = mock(ActionListener.class);
    }

    public void testCheckAvailableTaskSlots() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, CHECK_AVAILABLE_TASK_SLOTS);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskManager, times(1)).checkTaskSlots(any(), any(), any(), any(), any(), any(), any());
    }

    public void testNextEntityTaskForSingleEntityDetector() throws IOException {
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(false);

        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_SINGLE_ENTITY);
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, NEXT_ENTITY);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(listener, times(1)).onFailure(any());
    }

    public void testNextEntityTaskWithNoPendingEntity() throws IOException {
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(false);

        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR);
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, NEXT_ENTITY);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(1)).setDetectorTaskSlots(anyString(), eq(0));
        verify(adTaskManager, times(1)).setHCDetectorTaskDone(any(), any(), any());
    }

    public void testNextEntityTaskWithPendingEntity() throws IOException {
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(true);

        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR);
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, NEXT_ENTITY);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskManager, times(1)).runNextEntityForHCADHistorical(any(), any(), any());
        verify(adTaskManager, times(1)).updateADHCDetectorTask(any(), any(), any());
    }

    public void testPushBackEntityForSingleEntityDetector() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_SINGLE_ENTITY);
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, PUSH_BACK_ENTITY);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(listener, times(1)).onFailure(any());
    }

    public void testPushBackEntityForNonRetryableExceptionAndNoPendingEntity() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        when(adTaskManager.convertEntityToString(any())).thenReturn(randomAlphaOfLength(5));
        when(adTaskManager.isRetryableError(any())).thenReturn(false);
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(false);
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, PUSH_BACK_ENTITY);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(1)).removeEntity(anyString(), anyString());
        verify(adTaskCacheManager, times(1)).setDetectorTaskSlots(anyString(), eq(0));
        verify(adTaskManager, times(1)).setHCDetectorTaskDone(any(), any(), any());
    }

    public void testPushBackEntityForNonRetryableExceptionAndPendingEntity() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        when(adTaskManager.convertEntityToString(any())).thenReturn(randomAlphaOfLength(5));
        when(adTaskManager.isRetryableError(any())).thenReturn(false);
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(true);
        when(adTaskCacheManager.scaleDownHCDetectorTaskSlots(anyString(), anyInt())).thenReturn(randomIntBetween(2, 10)).thenReturn(1);

        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, PUSH_BACK_ENTITY);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(1)).removeEntity(anyString(), anyString());
        verify(adTaskManager, times(0)).runNextEntityForHCADHistorical(any(), any(), any());

        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(2)).removeEntity(anyString(), anyString());
        verify(adTaskManager, times(1)).runNextEntityForHCADHistorical(any(), any(), any());
    }

    public void testPushBackEntityForRetryableExceptionAndNoPendingEntity() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        when(adTaskManager.convertEntityToString(any())).thenReturn(randomAlphaOfLength(5));
        when(adTaskManager.isRetryableError(any())).thenReturn(true);
        when(adTaskCacheManager.exceedRetryLimit(any(), any())).thenReturn(false).thenReturn(true);
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(false);

        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, PUSH_BACK_ENTITY);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(1)).pushBackEntity(anyString(), anyString(), anyString());
        verify(adTaskCacheManager, times(1)).setDetectorTaskSlots(anyString(), eq(0));
        verify(adTaskManager, times(1)).setHCDetectorTaskDone(any(), any(), any());

        forwardADTaskTransportAction.doExecute(task, request, listener);
        // will not push back entity task if exceed retry limit
        verify(adTaskCacheManager, times(1)).pushBackEntity(anyString(), anyString(), anyString());
        verify(adTaskCacheManager, times(2)).setDetectorTaskSlots(anyString(), eq(0));
        verify(adTaskManager, times(2)).setHCDetectorTaskDone(any(), any(), any());
    }

    public void testPushBackEntityForRetryableExceptionAndPendingEntity() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        when(adTaskManager.convertEntityToString(any())).thenReturn(randomAlphaOfLength(5));
        when(adTaskManager.isRetryableError(any())).thenReturn(true);
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(true);
        when(adTaskCacheManager.scaleDownHCDetectorTaskSlots(anyString(), anyInt())).thenReturn(randomIntBetween(2, 10)).thenReturn(1);

        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, PUSH_BACK_ENTITY);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(1)).pushBackEntity(anyString(), anyString(), anyString());
        verify(adTaskCacheManager, times(0)).removeEntity(anyString(), anyString());
        verify(adTaskManager, times(0)).runNextEntityForHCADHistorical(any(), any(), any());

        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(2)).pushBackEntity(anyString(), anyString(), anyString());
        verify(adTaskCacheManager, times(0)).removeEntity(anyString(), anyString());
        verify(adTaskManager, times(1)).runNextEntityForHCADHistorical(any(), any(), any());
    }

    public void testScaleEntityTaskSlotsWithNoAvailableSlots() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, null, SCALE_ENTITY_TASK_SLOTS);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, never()).scaleUpDetectorTaskSlots(anyString(), anyInt());
        verify(listener, times(1)).onResponse(any());

        request = new ForwardADTaskRequest(adTask, 0, SCALE_ENTITY_TASK_SLOTS);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, never()).scaleUpDetectorTaskSlots(anyString(), anyInt());
        verify(listener, times(2)).onResponse(any());
    }

    public void testScaleEntityTaskSlotsWithAvailableSlots() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        when(adTaskManager.detectorTaskSlotScaleDelta(anyString())).thenReturn(randomIntBetween(10, 20)).thenReturn(-1);
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, 1, SCALE_ENTITY_TASK_SLOTS);

        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(1)).setAllowedRunningEntities(anyString(), anyInt());
        verify(adTaskCacheManager, times(1)).scaleUpDetectorTaskSlots(anyString(), anyInt());
        verify(listener, times(1)).onResponse(any());

        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(1)).setAllowedRunningEntities(anyString(), anyInt());
        verify(adTaskCacheManager, times(1)).scaleUpDetectorTaskSlots(anyString(), anyInt());
        verify(listener, times(2)).onResponse(any());
    }

    public void testCancelSingleEntityDetector() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_SINGLE_ENTITY);
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, CANCEL);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(listener, times(1)).onFailure(any());
    }

    public void testCancelHCDetector() throws IOException {
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(true).thenReturn(false).thenReturn(true);
        when(adTaskManager.convertEntityToString(any())).thenReturn(randomAlphaOfLength(5));

        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, CANCEL);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(1)).clearPendingEntities(anyString());
        verify(adTaskCacheManager, times(1)).removeRunningEntity(anyString(), anyString());
        verify(listener, times(1)).onResponse(any());

        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(2)).clearPendingEntities(anyString());
        verify(adTaskCacheManager, times(2)).removeRunningEntity(anyString(), anyString());
        verify(adTaskManager, times(1)).setHCDetectorTaskDone(any(), any(), any());
        verify(listener, times(2)).onResponse(any());

        request = new ForwardADTaskRequest(TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR), CANCEL);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskCacheManager, times(3)).clearPendingEntities(anyString());
        verify(adTaskCacheManager, times(3)).removeRunningEntity(anyString(), anyString());
        verify(adTaskManager, times(2)).setHCDetectorTaskDone(any(), any(), any());
        verify(listener, times(3)).onResponse(any());
    }

    public void testCleanStaleRunningEntities() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR);
        ImmutableList<String> staleEntities = ImmutableList.of(randomAlphaOfLength(5));
        ForwardADTaskRequest request = new ForwardADTaskRequest(adTask, CLEAN_STALE_RUNNING_ENTITIES, staleEntities);
        forwardADTaskTransportAction.doExecute(task, request, listener);
        verify(adTaskManager, times(staleEntities.size())).removeStaleRunningEntity(any(), any(), any(), any());
        verify(listener, times(1)).onResponse(any());
    }
}
