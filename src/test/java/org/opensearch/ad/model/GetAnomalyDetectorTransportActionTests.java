/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.ad.transport.GetAnomalyDetectorTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.transport.TransportService;

public class GetAnomalyDetectorTransportActionTests extends AbstractTimeSeriesTest {
    @SuppressWarnings("unchecked")
    public void testRealtimeTaskAssignedWithSingleStreamRealTimeTaskName() throws Exception {
        // Arrange
        String configID = "test-config-id";

        // Create a task with singleStreamRealTimeTaskName
        Map<String, ADTask> tasks = new HashMap<>();
        ADTask adTask = ADTask.builder().taskType(ADTaskType.HISTORICAL.name()).build();
        tasks.put(ADTaskType.HISTORICAL.name(), adTask);

        // Mock taskManager to return the tasks
        ADTaskManager taskManager = mock(ADTaskManager.class);
        doAnswer(invocation -> {
            List<ADTask> taskList = new ArrayList<>(tasks.values());
            ((Consumer<List<ADTask>>) invocation.getArguments()[4]).accept(taskList);
            return null;
        }).when(taskManager).getAndExecuteOnLatestTasks(anyString(), any(), any(), any(), any(), any(), anyBoolean(), anyInt(), any());

        // Mock listener
        ActionListener<GetAnomalyDetectorResponse> listener = mock(ActionListener.class);

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(settings);
        GetAnomalyDetectorTransportAction getForecaster = spy(
            new GetAnomalyDetectorTransportAction(
                mock(TransportService.class),
                null,
                mock(ActionFilters.class),
                clusterService,
                null,
                null,
                Settings.EMPTY,
                null,
                taskManager,
                null
            )
        );

        // Act
        GetConfigRequest request = new GetConfigRequest(configID, 0L, true, true, "", "", true, null);
        getForecaster.getExecute(request, listener);

        // Assert
        // Verify that realtimeTask is assigned using singleStreamRealTimeTaskName
        // This can be checked by verifying interactions or internal state
        // For this example, we'll verify that the correct task is passed to getConfigAndJob
        verify(getForecaster).getConfigAndJob(eq(configID), anyBoolean(), anyBoolean(), any(), eq(Optional.of(adTask)), eq(listener));
    }

    @SuppressWarnings("unchecked")
    public void testInvalidTaskName() throws Exception {
        // Arrange
        String configID = "test-config-id";

        // Create a task with singleStreamRealTimeTaskName
        Map<String, ADTask> tasks = new HashMap<>();
        String invalidTaskName = "blah";
        ADTask adTask = ADTask.builder().taskType(invalidTaskName).build();
        tasks.put(invalidTaskName, adTask);

        // Mock taskManager to return the tasks
        ADTaskManager taskManager = mock(ADTaskManager.class);
        doAnswer(invocation -> {
            List<ADTask> taskList = new ArrayList<>(tasks.values());
            ((Consumer<List<ADTask>>) invocation.getArguments()[4]).accept(taskList);
            return null;
        }).when(taskManager).getAndExecuteOnLatestTasks(anyString(), any(), any(), any(), any(), any(), anyBoolean(), anyInt(), any());

        // Mock listener
        ActionListener<GetAnomalyDetectorResponse> listener = mock(ActionListener.class);

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(settings);
        GetAnomalyDetectorTransportAction getForecaster = spy(
            new GetAnomalyDetectorTransportAction(
                mock(TransportService.class),
                null,
                mock(ActionFilters.class),
                clusterService,
                null,
                null,
                Settings.EMPTY,
                null,
                taskManager,
                null
            )
        );

        // Act
        GetConfigRequest request = new GetConfigRequest(configID, 0L, true, true, "", "", true, null);
        getForecaster.getExecute(request, listener);

        // Assert
        // Verify that realtimeTask is assigned using singleStreamRealTimeTaskName
        // This can be checked by verifying interactions or internal state
        // For this example, we'll verify that the correct task is passed to getConfigAndJob
        verify(getForecaster).getConfigAndJob(eq(configID), anyBoolean(), anyBoolean(), any(), eq(Optional.empty()), eq(listener));
    }
}
