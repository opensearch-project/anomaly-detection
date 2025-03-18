/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

public class GetForecasterTransportActionTests extends AbstractTimeSeriesTest {
    @SuppressWarnings("unchecked")
    public void testRealtimeTaskAssignedWithSingleStreamRealTimeTaskName() throws Exception {
        // Arrange
        String configID = "test-config-id";

        // Create a task with singleStreamRealTimeTaskName
        Map<String, ForecastTask> tasks = new HashMap<>();
        ForecastTask forecastTask = ForecastTask.builder().taskType(ForecastTaskType.REALTIME_FORECAST_SINGLE_STREAM.name()).build();
        tasks.put(ForecastTaskType.REALTIME_FORECAST_SINGLE_STREAM.name(), forecastTask);

        // Mock taskManager to return the tasks
        ForecastTaskManager taskManager = mock(ForecastTaskManager.class);
        doAnswer(invocation -> {
            List<ForecastTask> taskList = new ArrayList<>(tasks.values());
            ((Consumer<List<ForecastTask>>) invocation.getArguments()[4]).accept(taskList);
            return null;
        }).when(taskManager).getAndExecuteOnLatestTasks(anyString(), any(), any(), any(), any(), any(), anyBoolean(), anyInt(), any());

        // Mock listener
        ActionListener<GetForecasterResponse> listener = mock(ActionListener.class);

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(settings);
        GetForecasterTransportAction getForecaster = spy(
            new GetForecasterTransportAction(
                mock(TransportService.class),
                null,
                mock(ActionFilters.class),
                clusterService,
                null,
                null,
                Settings.EMPTY,
                null,
                taskManager,
                null,
                mock(NodeClient.class)
            )
        );

        // Act
        GetConfigRequest request = new GetConfigRequest(configID, 0L, true, true, "", "", true, null);
        getForecaster.getExecute(request, listener);

        // Assert
        // Verify that realtimeTask is assigned using singleStreamRealTimeTaskName
        // This can be checked by verifying interactions or internal state
        // For this example, we'll verify that the correct task is passed to getConfigAndJob
        verify(getForecaster).getConfigAndJob(eq(configID), anyBoolean(), anyBoolean(), eq(Optional.of(forecastTask)), any(), eq(listener));
    }
}
