/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.task;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.forecast.settings.ForecastSettings.DELETE_FORECAST_RESULT_WHEN_DELETE_FORECASTER;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_OLD_TASK_DOCS_PER_FORECASTER;
import static org.opensearch.timeseries.TestHelpers.randomForecaster;

import java.io.IOException;
import java.time.Instant;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class ForecastTaskManagerTests extends AbstractTimeSeriesTest {

    private ForecastTaskManager forecastTaskManager;
    private Client client;
    private ForecastTask task;
    private final String configId = "test-config-id";
    private TransportService transportService;

    @Before
    public void setup() throws Exception {
        super.setUp();
        TaskCacheManager taskCacheManager = mock(TaskCacheManager.class);
        client = mock(Client.class);
        NamedXContentRegistry xContentRegistry = TestHelpers.xContentRegistry();
        ForecastIndexManagement forecastIndices = mock(ForecastIndexManagement.class);

        Settings settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .build();

        ClusterSettings clusterSettings = clusterSetting(
            settings,
            MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            BATCH_TASK_PIECE_INTERVAL_SECONDS,
            AD_REQUEST_TIMEOUT,
            DELETE_FORECAST_RESULT_WHEN_DELETE_FORECASTER,
            MAX_OLD_TASK_DOCS_PER_FORECASTER,
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES
        );
        ClusterService clusterService = spy(new ClusterService(settings, clusterSettings, mock(ThreadPool.class), null));
        ThreadPool threadPool = mock(ThreadPool.class);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        transportService = mock(TransportService.class);

        forecastTaskManager = spy(
            new ForecastTaskManager(
                taskCacheManager,
                client,
                xContentRegistry,
                forecastIndices,
                clusterService,
                Settings.EMPTY,
                threadPool,
                nodeStateManager
            )
        );

        task = ForecastTask
            .builder()
            .configId(configId)
            .taskType(ForecastTaskType.REALTIME_FORECAST_SINGLE_STREAM.name())
            .state(TaskState.FORECAST_FAILURE.name())
            .isLatest(true)
            .taskId("test-task-id")
            .forecaster(randomForecaster())
            .executionStartTime(Instant.now())
            .lastUpdateTime(Instant.now())
            .build();
    }

    private void setupSearchTask() throws IOException {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> searchListener = invocation.getArgument(1);
            SearchHit[] hits = new SearchHit[] { new SearchHit(1, task.getTaskId(), null, null) };
            XContentBuilder builder = XContentFactory.jsonBuilder();
            hits[0].sourceRef(BytesReference.bytes(task.toXContent(builder, ToXContent.EMPTY_PARAMS)));
            SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
            SearchResponse searchResponse = new SearchResponse(
                new InternalSearchResponse(searchHits, null, null, null, false, null, 1),
                "",
                1,
                1,
                0,
                100,
                null,
                null
            );
            searchListener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testStopLatestRealtimeTask_withForecastFailureState_doesNotThrowJobStoppedException() throws IOException {
        ActionListener<JobResponse> listener = mock(ActionListener.class);

        setupSearchTask();

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateListener = invocation.getArgument(1);
            updateListener
                .onResponse(new UpdateResponse(null, new ShardId("test", "test", 0), "test", 0, 0, 0, DocWriteResponse.Result.CREATED));
            return null;
        }).when(client).update(any(), any());

        forecastTaskManager.stopLatestRealtimeTask(configId, TaskState.STOPPED, null, transportService, listener);

        verify(client, times(1)).update(any(), any());
        verify(listener, never()).onFailure(any());
        verify(listener, times(1)).onResponse(any(JobResponse.class));
    }
}
