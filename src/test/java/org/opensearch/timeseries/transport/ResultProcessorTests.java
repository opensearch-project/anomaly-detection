/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.ArgumentCaptor;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastResultProcessor;
import org.opensearch.forecast.transport.ForecastResultRequest;
import org.opensearch.forecast.transport.ForecastResultResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.feature.CompositeRetriever;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class ResultProcessorTests extends OpenSearchTestCase {

    private TestForecastResultProcessor resultProcessor;
    private ThreadPool threadPool;
    private HashRing hashRing;
    private NodeStateManager nodeStateManager;
    private TransportService transportService;
    private final String entityResultAction = "cluster:admin/opensearch/forecast/result";
    private Runnable scheduledCheckerTask;
    private ScheduledCancellable scheduledCancellable;

    private static class TestForecastResultProcessor extends ForecastResultProcessor {
        private boolean imputeCalled;

        TestForecastResultProcessor(
            Setting<TimeValue> requestTimeoutSetting,
            String entityResultAction,
            StatNames hcRequestCountStat,
            Settings settings,
            ClusterService clusterService,
            ThreadPool threadPool,
            HashRing hashRing,
            NodeStateManager nodeStateManager,
            TransportService transportService,
            ForecastStats stats,
            ForecastTaskManager taskManager,
            NamedXContentRegistry xContentRegistry,
            Client client,
            SecurityClientUtil clientUtil,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Class<ForecastResultResponse> transportResultResponseClazz,
            FeatureManager featureManager,
            AnalysisType analysisType,
            boolean runOnce
        ) {
            super(
                requestTimeoutSetting,
                entityResultAction,
                hcRequestCountStat,
                settings,
                clusterService,
                threadPool,
                hashRing,
                nodeStateManager,
                transportService,
                stats,
                taskManager,
                xContentRegistry,
                client,
                clientUtil,
                indexNameExpressionResolver,
                transportResultResponseClazz,
                featureManager,
                analysisType,
                runOnce
            );
        }

        @Override
        protected void imputeHC(long start, long end, String configId, String taskId) {
            imputeCalled = true;               // record invocation
        }

        boolean isImputeCalled() {
            return imputeCalled;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
        hashRing = mock(HashRing.class);
        nodeStateManager = mock(NodeStateManager.class);
        transportService = mock(TransportService.class);
        ForecastStats stats = mock(ForecastStats.class);
        when(stats.getStat(anyString())).thenReturn(mock(TimeSeriesStat.class));
        ForecastTaskManager taskManager = mock(ForecastTaskManager.class);

        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set
                .of(
                    ForecastSettings.FORECAST_MAX_ENTITIES_PER_INTERVAL,
                    ForecastSettings.FORECAST_PAGE_SIZE,
                    ForecastSettings.FORECAST_REQUEST_TIMEOUT
                )
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ExecutorService directExecutor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            return null;
        }).when(directExecutor).execute(any(Runnable.class));

        when(threadPool.executor(anyString())).thenReturn(directExecutor);

        scheduledCheckerTask = null;
        scheduledCancellable = null;
        doAnswer(invocation -> {
            scheduledCheckerTask = invocation.getArgument(0);
            scheduledCancellable = mock(ScheduledCancellable.class);
            return scheduledCancellable;
        }).when(threadPool).scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class), anyString());

        TestForecastResultProcessor baseProcessor = new TestForecastResultProcessor(
            ForecastSettings.FORECAST_REQUEST_TIMEOUT,
            entityResultAction,
            StatNames.FORECAST_HC_EXECUTE_REQUEST_COUNT,
            Settings.EMPTY,
            clusterService,
            threadPool,
            hashRing,
            nodeStateManager,
            transportService,
            stats,
            taskManager,
            NamedXContentRegistry.EMPTY,
            mock(Client.class),
            mock(org.opensearch.timeseries.util.SecurityClientUtil.class),
            mock(IndexNameExpressionResolver.class),
            ForecastResultResponse.class,
            mock(FeatureManager.class),
            AnalysisType.FORECAST,
            false
        );
        resultProcessor = spy(baseProcessor);
    }

    public void testPageListenerRemovesNullModelNodeBeforeDispatch() {
        CompositeRetriever.PageIterator pageIterator = mock(CompositeRetriever.PageIterator.class);
        when(pageIterator.hasNext()).thenReturn(false);

        Config config = mock(Config.class);
        when(config.getId()).thenReturn("configId");
        when(config.getImputationOption()).thenReturn(null);

        ResultProcessor<ForecastResultRequest, ?, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ?, ?, ForecastTaskManager>.PageListener listener =
            resultProcessor.new PageListener(pageIterator, config, 0L, 1L, "taskId");

        CompositeRetriever.Page page = mock(CompositeRetriever.Page.class);
        when(page.isEmpty()).thenReturn(false);

        Entity entity = mock(Entity.class);
        when(entity.toString()).thenReturn("entityKey");
        Map<Entity, double[]> results = new HashMap<>();
        results.put(entity, new double[] { 1.0d });
        when(page.getResults()).thenReturn(results);

        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(anyString())).thenReturn(Optional.empty());

        listener.onResponse(page);

        verify(nodeStateManager, never()).isMuted(anyString(), anyString());
        verifyNoInteractions(transportService);
    }

    public void testPageListenerSkipsMutedNodeBeforeDispatch() {
        CompositeRetriever.PageIterator pageIterator = mock(CompositeRetriever.PageIterator.class);
        when(pageIterator.hasNext()).thenReturn(false);

        Config config = mock(Config.class);
        when(config.getId()).thenReturn("configId");
        when(config.getImputationOption()).thenReturn(null);

        ResultProcessor<ForecastResultRequest, ?, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ?, ?, ForecastTaskManager>.PageListener listener =
            resultProcessor.new PageListener(pageIterator, config, 0L, 1L, "taskId");

        CompositeRetriever.Page page = mock(CompositeRetriever.Page.class);
        when(page.isEmpty()).thenReturn(false);

        Entity entity = mock(Entity.class);
        when(entity.toString()).thenReturn("entityKey");
        Map<Entity, double[]> results = new HashMap<>();
        results.put(entity, new double[] { 1.0d });
        when(page.getResults()).thenReturn(results);

        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("nodeId");
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(anyString())).thenReturn(Optional.of(node));
        when(nodeStateManager.isMuted("nodeId", "configId")).thenReturn(true);

        listener.onResponse(page);

        verify(nodeStateManager).isMuted("nodeId", "configId");
        verifyNoInteractions(transportService);
    }

    public void testPageListenerSkipsEmptyPage() {
        CompositeRetriever.PageIterator pageIterator = mock(CompositeRetriever.PageIterator.class);
        when(pageIterator.hasNext()).thenReturn(false);

        Config config = mock(Config.class);
        when(config.getId()).thenReturn("configId");
        when(config.getImputationOption()).thenReturn(null);

        ResultProcessor<ForecastResultRequest, ?, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ?, ?, ForecastTaskManager>.PageListener listener =
            resultProcessor.new PageListener(pageIterator, config, 0L, 1L, "taskId");

        CompositeRetriever.Page emptyPage = mock(CompositeRetriever.Page.class);
        when(emptyPage.isEmpty()).thenReturn(true);

        listener.onResponse(emptyPage);

        verifyNoInteractions(hashRing);
        verifyNoInteractions(transportService);
        verify(nodeStateManager, never()).isMuted(anyString(), anyString());
    }

    public void testPageListenerHandlesExceptionDuringDispatch() {
        CompositeRetriever.PageIterator pageIterator = mock(CompositeRetriever.PageIterator.class);
        when(pageIterator.hasNext()).thenReturn(false);

        Config config = mock(Config.class);
        when(config.getId()).thenReturn("configId");
        when(config.getImputationOption()).thenReturn(null);

        ResultProcessor<ForecastResultRequest, ?, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ?, ?, ForecastTaskManager>.PageListener listener =
            resultProcessor.new PageListener(pageIterator, config, 0L, 1L, "taskId");

        CompositeRetriever.Page page = mock(CompositeRetriever.Page.class);
        when(page.isEmpty()).thenReturn(false);

        Entity entity = mock(Entity.class);
        when(entity.toString()).thenReturn("entityKey");
        Map<Entity, double[]> results = new HashMap<>();
        results.put(entity, new double[] { 1.0d });
        when(page.getResults()).thenReturn(results);

        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(anyString())).thenThrow(new RuntimeException("boom"));

        listener.onResponse(page);

        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(nodeStateManager).setException(eq("configId"), captor.capture());
        assertTrue(captor.getValue() instanceof InternalFailure);
        verifyNoInteractions(transportService);
    }

    public void testScheduleImputeHCTaskCancelsAfterTimeout() throws Exception {
        CompositeRetriever.PageIterator pageIterator = mock(CompositeRetriever.PageIterator.class);
        when(pageIterator.hasNext()).thenReturn(false);

        Config config = mock(Config.class);
        when(config.getId()).thenReturn("configId");
        when(config.getImputationOption()).thenReturn(new ImputationOption(ImputationMethod.ZERO));
        when(config.getIntervalInMilliseconds()).thenReturn(0L);
        when(config.getWindowDelay()).thenReturn(null);

        ResultProcessor<ForecastResultRequest, ?, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ?, ?, ForecastTaskManager>.PageListener listener =
            resultProcessor.new PageListener(pageIterator, config, 0L, 0L, "taskId");

        CompositeRetriever.Page page = mock(CompositeRetriever.Page.class);
        when(page.isEmpty()).thenReturn(true);

        listener.onResponse(page);

        assertNotNull("Scheduled checker task should be captured", scheduledCheckerTask);
        assertNotNull("Scheduled cancellable should be captured", scheduledCancellable);

        Field sentOutField = listener.getClass().getDeclaredField("sentOutPages");
        sentOutField.setAccessible(true);
        AtomicInteger sentOutPages = (AtomicInteger) sentOutField.get(listener);
        sentOutPages.set(1);

        Field receivedField = listener.getClass().getDeclaredField("receivedPages");
        receivedField.setAccessible(true);
        AtomicInteger receivedPages = (AtomicInteger) receivedField.get(listener);
        receivedPages.set(0);

        Field inFlightField = listener.getClass().getDeclaredField("pagesInFlight");
        inFlightField.setAccessible(true);
        AtomicInteger pagesInFlight = (AtomicInteger) inFlightField.get(listener);
        pagesInFlight.set(0);

        scheduledCheckerTask.run();

        verify(scheduledCancellable).cancel();
        verifyNoInteractions(transportService);
    }

    public void testScheduleImputeHCTaskWaitsWhenPagesStillInFlight() throws Exception {
        CompositeRetriever.PageIterator pageIterator = mock(CompositeRetriever.PageIterator.class);
        when(pageIterator.hasNext()).thenReturn(false);

        long futureEndTime = Instant.now().plusSeconds(60).toEpochMilli();

        Config config = mock(Config.class);
        when(config.getId()).thenReturn("configId");
        when(config.getImputationOption()).thenReturn(new ImputationOption(ImputationMethod.ZERO));
        when(config.getIntervalInMilliseconds()).thenReturn(1000L);
        when(config.getWindowDelay()).thenReturn(null);

        ResultProcessor<ForecastResultRequest, ?, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ?, ?, ForecastTaskManager>.PageListener listener =
            resultProcessor.new PageListener(pageIterator, config, 0L, futureEndTime, "taskId");

        CompositeRetriever.Page page = mock(CompositeRetriever.Page.class);
        when(page.isEmpty()).thenReturn(true);

        listener.onResponse(page);

        assertNotNull("Scheduled checker task should be captured", scheduledCheckerTask);
        assertNotNull("Scheduled cancellable should be captured", scheduledCancellable);

        Field sentOutField = listener.getClass().getDeclaredField("sentOutPages");
        sentOutField.setAccessible(true);
        AtomicInteger sentOutPages = (AtomicInteger) sentOutField.get(listener);
        sentOutPages.set(0);

        Field receivedField = listener.getClass().getDeclaredField("receivedPages");
        receivedField.setAccessible(true);
        AtomicInteger receivedPages = (AtomicInteger) receivedField.get(listener);
        receivedPages.set(0);

        Field inFlightField = listener.getClass().getDeclaredField("pagesInFlight");
        inFlightField.setAccessible(true);
        AtomicInteger pagesInFlight = (AtomicInteger) inFlightField.get(listener);
        pagesInFlight.set(1);

        scheduledCheckerTask.run();

        verify(scheduledCancellable, never()).cancel();
        assertFalse(resultProcessor.isImputeCalled());
    }

}
