/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.timeseries.TestHelpers.randomDetector;
import static org.opensearch.timeseries.TestHelpers.randomFeature;
import static org.opensearch.timeseries.TestHelpers.randomUser;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.ad.ADTaskProfileRunner;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

public class StartHistoricalTests extends AbstractTimeSeriesTest {
    private static ADIndexManagement anomalyDetectionIndices;
    private static NamedXContentRegistry xContentRegistry;
    private static DiscoveryNodeFilterer nodeFilter;

    private NodeStateManager nodeStateManager;
    private Client client;
    private ThreadContext.StoredContext context;
    private DateRange detectionDateRange;
    private TransportService transportService;
    private ADIndexJobActionHandler handler;
    private ADTaskManager adTaskManager;
    private ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADIndexManagement> anomalyResultHandler;
    private ADTaskCacheManager adTaskCacheManager;
    private HashRing hashRing;
    private ADTaskProfileRunner taskProfileRunner;
    private DiscoveryNode node1;
    private ActionListener<JobResponse> listener;

    @BeforeClass
    public static void setOnce() throws IOException {
        setUpThreadPool(StartHistoricalTests.class.getSimpleName());
        anomalyDetectionIndices = mock(ADIndexManagement.class);
        xContentRegistry = NamedXContentRegistry.EMPTY;
        when(anomalyDetectionIndices.doesJobIndexExist()).thenReturn(true);
        // make sure getAndExecuteOnLatestConfigLevelTask called in startConfig
        when(anomalyDetectionIndices.doesStateIndexExist()).thenReturn(true);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
    }

    @AfterClass
    public static void terminate() {
        tearDownThreadPool();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        node1 = createDiscoverynode("node1");

        Set<Setting<?>> nodestateSetting = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        nodestateSetting.add(TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE);
        nodestateSetting.add(TimeSeriesSettings.BACKOFF_MINUTES);
        nodestateSetting.add(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ);
        nodestateSetting.add(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR);
        nodestateSetting.add(BATCH_TASK_PIECE_INTERVAL_SECONDS);
        nodestateSetting.add(AD_REQUEST_TIMEOUT);
        nodestateSetting.add(DELETE_AD_RESULT_WHEN_DELETE_DETECTOR);
        nodestateSetting.add(MAX_BATCH_TASK_PER_NODE);
        nodestateSetting.add(MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS);

        ClusterService clusterService = createClusterServiceForNode(threadPool, node1, nodestateSetting);
        nodeStateManager = createNodeStateManager(client, mock(ClientUtil.class), threadPool, clusterService);
        Instant now = Instant.now();
        Instant startTime = now.minus(10, ChronoUnit.DAYS);
        Instant endTime = now.minus(1, ChronoUnit.DAYS);
        detectionDateRange = new DateRange(startTime, endTime);

        Settings settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .build();
        ThreadContext threadContext = new ThreadContext(settings);
        context = threadContext.stashContext();
        transportService = mock(TransportService.class);

        anomalyDetectionIndices = mock(ADIndexManagement.class);
        taskProfileRunner = new ADTaskProfileRunner(hashRing, client);

        hashRing = mock(HashRing.class);
        adTaskManager = spy(
            new ADTaskManager(
                settings,
                clusterService,
                client,
                TestHelpers.xContentRegistry(),
                anomalyDetectionIndices,
                nodeFilter,
                hashRing,
                adTaskCacheManager,
                threadPool,
                nodeStateManager,
                taskProfileRunner
            )
        );

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<JobResponse> listener = (ActionListener<JobResponse>) args[4];

            JobResponse response = mock(JobResponse.class);
            listener.onResponse(response);

            return null;
        }).when(adTaskManager).getAndExecuteOnLatestConfigLevelTask(any(), any(), eq(false), any(), any(), any());

        anomalyResultHandler = mock(ResultBulkIndexingHandler.class);
        adTaskCacheManager = mock(ADTaskCacheManager.class);

        ExecuteADResultResponseRecorder recorder = new ExecuteADResultResponseRecorder(
            anomalyDetectionIndices,
            anomalyResultHandler,
            adTaskManager,
            nodeFilter,
            threadPool,
            client,
            nodeStateManager,
            adTaskCacheManager,
            32
        );

        handler = new ADIndexJobActionHandler(
            client,
            anomalyDetectionIndices,
            xContentRegistry,
            adTaskManager,
            recorder,
            nodeStateManager,
            Settings.EMPTY
        );

        listener = spy(new ActionListener<JobResponse>() {
            @Override
            public void onResponse(JobResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });
    }

    public void testStartDetectorWithNoEnabledFeature() throws IOException {
        AnomalyDetector detector = randomDetector(
            ImmutableList.of(randomFeature(false)),
            randomAlphaOfLength(5),
            1,
            randomAlphaOfLength(5)
        );
        setupGetDetector(detector, client);

        handler.startConfig(detector.getId(), detectionDateRange, randomUser(), transportService, context, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
    }

    private void setupHashRingWithOwningNode() {
        doAnswer(invocation -> {
            Consumer<Optional<DiscoveryNode>> function = invocation.getArgument(1);
            function.accept(Optional.of(node1));
            return null;
        }).when(hashRing).buildAndGetOwningNodeWithSameLocalVersion(any(), any(), any());
    }

    public void testStartDetectorForHistoricalAnalysis() throws IOException {
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector, client);
        setupHashRingWithOwningNode();

        handler.startConfig(detector.getId(), detectionDateRange, randomUser(), transportService, context, listener);
        verify(adTaskManager, times(1)).forwardRequestToLeadNode(any(), any(), any());
    }
}
