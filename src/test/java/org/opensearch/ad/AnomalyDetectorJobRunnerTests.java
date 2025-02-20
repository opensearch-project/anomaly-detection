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

package org.opensearch.ad;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.core.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.get.GetResult;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.JobProcessor;
import org.opensearch.timeseries.JobRunner;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.rest.handler.IndexJobActionHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.StopConfigRequest;
import org.opensearch.timeseries.transport.StopConfigResponse;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import com.google.common.collect.ImmutableList;

public class AnomalyDetectorJobRunnerTests extends AbstractTimeSeriesTest {

    @Mock
    private Client client;

    @Mock
    private ClientUtil clientUtil;

    @Mock
    private ClusterService clusterService;

    private LockService lockService;

    @Mock
    private Job jobParameter;

    @Mock
    private JobExecutionContext context;

    private JobRunner runner = JobRunner.getJobRunnerInstance();

    private ADJobProcessor adJobProcessor = ADJobProcessor.getInstance();

    @Mock
    private ThreadPool mockedThreadPool;

    private ExecutorService executorService;

    @Mock
    private Iterator<TimeValue> backoff;

    @Mock
    private ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADIndexManagement> anomalyResultHandler;

    @Mock
    private ADTaskManager adTaskManager;

    private ExecuteADResultResponseRecorder recorder;

    @Mock
    private DiscoveryNodeFilterer nodeFilter;

    private AnomalyDetector detector;

    @Mock
    private ADTaskCacheManager adTaskCacheManager;

    @Mock
    private NodeStateManager nodeStateManager;

    private ADIndexManagement anomalyDetectionIndices;

    private Settings settings;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyDetectorJobRunnerTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(JobProcessor.class, true);
        MockitoAnnotations.initMocks(this);
        ThreadFactory threadFactory = OpenSearchExecutors.daemonThreadFactory(OpenSearchExecutors.threadName("node1", "test-ad"));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        executorService = OpenSearchExecutors.newFixed("test-ad", 4, 100, threadFactory, threadContext);
        Mockito.doReturn(executorService).when(mockedThreadPool).executor(anyString());
        Mockito.doReturn(mockedThreadPool).when(client).threadPool();
        Mockito.doReturn(threadContext).when(mockedThreadPool).getThreadContext();
        adJobProcessor.setThreadPool(mockedThreadPool);
        adJobProcessor.setClient(client);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String configId = (String) args[0];
            ActionListener<JobResponse> listener = (ActionListener<JobResponse>) args[4];
            listener.onResponse(new JobResponse(configId));
            return null;
        }).when(adTaskManager).stopLatestRealtimeTask(anyString(), any(), any(), any(), any());

        adJobProcessor.setTaskManager(adTaskManager);

        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.max_retry_for_backoff", 2)
            .put("plugins.anomaly_detection.backoff_initial_delay", TimeValue.timeValueMillis(1))
            .put("plugins.anomaly_detection.max_retry_for_end_run_exception", 3)
            .build();
        setUpJobParameter();

        adJobProcessor.registerSettings(settings);

        anomalyDetectionIndices = mock(ADIndexManagement.class);

        adJobProcessor.setIndexManagement(anomalyDetectionIndices);

        lockService = new LockService(client, clusterService);
        doReturn(lockService).when(context).getLockService();

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            if (request.index().equals(CommonName.JOB_INDEX)) {
                Job job = TestHelpers.randomJob(true);
                listener.onResponse(TestHelpers.createGetResponse(job, randomAlphaOfLength(5), CommonName.JOB_INDEX));
            }
            return null;
        }).when(client).get(any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length >= 2
            );

            IndexRequest request = null;
            ActionListener<IndexResponse> listener = null;
            if (args[0] instanceof IndexRequest) {
                request = (IndexRequest) args[0];
            }
            if (args[1] instanceof ActionListener) {
                listener = (ActionListener<IndexResponse>) args[1];
            }

            assertTrue(request != null && listener != null);
            ShardId shardId = new ShardId(new Index(CommonName.JOB_INDEX, randomAlphaOfLength(10)), 0);
            listener.onResponse(new IndexResponse(shardId, request.id(), 1, 1, 1, true));

            return null;
        }).when(client).index(any(), any());

        when(adTaskCacheManager.hasQueriedResultIndex(anyString())).thenReturn(false);

        detector = TestHelpers.randomAnomalyDetector("timestamp", "sourceIndex");
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        adJobProcessor.setNodeStateManager(nodeStateManager);

        recorder = new ExecuteADResultResponseRecorder(
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
        adJobProcessor.setExecuteResultResponseRecorder(recorder);

        ADIndexJobActionHandler adIndexJobActionHandler = new ADIndexJobActionHandler(
            client,
            anomalyDetectionIndices,
            NamedXContentRegistry.EMPTY,
            adTaskManager,
            recorder,
            nodeStateManager,
            settings
        );
        adJobProcessor.setIndexJobActionHandler(adIndexJobActionHandler);
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
        executorService.shutdown();
    }

    @Test
    public void testRunJobWithWrongParameterType() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Job parameter is not instance of Job, type: ");

        ScheduledJobParameter parameter = mock(ScheduledJobParameter.class);
        when(jobParameter.getLockDurationSeconds()).thenReturn(null);
        when(jobParameter.getAnalysisType()).thenReturn(AnalysisType.AD);
        runner.runJob(parameter, context);
    }

    @Test
    public void testRunJobWithNullLockDuration() throws InterruptedException {
        when(jobParameter.getLockDurationSeconds()).thenReturn(null);
        when(jobParameter.getSchedule()).thenReturn(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        when(jobParameter.getAnalysisType()).thenReturn(AnalysisType.AD);
        runner.runJob(jobParameter, context);
        Thread.sleep(2000);
        assertTrue(testAppender.containsMessage("Can't get lock for job"));
    }

    @Test
    public void testRunJobWithLockDuration() throws InterruptedException {
        when(jobParameter.getLockDurationSeconds()).thenReturn(100L);
        when(jobParameter.getSchedule()).thenReturn(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        when(jobParameter.getAnalysisType()).thenReturn(AnalysisType.AD);
        runner.runJob(jobParameter, context);
        Thread.sleep(1000);
        assertFalse(testAppender.containsMessage("Can't get lock for job"));
        verify(context, times(1)).getLockService();
    }

    @Test
    public void testRunAdJobWithNullLock() {
        LockModel lock = null;
        adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), recorder, detector);
        verify(client, never()).execute(any(), any(), any());
    }

    @Test
    public void testRunAdJobWithLock() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);

        adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), recorder, detector);
        verify(client, times(1)).execute(any(), any(), any());
    }

    @Test
    public void testRunAdJobWithExecuteException() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);

        doThrow(RuntimeException.class).when(client).execute(any(), any(), any());

        adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), recorder, detector);
        verify(client, times(1)).execute(any(), any(), any());
        assertTrue(testAppender.containsMessage("Failed to execute AD job"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNow() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
        Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);
        doAnswer(invocation -> {
            ActionRequest request = invocation.getArgument(1);
            if (request instanceof StopConfigRequest) {
                ActionListener<StopConfigResponse> listener = invocation.getArgument(2);
                listener.onResponse(new StopConfigResponse(true));
            }
            return null;
        }).when(client).execute(any(), any(), any());
        adJobProcessor
            .handleException(
                jobParameter,
                lockService,
                lock,
                Instant.now().minusMillis(1000 * 60),
                Instant.now(),
                exception,
                recorder,
                detector
            );
        verify(anomalyResultHandler).index(any(), any(), any());
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndExistingAdJob() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, true, true);
        verify(anomalyResultHandler).index(any(), any(), any());
        verify(client).index(any(IndexRequest.class), any());
        assertTrue(testAppender.containsMessage("EndRunException happened when executing result action for"));
        assertTrue(testAppender.containsMessage("JobRunner will stop job due to EndRunException for"));
        assertTrue(testAppender.containsMessage("was disabled by JobRunner"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndExistingAdJobAndIndexException() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, true, false);
        verify(anomalyResultHandler).index(any(), any(), any());
        verify(client).index(any(IndexRequest.class), any());
        assertTrue(testAppender.containsMessage("JobRunner failed to update job"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndNotExistingEnabledAdJob() {
        Pair<TestAppender, Logger> indexJobActionHandlerAppender = getLog4jAppenderForJUnit(IndexJobActionHandler.class, true);
        try {
            testRunAdJobWithEndRunExceptionNowAndStopAdJob(false, true, true);
            verify(client, never()).index(any(), any());
            assertTrue(testAppender.containsMessage("was disabled by JobRunner"));
            assertFalse(testAppender.containsMessage("JobRunner failed to update job"));
            assertTrue(indexJobActionHandlerAppender.getLeft().containsMessage("was not found"));
            verify(anomalyResultHandler).index(any(), any(), any());
            verify(adTaskManager).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
        } finally {
            tearDownLog4jForJUnit(indexJobActionHandlerAppender.getLeft(), indexJobActionHandlerAppender.getRight());
        }
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndExistingDisabledAdJob() {
        Pair<TestAppender, Logger> indexJobActionHandlerAppender = getLog4jAppenderForJUnit(IndexJobActionHandler.class, true);
        try {
            testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, false, true);
            verify(anomalyResultHandler).index(any(), any(), any());
            verify(client, never()).index(any(), any());
            assertFalse(indexJobActionHandlerAppender.getLeft().containsMessage("was not found"));
            assertTrue(testAppender.containsMessage("was disabled by JobRunner"));
        } finally {
            tearDownLog4jForJUnit(indexJobActionHandlerAppender.getLeft(), indexJobActionHandlerAppender.getRight());
        }
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndNotExistingDisabledAdJob() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(false, false, true);
        verify(anomalyResultHandler).index(any(), any(), any());
        verify(client, never()).index(any(), any());
        assertTrue(testAppender.containsMessage("was disabled by JobRunner"));
    }

    private void testRunAdJobWithEndRunExceptionNowAndStopAdJob(boolean jobExists, boolean jobEnabled, boolean disableSuccessfully) {
        LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
        Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            GetResponse response = new GetResponse(
                new GetResult(
                    CommonName.JOB_INDEX,
                    jobParameter.getName(),
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    jobExists,
                    BytesReference
                        .bytes(
                            new Job(
                                jobParameter.getName(),
                                jobParameter.getSchedule(),
                                jobParameter.getWindowDelay(),
                                jobEnabled,
                                Instant.now().minusSeconds(60),
                                Instant.now(),
                                Instant.now(),
                                60L,
                                TestHelpers.randomUser(),
                                jobParameter.getCustomResultIndexOrAlias(),
                                AnalysisType.AD
                            ).toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
                        ),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );

            listener.onResponse(response);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(invocation -> {
            IndexRequest request = invocation.getArgument(0);
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            ShardId shardId = new ShardId(new Index(CommonName.JOB_INDEX, randomAlphaOfLength(10)), 0);
            if (disableSuccessfully) {
                listener.onResponse(new IndexResponse(shardId, request.id(), 1, 1, 1, true));
            } else {
                listener.onResponse(null);
            }
            return null;
        }).when(client).index(any(IndexRequest.class), any());

        doAnswer(invocation -> {
            ActionRequest request = invocation.getArgument(1);
            if (request instanceof StopConfigRequest) {
                ActionListener<StopConfigResponse> listener = invocation.getArgument(2);
                listener.onResponse(new StopConfigResponse(true));
            }
            return null;
        }).when(client).execute(any(), any(), any());

        adJobProcessor
            .handleException(
                jobParameter,
                lockService,
                lock,
                Instant.now().minusMillis(1000 * 60),
                Instant.now(),
                exception,
                recorder,
                detector
            );
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndGetJobException() {
        Pair<TestAppender, Logger> appenderAndLogger = getLog4jAppenderForJUnit(IndexJobActionHandler.class, true);
        try {
            LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
            Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);

            doAnswer(invocation -> {
                ActionListener<GetResponse> listener = invocation.getArgument(1);
                listener.onFailure(new RuntimeException("test"));
                return null;
            }).when(client).get(any(GetRequest.class), any());

            adJobProcessor
                .handleException(
                    jobParameter,
                    lockService,
                    lock,
                    Instant.now().minusMillis(1000 * 60),
                    Instant.now(),
                    exception,
                    recorder,
                    detector
                );
            assertTrue(testAppender.containsMessage("JobRunner will stop job due to EndRunException for"));
            assertTrue(appenderAndLogger.getLeft().containsMessage("JobRunner failed to get job"));
            verify(anomalyResultHandler).index(any(), any(), any());
            assertEquals(1, appenderAndLogger.getLeft().countMessage("JobRunner failed to get job"));
        } finally {
            tearDownLog4jForJUnit(appenderAndLogger.getLeft(), appenderAndLogger.getRight());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRunAdJobWithEndRunExceptionNowAndFailToGetJob() {
        Pair<TestAppender, Logger> appenderAndLogger = super.getLog4jAppenderForJUnit(IndexJobActionHandler.class, true);
        try {
            LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
            Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);

            doAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                GetRequest request = (GetRequest) args[0];
                ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

                if (request.index().equals(CommonName.JOB_INDEX)) {
                    listener.onFailure(new RuntimeException("fail to get job"));
                }
                return null;
            }).when(client).get(any(), any());

            adJobProcessor
                .handleException(
                    jobParameter,
                    lockService,
                    lock,
                    Instant.now().minusMillis(1000 * 60),
                    Instant.now(),
                    exception,
                    recorder,
                    detector
                );
            verify(anomalyResultHandler).index(any(), any(), any());
            assertEquals(1, appenderAndLogger.getLeft().countMessage("JobRunner failed to get job"));
        } finally {
            tearDownLog4jForJUnit(appenderAndLogger.getLeft(), appenderAndLogger.getRight());
        }
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNotNowAndRetryUntilStop() throws InterruptedException {
        LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
        Instant executionStartTime = Instant.now();
        Schedule schedule = mock(IntervalSchedule.class);
        when(jobParameter.getSchedule()).thenReturn(schedule);
        when(schedule.getNextExecutionTime(executionStartTime)).thenReturn(executionStartTime.plusSeconds(5));

        doAnswer(invocation -> {
            Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), false);
            ActionListener<?> listener = invocation.getArgument(2);
            listener.onFailure(exception);
            return null;
        }).when(client).execute(any(), any(), any());

        for (int i = 0; i < 3; i++) {
            adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);
            assertEquals(i + 1, testAppender.countMessage("EndRunException happened for"));
        }
        adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);
        assertEquals(1, testAppender.countMessage("JobRunner will stop job due to EndRunException retry exceeds upper limit"));
    }

    private void setUpJobParameter() {
        when(jobParameter.getName()).thenReturn(randomAlphaOfLength(10));
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES);
        when(jobParameter.getSchedule()).thenReturn(schedule);
        when(jobParameter.getWindowDelay()).thenReturn(new IntervalTimeConfiguration(10, ChronoUnit.SECONDS));
    }

    /**
     * Test updateLatestRealtimeTask.confirmTotalRCFUpdatesFound
     * @throws InterruptedException
     */
    public Instant confirmInitializedSetup() {
        Schedule schedule = mock(IntervalSchedule.class);
        when(jobParameter.getSchedule()).thenReturn(schedule);
        Instant executionStartTime = Instant.now();
        when(schedule.getNextExecutionTime(executionStartTime)).thenReturn(executionStartTime.plusSeconds(5));

        AnomalyResultResponse response = new AnomalyResultResponse(
            4d,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData("123", "abc", 0d)),
            randomAlphaOfLength(4),
            // not fully initialized
            Long.valueOf(TimeSeriesSettings.NUM_MIN_SAMPLES - 1),
            randomLong(),
            // not an HC detector
            false,
            randomInt(),
            new double[] { randomDoubleBetween(0, 1.0, true), randomDoubleBetween(0, 1.0, true) },
            new double[] { randomDouble(), randomDouble() },
            new double[][] { new double[] { randomDouble(), randomDouble() } },
            new double[] { randomDouble() },
            randomDoubleBetween(1.1, 10.0, true),
            null
        );
        doAnswer(invocation -> {
            ActionListener<AnomalyResultResponse> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(client).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
        return executionStartTime;
    }

    @SuppressWarnings("unchecked")
    public void testFailtoFindDetector() {
        Pair<TestAppender, Logger> appenderAndLogger = super.getLog4jAppenderForJUnit(ExecuteResultResponseRecorder.class, true);
        try {
            Instant executionStartTime = confirmInitializedSetup();

            doAnswer(invocation -> {
                ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
                listener.onFailure(new RuntimeException());
                return null;
            }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

            LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);

            adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

            verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
            verify(adTaskCacheManager, times(1)).hasQueriedResultIndex(anyString());
            verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
            verify(nodeStateManager, times(0)).getJob(any(String.class), any(ActionListener.class));
            verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
            assertEquals(1, appenderAndLogger.getLeft().countMessage("Fail to confirm rcf update"));
            assertTrue(appenderAndLogger.getLeft().containExceptionMsg(TimeSeriesException.class, "fail to get config"));
        } finally {
            super.tearDownLog4jForJUnit(appenderAndLogger.getLeft(), appenderAndLogger.getRight());
        }
    }

    @SuppressWarnings("unchecked")
    public void testFailtoFindJob() {
        Pair<TestAppender, Logger> appenderAndLogger = super.getLog4jAppenderForJUnit(ExecuteResultResponseRecorder.class, true);
        try {
            Instant executionStartTime = confirmInitializedSetup();

            doAnswer(invocation -> {
                ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
                listener.onResponse(Optional.of(detector));
                return null;
            }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

            doAnswer(invocation -> {
                ActionListener<Optional<Job>> listener = invocation.getArgument(1);
                listener.onFailure(new RuntimeException());
                return null;
            }).when(nodeStateManager).getJob(any(String.class), any(ActionListener.class));

            LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);

            adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

            verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
            verify(adTaskCacheManager, times(1)).hasQueriedResultIndex(anyString());
            verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
            verify(nodeStateManager, times(1)).getJob(any(String.class), any(ActionListener.class));
            verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
            assertEquals(1, appenderAndLogger.getLeft().countMessage("Fail to confirm rcf update"));
            assertTrue(appenderAndLogger.getLeft().containExceptionMsg(TimeSeriesException.class, "fail to get job"));
        } finally {
            super.tearDownLog4jForJUnit(appenderAndLogger.getLeft(), appenderAndLogger.getRight());
        }
    }

    @SuppressWarnings("unchecked")
    public void testEmptyDetector() {
        Pair<TestAppender, Logger> appenderAndLogger = getLog4jAppenderForJUnit(ExecuteResultResponseRecorder.class, true);

        try {
            Instant executionStartTime = confirmInitializedSetup();

            doAnswer(invocation -> {
                ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
                listener.onResponse(Optional.empty());
                return null;
            }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

            LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);

            adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

            verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
            verify(adTaskCacheManager, times(1)).hasQueriedResultIndex(anyString());
            verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
            verify(nodeStateManager, times(0)).getJob(any(String.class), any(ActionListener.class));
            verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
            assertEquals(1, appenderAndLogger.getLeft().countMessage("Fail to confirm rcf update"));
            assertTrue(appenderAndLogger.getLeft().containExceptionMsg(TimeSeriesException.class, "fail to get config"));
        } finally {
            super.tearDownLog4jForJUnit(appenderAndLogger.getLeft(), appenderAndLogger.getRight());
        }
    }

    @SuppressWarnings("unchecked")
    public void testEmptyJob() {
        Pair<TestAppender, Logger> appenderAndLogger = super.getLog4jAppenderForJUnit(ExecuteResultResponseRecorder.class, true);
        try {
            Instant executionStartTime = confirmInitializedSetup();

            doAnswer(invocation -> {
                ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
                listener.onResponse(Optional.of(detector));
                return null;
            }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

            doAnswer(invocation -> {
                ActionListener<Optional<Job>> listener = invocation.getArgument(1);
                listener.onResponse(Optional.empty());
                return null;
            }).when(nodeStateManager).getJob(any(String.class), any(ActionListener.class));

            LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);

            adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

            verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
            verify(adTaskCacheManager, times(1)).hasQueriedResultIndex(anyString());
            verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
            verify(nodeStateManager, times(1)).getJob(any(String.class), any(ActionListener.class));
            verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
            assertEquals(1, appenderAndLogger.getLeft().countMessage("Fail to confirm rcf update"));
            assertTrue(appenderAndLogger.getLeft().containExceptionMsg(TimeSeriesException.class, "fail to get job"));
        } finally {
            super.tearDownLog4jForJUnit(appenderAndLogger.getLeft(), appenderAndLogger.getRight());
        }
    }

    @SuppressWarnings("unchecked")
    public void testMarkResultIndexQueried() throws IOException {
        detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance(1)
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .setResultIndex(ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "index")
            .build();
        Instant executionStartTime = confirmInitializedSetup();

        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<Job>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(TestHelpers.randomJob(true, Instant.ofEpochMilli(1602401500000L), null)));
            return null;
        }).when(nodeStateManager).getJob(any(String.class), any(ActionListener.class));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            SearchResponse mockResponse = mock(SearchResponse.class);
            int totalHits = 1001;
            when(mockResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

            listener.onResponse(mockResponse);

            return null;
        }).when(client).search(any(), any(ActionListener.class));

        // use a unmocked adTaskCacheManager to test the value of hasQueriedResultIndex has changed
        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE.getKey(), 2)
            .put(TimeSeriesSettings.MAX_CACHED_DELETED_TASKS.getKey(), 100)
            .build();

        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays.asList(AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE, TimeSeriesSettings.MAX_CACHED_DELETED_TASKS)
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        MemoryTracker memoryTracker = mock(MemoryTracker.class);
        adTaskCacheManager = new ADTaskCacheManager(settings, clusterService, memoryTracker);

        // init real time task cache for the detector. We will do this during AnomalyResultTransportAction.
        // Since we mocked the execution by returning anomaly result directly, we need to init it explicitly.
        adTaskCacheManager.initRealtimeTaskCache(detector.getId(), 0);

        // recreate recorder since we need to use the unmocked adTaskCacheManager
        recorder = new ExecuteADResultResponseRecorder(
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

        assertEquals(false, adTaskCacheManager.hasQueriedResultIndex(detector.getId()));

        LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);

        adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

        verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
        verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        verify(nodeStateManager, times(1)).getJob(any(String.class), any(ActionListener.class));
        verify(client, times(1)).search(any(), any());

        ArgumentCaptor<Long> totalUpdates = ArgumentCaptor.forClass(Long.class);
        verify(adTaskManager, times(1))
            .updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), totalUpdates.capture(), any(), any(), any());
        assertEquals(TimeSeriesSettings.NUM_MIN_SAMPLES, totalUpdates.getValue().longValue());
        assertEquals(true, adTaskCacheManager.hasQueriedResultIndex(detector.getId()));
    }

    public void testValidateCustomResult() throws IOException {
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "index";
        when(jobParameter.getCustomResultIndexOrAlias()).thenReturn(resultIndex);
        doAnswer(invocation -> {
            ExecutorFunction function = invocation.getArgument(4);
            function.execute();
            return null;
        }).when(anomalyDetectionIndices).validateCustomIndexForBackendJob(any(), any(), any(), any(), any(ExecutorFunction.class), any());
        LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
        Instant executionStartTime = confirmInitializedSetup();

        adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);
        verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
    }

    enum CreateIndexMode {
        NOT_ACKED,
        RESOUCE_EXISTS,
        RUNTIME_EXCEPTION
    }

    private void setUpValidateCustomResultIndex(CreateIndexMode mode) throws IOException {
        String resultIndexAlias = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "index";
        when(jobParameter.getCustomResultIndexOrAlias()).thenReturn(resultIndexAlias);

        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                                AnomalyDetectorSettings.AD_MAX_PRIMARY_SHARDS
                            )
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            if (CreateIndexMode.NOT_ACKED == mode) {
                // not acked
                listener.onResponse(new CreateIndexResponse(false, false, resultIndexAlias));
            } else if (CreateIndexMode.RESOUCE_EXISTS == mode) {
                listener.onFailure(new ResourceAlreadyExistsException("index created"));
            } else {
                listener.onFailure(new RuntimeException());
            }

            return null;
        }).when(indicesAdminClient).create(any(), any(ActionListener.class));

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        anomalyDetectionIndices = new ADIndexManagement(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            1,
            NamedXContentRegistry.EMPTY
        );
        adJobProcessor.setIndexManagement(anomalyDetectionIndices);
    }

    public void testNotAckedValidCustomResultCreation() throws IOException {
        setUpValidateCustomResultIndex(CreateIndexMode.NOT_ACKED);
        LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
        Instant executionStartTime = confirmInitializedSetup();

        assertEquals(
            "Expect 0 EndRunException of "
                + jobParameter.getName()
                + ". Got "
                + adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue(),
            0,
            adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue()
        );
        adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);
        assertEquals(
            "Expect 1 EndRunException of "
                + jobParameter.getName()
                + ". Got "
                + adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue(),
            1,
            adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue()
        );
    }

    public void testCustomResultExistsWhileCreation() throws IOException {
        setUpValidateCustomResultIndex(CreateIndexMode.RESOUCE_EXISTS);
        LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
        Instant executionStartTime = confirmInitializedSetup();

        assertEquals(
            "Expect 0 EndRunException of "
                + jobParameter.getName()
                + ". Got "
                + adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue(),
            0,
            adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue()
        );
        adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);
        assertEquals(
            "Expect 0 EndRunException of "
                + jobParameter.getName()
                + ". Got "
                + adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue(),
            0,
            adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue()
        );
        verify(client, times(1)).index(any(), any());
        verify(client, times(1)).delete(any(), any());
    }

    public void testUnexpectedWhileCreation() throws IOException {
        setUpValidateCustomResultIndex(CreateIndexMode.RUNTIME_EXCEPTION);
        LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
        Instant executionStartTime = confirmInitializedSetup();

        assertEquals(
            "Expect 0 EndRunException of "
                + jobParameter.getName()
                + ". Got "
                + adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue(),
            0,
            adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue()
        );
        adJobProcessor.runJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);
        assertEquals(
            "Expect 1 EndRunException of "
                + jobParameter.getName()
                + ". Got "
                + adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue(),
            1,
            adJobProcessor.getEndRunExceptionCount(jobParameter.getName()).intValue()
        );
        verify(client, times(0)).index(any(), any());
        verify(client, times(0)).delete(any(), any());
    }
}
