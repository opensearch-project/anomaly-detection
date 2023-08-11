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
import static org.opensearch.ad.settings.AnomalyDetectorSettings.NUM_MIN_SAMPLES;
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
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.ad.transport.handler.AnomalyIndexHandler;
import org.opensearch.client.Client;
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
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

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

    private AnomalyDetectorJobRunner runner = AnomalyDetectorJobRunner.getJobRunnerInstance();

    @Mock
    private ThreadPool mockedThreadPool;

    private ExecutorService executorService;

    @Mock
    private Iterator<TimeValue> backoff;

    @Mock
    private AnomalyIndexHandler<AnomalyResult> anomalyResultHandler;

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
        super.setUpLog4jForJUnit(AnomalyDetectorJobRunner.class);
        MockitoAnnotations.initMocks(this);
        ThreadFactory threadFactory = OpenSearchExecutors.daemonThreadFactory(OpenSearchExecutors.threadName("node1", "test-ad"));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        executorService = OpenSearchExecutors.newFixed("test-ad", 4, 100, threadFactory, threadContext);
        Mockito.doReturn(executorService).when(mockedThreadPool).executor(anyString());
        Mockito.doReturn(mockedThreadPool).when(client).threadPool();
        Mockito.doReturn(threadContext).when(mockedThreadPool).getThreadContext();
        runner.setThreadPool(mockedThreadPool);
        runner.setClient(client);
        runner.setAdTaskManager(adTaskManager);

        Settings settings = Settings
            .builder()
            .put("plugins.anomaly_detection.max_retry_for_backoff", 2)
            .put("plugins.anomaly_detection.backoff_initial_delay", TimeValue.timeValueMillis(1))
            .put("plugins.anomaly_detection.max_retry_for_end_run_exception", 3)
            .build();
        setUpJobParameter();

        runner.setSettings(settings);

        anomalyDetectionIndices = mock(ADIndexManagement.class);

        runner.setAnomalyDetectionIndices(anomalyDetectionIndices);

        lockService = new LockService(client, clusterService);
        doReturn(lockService).when(context).getLockService();

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            if (request.index().equals(CommonName.JOB_INDEX)) {
                Job job = TestHelpers.randomAnomalyDetectorJob(true);
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

        detector = TestHelpers.randomAnomalyDetectorWithEmptyFeature();
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        runner.setNodeStateManager(nodeStateManager);

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
        runner.setExecuteADResultResponseRecorder(recorder);
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
        runner.runJob(parameter, context);
    }

    @Test
    public void testRunJobWithNullLockDuration() throws InterruptedException {
        when(jobParameter.getLockDurationSeconds()).thenReturn(null);
        when(jobParameter.getSchedule()).thenReturn(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        runner.runJob(jobParameter, context);
        Thread.sleep(2000);
        assertTrue(testAppender.containsMessage("Can't get lock for AD job"));
    }

    @Test
    public void testRunJobWithLockDuration() throws InterruptedException {
        when(jobParameter.getLockDurationSeconds()).thenReturn(100L);
        when(jobParameter.getSchedule()).thenReturn(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
        runner.runJob(jobParameter, context);
        Thread.sleep(1000);
        assertFalse(testAppender.containsMessage("Can't get lock for AD job"));
        verify(context, times(1)).getLockService();
    }

    @Test
    public void testRunAdJobWithNullLock() {
        LockModel lock = null;
        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), recorder, detector);
        verify(client, never()).execute(any(), any(), any());
    }

    @Test
    public void testRunAdJobWithLock() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);

        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), recorder, detector);
        verify(client, times(1)).execute(any(), any(), any());
    }

    @Test
    public void testRunAdJobWithExecuteException() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);

        doThrow(RuntimeException.class).when(client).execute(any(), any(), any());

        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), recorder, detector);
        verify(client, times(1)).execute(any(), any(), any());
        assertTrue(testAppender.containsMessage("Failed to execute AD job"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNow() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
        Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);
        runner
            .handleAdException(
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
        assertTrue(testAppender.containsMessage("EndRunException happened when executing anomaly result action for"));
        assertTrue(testAppender.containsMessage("JobRunner will stop AD job due to EndRunException for"));
        assertTrue(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndExistingAdJobAndIndexException() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, true, false);
        verify(anomalyResultHandler).index(any(), any(), any());
        verify(client).index(any(IndexRequest.class), any());
        assertTrue(testAppender.containsMessage("Failed to disable AD job for"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndNotExistingEnabledAdJob() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(false, true, true);
        verify(client, never()).index(any(), any());
        assertFalse(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
        assertFalse(testAppender.containsMessage("Failed to disable AD job for"));
        assertTrue(testAppender.containsMessage("AD Job was not found for"));
        verify(anomalyResultHandler).index(any(), any(), any());
        verify(adTaskManager).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndExistingDisabledAdJob() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, false, true);
        verify(anomalyResultHandler).index(any(), any(), any());
        verify(client, never()).index(any(), any());
        assertFalse(testAppender.containsMessage("AD Job was not found for"));
        assertFalse(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
    }

    @Test
    public void testRunAdJobWithEndRunExceptionNowAndNotExistingDisabledAdJob() {
        testRunAdJobWithEndRunExceptionNowAndStopAdJob(false, false, true);
        verify(anomalyResultHandler).index(any(), any(), any());
        verify(client, never()).index(any(), any());
        assertFalse(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
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
                                jobParameter.getCustomResultIndex()
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

        runner
            .handleAdException(
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
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
        Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        runner
            .handleAdException(
                jobParameter,
                lockService,
                lock,
                Instant.now().minusMillis(1000 * 60),
                Instant.now(),
                exception,
                recorder,
                detector
            );
        assertTrue(testAppender.containsMessage("JobRunner will stop AD job due to EndRunException for"));
        assertTrue(testAppender.containsMessage("JobRunner failed to get detector job"));
        verify(anomalyResultHandler).index(any(), any(), any());
        assertEquals(1, testAppender.countMessage("JobRunner failed to get detector job"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRunAdJobWithEndRunExceptionNowAndFailToGetJob() {
        LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
        Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            if (request.index().equals(CommonName.JOB_INDEX)) {
                listener.onFailure(new RuntimeException("fail to get AD job"));
            }
            return null;
        }).when(client).get(any(), any());

        runner
            .handleAdException(
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
        assertEquals(1, testAppender.countMessage("JobRunner failed to get detector job"));
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
            runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);
            assertEquals(i + 1, testAppender.countMessage("EndRunException happened for"));
        }
        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);
        assertEquals(1, testAppender.countMessage("JobRunner will stop AD job due to EndRunException retry exceeds upper limit"));
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
        // clear the appender created in setUp before creating another association; otherwise
        // we will have unexpected error (e.g., some appender does not record messages even
        // though we have configured to do so).
        super.tearDownLog4jForJUnit();
        setUpLog4jForJUnit(ExecuteADResultResponseRecorder.class, true);
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
            Long.valueOf(AnomalyDetectorSettings.NUM_MIN_SAMPLES - 1),
            randomLong(),
            // not an HC detector
            false,
            randomInt(),
            new double[] { randomDoubleBetween(0, 1.0, true), randomDoubleBetween(0, 1.0, true) },
            new double[] { randomDouble(), randomDouble() },
            new double[][] { new double[] { randomDouble(), randomDouble() } },
            new double[] { randomDouble() },
            randomDoubleBetween(1.1, 10.0, true)
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
        Instant executionStartTime = confirmInitializedSetup();

        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

        LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);

        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

        verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).hasQueriedResultIndex(anyString());
        verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        verify(nodeStateManager, times(0)).getJob(any(String.class), any(ActionListener.class));
        verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
        assertEquals(1, testAppender.countMessage("Fail to confirm rcf update"));
        assertTrue(testAppender.containExceptionMsg(TimeSeriesException.class, "fail to get detector"));
    }

    @SuppressWarnings("unchecked")
    public void testFailtoFindJob() {
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

        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

        verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).hasQueriedResultIndex(anyString());
        verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        verify(nodeStateManager, times(1)).getJob(any(String.class), any(ActionListener.class));
        verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
        assertEquals(1, testAppender.countMessage("Fail to confirm rcf update"));
        assertTrue(testAppender.containExceptionMsg(TimeSeriesException.class, "fail to get job"));
    }

    @SuppressWarnings("unchecked")
    public void testEmptyDetector() {
        Instant executionStartTime = confirmInitializedSetup();

        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.empty());
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

        LockModel lock = new LockModel(CommonName.JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);

        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

        verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).hasQueriedResultIndex(anyString());
        verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        verify(nodeStateManager, times(0)).getJob(any(String.class), any(ActionListener.class));
        verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
        assertEquals(1, testAppender.countMessage("Fail to confirm rcf update"));
        assertTrue(testAppender.containExceptionMsg(TimeSeriesException.class, "fail to get detector"));
    }

    @SuppressWarnings("unchecked")
    public void testEmptyJob() {
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

        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

        verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).hasQueriedResultIndex(anyString());
        verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        verify(nodeStateManager, times(1)).getJob(any(String.class), any(ActionListener.class));
        verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
        assertEquals(1, testAppender.countMessage("Fail to confirm rcf update"));
        assertTrue(testAppender.containExceptionMsg(TimeSeriesException.class, "fail to get job"));
    }

    @SuppressWarnings("unchecked")
    public void testMarkResultIndexQueried() throws IOException {
        detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance()
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
            listener.onResponse(Optional.of(TestHelpers.randomAnomalyDetectorJob(true, Instant.ofEpochMilli(1602401500000L), null)));
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
            .put(AnomalyDetectorSettings.MAX_CACHED_DELETED_TASKS.getKey(), 100)
            .build();

        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays.asList(AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE, AnomalyDetectorSettings.MAX_CACHED_DELETED_TASKS)
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

        runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime, recorder, detector);

        verify(client, times(1)).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
        verify(nodeStateManager, times(1)).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        verify(nodeStateManager, times(1)).getJob(any(String.class), any(ActionListener.class));
        verify(client, times(1)).search(any(), any());

        ArgumentCaptor<Long> totalUpdates = ArgumentCaptor.forClass(Long.class);
        verify(adTaskManager, times(1))
            .updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), totalUpdates.capture(), any(), any(), any());
        assertEquals(NUM_MIN_SAMPLES, totalUpdates.getValue().longValue());
        assertEquals(true, adTaskCacheManager.hasQueriedResultIndex(detector.getId()));
    }
}
