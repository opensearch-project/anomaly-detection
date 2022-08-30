/// *
// * SPDX-License-Identifier: Apache-2.0
// *
// * The OpenSearch Contributors require contributions made to
// * this file be licensed under the Apache-2.0 license or a
// * compatible open source license.
// *
// * Modifications Copyright OpenSearch Contributors. See
// * GitHub history for details.
// */
//
// package org.opensearch.ad;
//
// import static org.mockito.ArgumentMatchers.any;
// import static org.mockito.ArgumentMatchers.anyString;
// import static org.mockito.Mockito.doAnswer;
// import static org.mockito.Mockito.doReturn;
// import static org.mockito.Mockito.doThrow;
// import static org.mockito.Mockito.mock;
// import static org.mockito.Mockito.never;
// import static org.mockito.Mockito.times;
// import static org.mockito.Mockito.verify;
// import static org.mockito.Mockito.when;
// import static org.opensearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
// import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
//
// import java.time.Instant;
// import java.time.temporal.ChronoUnit;
// import java.util.Arrays;
// import java.util.Collections;
// import java.util.Iterator;
// import java.util.Locale;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.ThreadFactory;
//
// import org.junit.AfterClass;
// import org.junit.Before;
// import org.junit.BeforeClass;
// import org.junit.Rule;
// import org.junit.Test;
// import org.junit.rules.ExpectedException;
// import org.mockito.Mock;
// import org.mockito.Mockito;
// import org.mockito.MockitoAnnotations;
// import org.opensearch.action.ActionListener;
// import org.opensearch.action.get.GetRequest;
// import org.opensearch.action.get.GetResponse;
// import org.opensearch.action.index.IndexRequest;
// import org.opensearch.action.index.IndexResponse;
// import org.opensearch.ad.common.exception.EndRunException;
// import org.opensearch.ad.indices.AnomalyDetectionIndices;
// import org.opensearch.ad.model.AnomalyDetectorJob;
// import org.opensearch.ad.model.AnomalyResult;
// import org.opensearch.ad.model.IntervalTimeConfiguration;
// import org.opensearch.ad.task.ADTaskManager;
// import org.opensearch.ad.transport.handler.AnomalyIndexHandler;
// import org.opensearch.ad.util.ClientUtil;
// import org.opensearch.ad.util.IndexUtils;
// import org.opensearch.client.Client;
// import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
// import org.opensearch.cluster.service.ClusterService;
// import org.opensearch.common.bytes.BytesReference;
// import org.opensearch.common.settings.Settings;
// import org.opensearch.common.unit.TimeValue;
// import org.opensearch.common.util.concurrent.OpenSearchExecutors;
// import org.opensearch.common.util.concurrent.ThreadContext;
// import org.opensearch.common.xcontent.ToXContent;
// import org.opensearch.index.Index;
// import org.opensearch.index.get.GetResult;
// import org.opensearch.index.shard.ShardId;
// import org.opensearch.jobscheduler.spi.JobExecutionContext;
// import org.opensearch.jobscheduler.spi.LockModel;
// import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
// import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
// import org.opensearch.jobscheduler.spi.schedule.Schedule;
// import org.opensearch.jobscheduler.spi.utils.LockService;
// import org.opensearch.threadpool.ThreadPool;
//
// public class AnomalyDetectorJobRunnerTests extends AbstractADTest {
//
// @Mock
// private Client client;
//
// @Mock
// private ClientUtil clientUtil;
//
// @Mock
// private ClusterService clusterService;
//
// private LockService lockService;
//
// @Mock
// private AnomalyDetectorJob jobParameter;
//
// @Mock
// private JobExecutionContext context;
//
// private AnomalyDetectorJobRunner runner = AnomalyDetectorJobRunner.getJobRunnerInstance();
//
// @Mock
// private ThreadPool mockedThreadPool;
//
// private ExecutorService executorService;
//
// @Mock
// private Iterator<TimeValue> backoff;
//
// @Mock
// private AnomalyIndexHandler<AnomalyResult> anomalyResultHandler;
//
// @Mock
// private ADTaskManager adTaskManager;
//
// @Mock
// private AnomalyDetectionIndices indexUtil;
//
// @BeforeClass
// public static void setUpBeforeClass() {
// setUpThreadPool(AnomalyDetectorJobRunnerTests.class.getSimpleName());
// }
//
// @AfterClass
// public static void tearDownAfterClass() {
// tearDownThreadPool();
// }
//
// @SuppressWarnings("unchecked")
// @Before
// public void setup() throws Exception {
// super.setUp();
// super.setUpLog4jForJUnit(AnomalyDetectorJobRunner.class);
// MockitoAnnotations.initMocks(this);
// ThreadFactory threadFactory = OpenSearchExecutors.daemonThreadFactory(OpenSearchExecutors.threadName("node1", "test-ad"));
// ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
// executorService = OpenSearchExecutors.newFixed("test-ad", 4, 100, threadFactory, threadContext);
// Mockito.doReturn(executorService).when(mockedThreadPool).executor(anyString());
// Mockito.doReturn(mockedThreadPool).when(client).threadPool();
// Mockito.doReturn(threadContext).when(mockedThreadPool).getThreadContext();
// runner.setThreadPool(mockedThreadPool);
// runner.setClient(client);
// runner.setAnomalyResultHandler(anomalyResultHandler);
// runner.setAdTaskManager(adTaskManager);
//
// Settings settings = Settings
// .builder()
// .put("plugins.anomaly_detection.max_retry_for_backoff", 2)
// .put("plugins.anomaly_detection.backoff_initial_delay", TimeValue.timeValueMillis(1))
// .put("plugins.anomaly_detection.max_retry_for_end_run_exception", 3)
// .build();
// setUpJobParameter();
//
// runner.setSettings(settings);
//
// AnomalyDetectionIndices anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
// IndexNameExpressionResolver indexNameResolver = mock(IndexNameExpressionResolver.class);
// IndexUtils indexUtils = new IndexUtils(client, clientUtil, clusterService, indexNameResolver);
// NodeStateManager stateManager = mock(NodeStateManager.class);
//
// runner.setAnomalyDetectionIndices(indexUtil);
//
// lockService = new LockService(client, clusterService);
// doReturn(lockService).when(context).getLockService();
//
// doAnswer(invocation -> {
// Object[] args = invocation.getArguments();
// GetRequest request = (GetRequest) args[0];
// ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
//
// if (request.index().equals(ANOMALY_DETECTOR_JOB_INDEX)) {
// AnomalyDetectorJob job = TestHelpers.randomAnomalyDetectorJob(true);
// listener.onResponse(TestHelpers.createGetResponse(job, randomAlphaOfLength(5), ANOMALY_DETECTOR_JOB_INDEX));
// }
// return null;
// }).when(client).get(any(), any());
//
// doAnswer(invocation -> {
// Object[] args = invocation.getArguments();
// assertTrue(
// String.format(Locale.ROOT, "The size of args is %d. Its content is %s", args.length, Arrays.toString(args)),
// args.length >= 2
// );
//
// IndexRequest request = null;
// ActionListener<IndexResponse> listener = null;
// if (args[0] instanceof IndexRequest) {
// request = (IndexRequest) args[0];
// }
// if (args[1] instanceof ActionListener) {
// listener = (ActionListener<IndexResponse>) args[1];
// }
//
// assertTrue(request != null && listener != null);
// ShardId shardId = new ShardId(new Index(ANOMALY_DETECTOR_JOB_INDEX, randomAlphaOfLength(10)), 0);
// listener.onResponse(new IndexResponse(shardId, request.id(), 1, 1, 1, true));
//
// return null;
// }).when(client).index(any(), any());
// }
//
// @Rule
// public ExpectedException expectedEx = ExpectedException.none();
//
// @Override
// public void tearDown() throws Exception {
// super.tearDown();
// super.tearDownLog4jForJUnit();
// executorService.shutdown();
// }
//
// @Test
// public void testRunJobWithWrongParameterType() {
// expectedEx.expect(IllegalArgumentException.class);
// expectedEx.expectMessage("Job parameter is not instance of AnomalyDetectorJob, type: ");
//
// ScheduledJobParameter parameter = mock(ScheduledJobParameter.class);
// when(jobParameter.getLockDurationSeconds()).thenReturn(null);
// runner.runJob(parameter, context);
// }
//
// @Test
// public void testRunJobWithNullLockDuration() throws InterruptedException {
// when(jobParameter.getLockDurationSeconds()).thenReturn(null);
// when(jobParameter.getSchedule()).thenReturn(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
// runner.runJob(jobParameter, context);
// Thread.sleep(1000);
// assertTrue(testAppender.containsMessage("Can't get lock for AD job"));
// }
//
// @Test
// public void testRunJobWithLockDuration() throws InterruptedException {
// when(jobParameter.getLockDurationSeconds()).thenReturn(100L);
// when(jobParameter.getSchedule()).thenReturn(new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES));
// runner.runJob(jobParameter, context);
// Thread.sleep(1000);
// assertFalse(testAppender.containsMessage("Can't get lock for AD job"));
// verify(context, times(1)).getLockService();
// }
//
// @Test
// public void testRunAdJobWithNullLock() {
// LockModel lock = null;
// runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now());
// verify(client, never()).execute(any(), any(), any());
// }
//
// @Test
// public void testRunAdJobWithLock() {
// LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
//
// runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now());
// verify(client, times(1)).execute(any(), any(), any());
// }
//
// @Test
// public void testRunAdJobWithExecuteException() {
// LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
//
// doThrow(RuntimeException.class).when(client).execute(any(), any(), any());
//
// runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now());
// verify(client, times(1)).execute(any(), any(), any());
// assertTrue(testAppender.containsMessage("Failed to execute AD job"));
// }
//
// @Test
// public void testRunAdJobWithEndRunExceptionNow() {
// LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
// Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);
// runner.handleAdException(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), exception);
// verify(anomalyResultHandler).index(any(), any(), any());
// }
//
// @Test
// public void testRunAdJobWithEndRunExceptionNowAndExistingAdJob() {
// testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, true, true);
// verify(anomalyResultHandler).index(any(), any(), any());
// verify(client).index(any(IndexRequest.class), any());
// assertTrue(testAppender.containsMessage("EndRunException happened when executing anomaly result action for"));
// assertTrue(testAppender.containsMessage("JobRunner will stop AD job due to EndRunException for"));
// assertTrue(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
// }
//
// @Test
// public void testRunAdJobWithEndRunExceptionNowAndExistingAdJobAndIndexException() {
// testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, true, false);
// verify(anomalyResultHandler).index(any(), any(), any());
// verify(client).index(any(IndexRequest.class), any());
// assertTrue(testAppender.containsMessage("Failed to disable AD job for"));
// }
//
// @Test
// public void testRunAdJobWithEndRunExceptionNowAndNotExistingEnabledAdJob() {
// testRunAdJobWithEndRunExceptionNowAndStopAdJob(false, true, true);
// verify(client, never()).index(any(), any());
// assertFalse(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
// assertFalse(testAppender.containsMessage("Failed to disable AD job for"));
// assertTrue(testAppender.containsMessage("AD Job was not found for"));
// verify(anomalyResultHandler).index(any(), any(), any());
// verify(adTaskManager).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
// }
//
// @Test
// public void testRunAdJobWithEndRunExceptionNowAndExistingDisabledAdJob() {
// testRunAdJobWithEndRunExceptionNowAndStopAdJob(true, false, true);
// verify(anomalyResultHandler).index(any(), any(), any());
// verify(client, never()).index(any(), any());
// assertFalse(testAppender.containsMessage("AD Job was not found for"));
// assertFalse(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
// }
//
// @Test
// public void testRunAdJobWithEndRunExceptionNowAndNotExistingDisabledAdJob() {
// testRunAdJobWithEndRunExceptionNowAndStopAdJob(false, false, true);
// verify(anomalyResultHandler).index(any(), any(), any());
// verify(client, never()).index(any(), any());
// assertFalse(testAppender.containsMessage("AD Job was disabled by JobRunner for"));
// }
//
// private void testRunAdJobWithEndRunExceptionNowAndStopAdJob(boolean jobExists, boolean jobEnabled, boolean disableSuccessfully) {
// LockModel lock = new LockModel(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
// Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);
//
// doAnswer(invocation -> {
// ActionListener<GetResponse> listener = invocation.getArgument(1);
// GetResponse response = new GetResponse(
// new GetResult(
// AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
// jobParameter.getName(),
// UNASSIGNED_SEQ_NO,
// 0,
// -1,
// jobExists,
// BytesReference
// .bytes(
// new AnomalyDetectorJob(
// jobParameter.getName(),
// jobParameter.getSchedule(),
// jobParameter.getWindowDelay(),
// jobEnabled,
// Instant.now().minusSeconds(60),
// Instant.now(),
// Instant.now(),
// 60L,
// TestHelpers.randomUser(),
// jobParameter.getResultIndex()
// ).toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
// ),
// Collections.emptyMap(),
// Collections.emptyMap()
// )
// );
//
// listener.onResponse(response);
// return null;
// }).when(client).get(any(GetRequest.class), any());
//
// doAnswer(invocation -> {
// IndexRequest request = invocation.getArgument(0);
// ActionListener<IndexResponse> listener = invocation.getArgument(1);
// ShardId shardId = new ShardId(new Index(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, randomAlphaOfLength(10)), 0);
// if (disableSuccessfully) {
// listener.onResponse(new IndexResponse(shardId, request.id(), 1, 1, 1, true));
// } else {
// listener.onResponse(null);
// }
// return null;
// }).when(client).index(any(IndexRequest.class), any());
//
// runner.handleAdException(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), exception);
// }
//
// @Test
// public void testRunAdJobWithEndRunExceptionNowAndGetJobException() {
// LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
// Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);
//
// doAnswer(invocation -> {
// ActionListener<GetResponse> listener = invocation.getArgument(1);
// listener.onFailure(new RuntimeException("test"));
// return null;
// }).when(client).get(any(GetRequest.class), any());
//
// runner.handleAdException(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), exception);
// assertTrue(testAppender.containsMessage("JobRunner will stop AD job due to EndRunException for"));
// assertTrue(testAppender.containsMessage("JobRunner failed to get detector job"));
// verify(anomalyResultHandler).index(any(), any(), any());
// assertEquals(1, testAppender.countMessage("JobRunner failed to get detector job"));
// }
//
// @SuppressWarnings("unchecked")
// @Test
// public void testRunAdJobWithEndRunExceptionNowAndFailToGetJob() {
// LockModel lock = new LockModel("indexName", "jobId", Instant.now(), 10, false);
// Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), true);
//
// doAnswer(invocation -> {
// Object[] args = invocation.getArguments();
// GetRequest request = (GetRequest) args[0];
// ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
//
// if (request.index().equals(ANOMALY_DETECTOR_JOB_INDEX)) {
// listener.onFailure(new RuntimeException("fail to get AD job"));
// }
// return null;
// }).when(client).get(any(), any());
//
// runner.handleAdException(jobParameter, lockService, lock, Instant.now().minusMillis(1000 * 60), Instant.now(), exception);
// verify(anomalyResultHandler).index(any(), any(), any());
// assertEquals(1, testAppender.countMessage("JobRunner failed to get detector job"));
// }
//
// @Test
// public void testRunAdJobWithEndRunExceptionNotNowAndRetryUntilStop() throws InterruptedException {
// LockModel lock = new LockModel(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, jobParameter.getName(), Instant.now(), 10, false);
// Instant executionStartTime = Instant.now();
// Schedule schedule = mock(IntervalSchedule.class);
// when(jobParameter.getSchedule()).thenReturn(schedule);
// when(schedule.getNextExecutionTime(executionStartTime)).thenReturn(executionStartTime.plusSeconds(5));
//
// doAnswer(invocation -> {
// Exception exception = new EndRunException(jobParameter.getName(), randomAlphaOfLength(5), false);
// ActionListener<?> listener = invocation.getArgument(2);
// listener.onFailure(exception);
// return null;
// }).when(client).execute(any(), any(), any());
//
// for (int i = 0; i < 3; i++) {
// runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime);
// assertEquals(i + 1, testAppender.countMessage("EndRunException happened for"));
// }
// runner.runAdJob(jobParameter, lockService, lock, Instant.now().minusSeconds(60), executionStartTime);
// assertEquals(1, testAppender.countMessage("JobRunner will stop AD job due to EndRunException retry exceeds upper limit"));
// }
//
// private void setUpJobParameter() {
// when(jobParameter.getName()).thenReturn(randomAlphaOfLength(10));
// IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES);
// when(jobParameter.getSchedule()).thenReturn(schedule);
// when(jobParameter.getWindowDelay()).thenReturn(new IntervalTimeConfiguration(10, ChronoUnit.SECONDS));
// }
//
// }
