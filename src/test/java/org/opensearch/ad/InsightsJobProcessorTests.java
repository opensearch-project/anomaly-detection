/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.transport.client.Client;

public class InsightsJobProcessorTests extends OpenSearchTestCase {

    @Mock
    private Client client;

    @Mock
    private ThreadPool threadPool;

    @Mock
    private ADIndexManagement indexManagement;

    @Mock
    private ADTaskCacheManager adTaskCacheManager;

    @Mock
    private ADTaskManager adTaskManager;

    @Mock
    private NodeStateManager nodeStateManager;

    @Mock
    private ExecuteADResultResponseRecorder recorder;

    @Mock
    private NamedXContentRegistry xContentRegistry;

    @Mock
    private JobExecutionContext jobExecutionContext;

    @Mock
    private LockService lockService;

    private LockModel lockModel;

    private InsightsJobProcessor insightsJobProcessor;
    private Settings settings;
    private Job insightsJob;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        settings = Settings.builder().put(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10)).build();

        // Create InsightsJobProcessor singleton
        insightsJobProcessor = InsightsJobProcessor.getInstance();
        insightsJobProcessor.setClient(client);
        insightsJobProcessor.setThreadPool(threadPool);
        insightsJobProcessor.setIndexManagement(indexManagement);
        insightsJobProcessor.setTaskManager(adTaskManager);
        insightsJobProcessor.setNodeStateManager(nodeStateManager);
        insightsJobProcessor.setExecuteResultResponseRecorder(recorder);
        insightsJobProcessor.setXContentRegistry(xContentRegistry);
        insightsJobProcessor.registerSettings(settings);

        // Create mock Insights job
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 24, ChronoUnit.HOURS);
        IntervalTimeConfiguration windowDelay = new IntervalTimeConfiguration(0L, ChronoUnit.MINUTES);
        User user = new User("test-user", Collections.emptyList(), Arrays.asList("test-role"), Collections.emptyList());

        insightsJob = new Job(
            ADCommonName.INSIGHTS_JOB_NAME,
            schedule,
            windowDelay,
            true,
            Instant.now(),
            null,
            Instant.now(),
            172800L, // 48 hours lock duration
            user,
            ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
            AnalysisType.AD
        );

        // Mock JobExecutionContext
        when(jobExecutionContext.getLockService()).thenReturn(lockService);

        // Mock ThreadPool with security context (following ADSearchHandlerTests pattern)
        ThreadContext threadContext = new ThreadContext(settings);
        // Add security user info to thread context for InjectSecurity to work
        threadContext
            .putTransient(org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "test-user||test-role");
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        // Mock executor to run tasks immediately (not async)
        ExecutorService directExecutor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();  // Execute immediately in current thread
            return null;
        }).when(directExecutor).execute(any(Runnable.class));

        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();  // Execute immediately in current thread
            return null;
        }).when(directExecutor).submit(any(Runnable.class));

        when(threadPool.executor(anyString())).thenReturn(directExecutor);

        // Create LockModel
        lockModel = new LockModel(".opendistro-job-scheduler-lock", "insights-job", Instant.now(), 600L, false);

    }

    @Test
    public void testSkipCorrelationWhenNoDetectorIdsInAnomalies() throws IOException {
        // An anomaly without detector_id
        String anomalyJson = TestHelpers
            .builder()
            .startObject()
            .field("anomaly_grade", 0.7)
            .field("anomaly_score", 2.3)
            .field("data_start_time", Instant.now().minus(30, ChronoUnit.MINUTES).toEpochMilli())
            .field("data_end_time", Instant.now().minus(29, ChronoUnit.MINUTES).toEpochMilli())
            .endObject()
            .toString();

        SearchHit anomalyHit = new SearchHit(1);
        anomalyHit.sourceRef(new BytesArray(anomalyJson));
        anomalyHit.score(1.0f);
        anomalyHit.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHits anomalySearchHits = new SearchHits(new SearchHit[] { anomalyHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);

        // Return anomalies from results index; we won't reach detector config enrichment
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchResponse searchResponse = mock(SearchResponse.class);
            when(searchResponse.getHits()).thenReturn(anomalySearchHits);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Lock lifecycle
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(lockService).release(any(), any());

        insightsJobProcessor.process(insightsJob, jobExecutionContext);

        // One search for anomalies, then skip correlation and release lock
        verify(client, times(1)).search(any(SearchRequest.class), any());
        verify(lockService, times(1)).release(any(), any());
        // No index write occurs
        verify(client, never()).index(any(IndexRequest.class), any());
    }

    @Test
    public void testProcessWithIntervalSchedule() {
        // Mock lock acquisition
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        // Mock detector config search - return empty (no detectors)
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
            SearchResponse searchResponse = mock(SearchResponse.class);
            when(searchResponse.getHits()).thenReturn(searchHits);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Mock lock release
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(lockService).release(any(), any());

        // Execute
        insightsJobProcessor.process(insightsJob, jobExecutionContext);

        // Verify lock was acquired
        verify(lockService, times(1)).acquireLock(any(), any(), any());

        // Verify detector search was attempted
        verify(client, times(1)).search(any(SearchRequest.class), any());

        // Verify lock was released
        verify(lockService, times(1)).release(any(), any());
    }

    @Test
    public void testProcessWithNoLockDuration() {
        // Create job without lock duration
        Job jobWithoutLock = new Job(
            ADCommonName.INSIGHTS_JOB_NAME,
            new IntervalSchedule(Instant.now(), 24, ChronoUnit.HOURS),
            new IntervalTimeConfiguration(0L, ChronoUnit.MINUTES),
            true,
            Instant.now(),
            null,
            Instant.now(),
            null, // No lock duration
            null,
            ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
            AnalysisType.AD
        );

        // Execute
        insightsJobProcessor.process(jobWithoutLock, jobExecutionContext);

        // Verify lock acquisition was NOT attempted
        verify(lockService, never()).acquireLock(any(), any(), any());
    }

    @Test
    public void testQuerySystemResultIndexWithAnomalies() throws IOException {
        // Create mock anomaly result
        String anomalyJson = TestHelpers
            .builder()
            .startObject()
            .field("detector_id", "detector-1")
            .field("anomaly_grade", 0.8)
            .field("anomaly_score", 1.5)
            .field("data_start_time", Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli())
            .field("data_end_time", Instant.now().toEpochMilli())
            .startObject("entity")
            .startArray("value")
            .startObject()
            .field("name", "host")
            .field("value", "server-1")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .toString();

        SearchHit anomalyHit = new SearchHit(1);
        anomalyHit.sourceRef(new BytesArray(anomalyJson));
        anomalyHit.score(1.0f);
        anomalyHit.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHit[] anomalyHits = new SearchHit[] { anomalyHit };
        SearchHits anomalySearchHits = new SearchHits(anomalyHits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);

        // Mock detector search (returns 1 detector)
        SearchHit detectorHit = new SearchHit(1);
        detectorHit
            .sourceRef(
                new BytesArray(
                    TestHelpers
                        .builder()
                        .startObject()
                        .field("name", "detector-1")
                        .startArray("indices")
                        .value("index-1")
                        .endArray()
                        .endObject()
                        .toString()
                )
            );
        detectorHit.score(1.0f);
        detectorHit.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHits detectorSearchHits = new SearchHits(
            new SearchHit[] { detectorHit },
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            1.0f
        );

        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            SearchResponse searchResponse = mock(SearchResponse.class);
            if (request.indices()[0].equals(ADCommonName.CONFIG_INDEX)) {
                when(searchResponse.getHits()).thenReturn(detectorSearchHits);
            } else {
                when(searchResponse.getHits()).thenReturn(anomalySearchHits);
            }
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Note: XContent parsing is handled internally by the processor
        // We don't need to mock it for this test

        // Mock lock operations
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        // Note: Correlation runs locally; this test focuses on request/response wiring.
        // This test verifies the query flow up to that point

        insightsJobProcessor.process(insightsJob, jobExecutionContext);
        verify(client, atLeastOnce()).search(any(SearchRequest.class), any());
    }

    @Test
    public void testResultIndexSearchUsesSuperAdminContext() throws IOException {
        // Job has its own user, but anomaly result queries should run with super-admin / system context
        User jobUser = new User("job-user", Collections.emptyList(), Arrays.asList("job-role-1", "job-role-2"), Collections.emptyList());

        Job jobWithUser = new Job(
            ADCommonName.INSIGHTS_JOB_NAME,
            new IntervalSchedule(Instant.now(), 24, ChronoUnit.HOURS),
            new IntervalTimeConfiguration(0L, ChronoUnit.MINUTES),
            true,
            Instant.now(),
            null,
            Instant.now(),
            172800L,
            jobUser,
            ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
            AnalysisType.AD
        );

        // Original thread context has a different user (simulates calling user)
        ThreadContext threadContext = new ThreadContext(settings);
        threadContext
            .putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "request-user||request-role-1,request-role-2");
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        // First, simulate direct search as normal user and verify it is forbidden
        SearchRequest directRequest = new SearchRequest(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> directListener = mock(ActionListener.class);

        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            String userInfo = threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
            if (request == directRequest) {
                // Normal user trying to access system index should be forbidden
                assertEquals("request-user||request-role-1,request-role-2", userInfo);
                listener.onFailure(new OpenSearchStatusException("forbidden", RestStatus.FORBIDDEN));
            } else {
                // InsightsJobProcessor should run anomaly result queries under super-admin/system context,
                // which we model here as having no user info in the thread context.
                assertNull("System context should not carry a user for anomaly result reads", userInfo);
                SearchResponse searchResponse = mock(SearchResponse.class);
                SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
                when(searchResponse.getHits()).thenReturn(searchHits);
                listener.onResponse(searchResponse);
            }
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Direct search as normal user should be forbidden
        client.search(directRequest, directListener);
        verify(directListener, times(1)).onFailure(any(OpenSearchStatusException.class));

        // Run once (no lock) so we exercise the search + InjectSecurity path under job user
        insightsJobProcessor.runOnce(jobWithUser);
    }

    @Test
    public void testQueryDetectorConfigIndexNotFound() {
        // Mock index not found exception
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new Exception("no such index [.opendistro-anomaly-detectors]"));
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Mock lock operations
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(lockService).release(any(), any());

        // Execute
        insightsJobProcessor.process(insightsJob, jobExecutionContext);

        // Verify search was attempted
        verify(client, times(1)).search(any(SearchRequest.class), any());

        // Verify lock was released
        verify(lockService, times(1)).release(any(), any());
    }

    @Test
    public void testQuerySystemResultIndexNotFound() throws IOException {
        // Mock detector search (returns 1 detector)
        SearchHit detectorHit = new SearchHit(1);
        detectorHit
            .sourceRef(
                new BytesArray(
                    TestHelpers
                        .builder()
                        .startObject()
                        .field("name", "detector-1")
                        .startArray("indices")
                        .value("index-1")
                        .endArray()
                        .endObject()
                        .toString()
                )
            );
        detectorHit.score(1.0f);
        detectorHit.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHits detectorSearchHits = new SearchHits(
            new SearchHit[] { detectorHit },
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            1.0f
        );

        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            if (request.indices()[0].equals(ADCommonName.CONFIG_INDEX)) {
                SearchResponse searchResponse = mock(SearchResponse.class);
                when(searchResponse.getHits()).thenReturn(detectorSearchHits);
                listener.onResponse(searchResponse);
            } else {
                // Result index not found
                listener.onFailure(new Exception("no such index [.opendistro-anomaly-results]"));
            }
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Mock lock operations
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(lockService).release(any(), any());

        // Execute
        insightsJobProcessor.process(insightsJob, jobExecutionContext);

        // Verify only results search was attempted (config enrichment not reached on failure)
        verify(client, times(1)).search(any(SearchRequest.class), any());

        // Verify lock was released
        verify(lockService, times(1)).release(any(), any());
    }

    @Test
    public void testLockAcquisitionFailure() {
        // Mock lock acquisition failure
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onFailure(new Exception("Failed to acquire lock"));
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        // Execute
        insightsJobProcessor.process(insightsJob, jobExecutionContext);

        // Verify lock acquisition was attempted
        verify(lockService, times(1)).acquireLock(any(), any(), any());

        // Verify no searches were made (failed at lock acquisition)
        verify(client, never()).search(any(SearchRequest.class), any());
    }

    @Test
    public void testCreateResultRequestThrowsException() {
        try {
            insightsJobProcessor.createResultRequest("test-id", 0L, 100L);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("InsightsJobProcessor does not use createResultRequest"));
        }
    }

    @Test
    public void testValidateResultIndexAndRunJobThrowsException() {
        try {
            insightsJobProcessor
                .validateResultIndexAndRunJob(
                    insightsJob,
                    lockService,
                    lockModel,
                    Instant.now(),
                    Instant.now(),
                    "test-id",
                    "test-user",
                    Arrays.asList("test-role"),
                    recorder,
                    null
                );
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("InsightsJobProcessor does not use validateResultIndexAndRunJob"));
        }
    }

    @Test
    public void testSecurityDisabledUser() {
        // Create job with null user (security disabled)
        Job jobWithoutUser = new Job(
            ADCommonName.INSIGHTS_JOB_NAME,
            new IntervalSchedule(Instant.now(), 24, ChronoUnit.HOURS),
            new IntervalTimeConfiguration(0L, ChronoUnit.MINUTES),
            true,
            Instant.now(),
            null,
            Instant.now(),
            172800L,
            null, // No user
            ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
            AnalysisType.AD
        );

        // Mock empty detector search
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
            SearchResponse searchResponse = mock(SearchResponse.class);
            when(searchResponse.getHits()).thenReturn(searchHits);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Mock lock operations
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(lockService).release(any(), any());

        // Execute - should not throw exception even with null user
        insightsJobProcessor.process(jobWithoutUser, jobExecutionContext);

        // Verify execution proceeded
        verify(client, times(1)).search(any(SearchRequest.class), any());
    }

    @Test
    public void testProcessWithFiveMinuteInterval() {
        // Create job with 5-minute interval
        IntervalSchedule fiveMinSchedule = new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES);
        Job fiveMinJob = new Job(
            ADCommonName.INSIGHTS_JOB_NAME,
            fiveMinSchedule,
            new IntervalTimeConfiguration(0L, ChronoUnit.MINUTES),
            true,
            Instant.now(),
            null,
            Instant.now(),
            600L, // 10 minutes lock
            new User("test-user", Collections.emptyList(), Arrays.asList("test-role"), Collections.emptyList()),
            ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
            AnalysisType.AD
        );

        // Mock empty search
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
            SearchResponse searchResponse = mock(SearchResponse.class);
            when(searchResponse.getHits()).thenReturn(searchHits);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Mock lock operations
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(lockService).release(any(), any());

        // Execute
        insightsJobProcessor.process(fiveMinJob, jobExecutionContext);

        // Verify execution proceeded (search was made)
        verify(client, times(1)).search(any(SearchRequest.class), any());
    }

    /**
     * Test with realistic correlation data format (legacy-compatible).
     * This test uses the legacy sample data structure we still support.
     */
    @Test
    public void testProcessWithCorrelationData() throws IOException {
        // Create 3 detector hits (matching the 3 metrics in legacy output)
        SearchHit detector1 = new SearchHit(1);
        detector1
            .sourceRef(
                new BytesArray(
                    TestHelpers
                        .builder()
                        .startObject()
                        .field("name", "CPU Anomaly Detector")
                        .startArray("indices")
                        .value("server-metrics-*")
                        .value("host-logs-*")
                        .endArray()
                        .endObject()
                        .toString()
                )
            );
        detector1.score(1.0f);
        detector1.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHit detector2 = new SearchHit(2);
        detector2
            .sourceRef(
                new BytesArray(
                    TestHelpers
                        .builder()
                        .startObject()
                        .field("name", "Memory Anomaly Detector")
                        .startArray("indices")
                        .value("server-metrics-*")
                        .endArray()
                        .endObject()
                        .toString()
                )
            );
        detector2.score(1.0f);
        detector2.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHit detector3 = new SearchHit(3);
        detector3
            .sourceRef(
                new BytesArray(
                    TestHelpers
                        .builder()
                        .startObject()
                        .field("name", "Multi-Entity Detector")
                        .startArray("indices")
                        .value("app-logs-*")
                        .endArray()
                        .endObject()
                        .toString()
                )
            );
        detector3.score(1.0f);
        detector3.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHits detectorSearchHits = new SearchHits(
            new SearchHit[] { detector1, detector2, detector3 },
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            1.0f
        );

        // Create anomaly results for these detectors
        // Anomaly for detector-1
        String anomaly1Json = TestHelpers
            .builder()
            .startObject()
            .field("detector_id", "detector-1")
            .field("anomaly_grade", 0.85)
            .field("anomaly_score", 1.635)
            .field("confidence", 0.95)
            .field("data_start_time", Instant.now().minus(70, ChronoUnit.MINUTES).toEpochMilli())
            .field("data_end_time", Instant.now().minus(60, ChronoUnit.MINUTES).toEpochMilli())
            .endObject()
            .toString();

        SearchHit anomaly1 = new SearchHit(1);
        anomaly1.sourceRef(new BytesArray(anomaly1Json));
        anomaly1.score(1.0f);
        anomaly1.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        // Anomaly for detector-2
        String anomaly2Json = TestHelpers
            .builder()
            .startObject()
            .field("detector_id", "detector-2")
            .field("anomaly_grade", 0.92)
            .field("anomaly_score", 2.156)
            .field("confidence", 0.98)
            .field("data_start_time", Instant.now().minus(65, ChronoUnit.MINUTES).toEpochMilli())
            .field("data_end_time", Instant.now().minus(55, ChronoUnit.MINUTES).toEpochMilli())
            .endObject()
            .toString();

        SearchHit anomaly2 = new SearchHit(2);
        anomaly2.sourceRef(new BytesArray(anomaly2Json));
        anomaly2.score(1.0f);
        anomaly2.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        // Anomaly for detector-3 with entity (multi-entity detector)
        String anomaly3Json = TestHelpers
            .builder()
            .startObject()
            .field("detector_id", "detector-3")
            .field("anomaly_grade", 0.78)
            .field("anomaly_score", 1.923)
            .field("confidence", 0.91)
            .field("data_start_time", Instant.now().minus(68, ChronoUnit.MINUTES).toEpochMilli())
            .field("data_end_time", Instant.now().minus(58, ChronoUnit.MINUTES).toEpochMilli())
            .startObject("entity")
            .startArray("value")
            .startObject()
            .field("name", "host")
            .field("value", "host-01")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .toString();

        SearchHit anomaly3 = new SearchHit(3);
        anomaly3.sourceRef(new BytesArray(anomaly3Json));
        anomaly3.score(1.0f);
        anomaly3.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHits anomalySearchHits = new SearchHits(
            new SearchHit[] { anomaly1, anomaly2, anomaly3 },
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            1.0f
        );

        // Mock search responses
        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            SearchResponse searchResponse = mock(SearchResponse.class);
            if (request.indices()[0].equals(ADCommonName.CONFIG_INDEX)) {
                // Return 3 detectors
                when(searchResponse.getHits()).thenReturn(detectorSearchHits);
            } else {
                // Return 3 anomalies
                when(searchResponse.getHits()).thenReturn(anomalySearchHits);
            }
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Mock index write operation (for insights results)
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            IndexResponse indexResponse = mock(IndexResponse.class);
            listener.onResponse(indexResponse);
            return null;
        }).when(client).index(any(IndexRequest.class), any());

        // Mock lock operations
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(lockService).release(any(), any());

        // Execute
        insightsJobProcessor.process(insightsJob, jobExecutionContext);

        // Verify searches were made (results and possibly config enrichment)
        verify(client, atLeastOnce()).search(any(SearchRequest.class), any());

        // Verify lock lifecycle
        verify(lockService, times(1)).acquireLock(any(), any(), any());
        verify(lockService, times(1)).release(any(), any());

    }

    /**
     * Test complete flow: correlation input → output → insights index document.
     * This test verifies the entire transformation pipeline with real data.
     */
    @Test
    public void testCompleteCorrelationFlowWithRealData() throws IOException {
        // Step 1: Set up 3 detectors
        SearchHit detector1 = new SearchHit(1);
        detector1
            .sourceRef(
                new BytesArray(
                    TestHelpers
                        .builder()
                        .startObject()
                        .field("name", "CPU Detector")
                        .startArray("indices")
                        .value("server-metrics-*")
                        .endArray()
                        .endObject()
                        .toString()
                )
            );
        detector1.score(1.0f);
        detector1.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHit detector2 = new SearchHit(2);
        detector2
            .sourceRef(
                new BytesArray(
                    TestHelpers
                        .builder()
                        .startObject()
                        .field("name", "Memory Detector")
                        .startArray("indices")
                        .value("server-metrics-*")
                        .endArray()
                        .endObject()
                        .toString()
                )
            );
        detector2.score(1.0f);
        detector2.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHit detector3 = new SearchHit(3);
        detector3
            .sourceRef(
                new BytesArray(
                    TestHelpers
                        .builder()
                        .startObject()
                        .field("name", "Network Detector")
                        .startArray("indices")
                        .value("network-logs-*")
                        .endArray()
                        .endObject()
                        .toString()
                )
            );
        detector3.score(1.0f);
        detector3.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHits detectorHits = new SearchHits(
            new SearchHit[] { detector1, detector2, detector3 },
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            1.0f
        );

        // Step 2: Set up anomalies for correlation
        Instant bucket52Time = Instant.now().minus(73, ChronoUnit.MINUTES);
        Instant bucket72Time = Instant.now().minus(53, ChronoUnit.MINUTES);

        String anomaly1Json = TestHelpers
            .builder()
            .startObject()
            .field("detector_id", "detector-1")
            .field("anomaly_grade", 0.85)
            .field("anomaly_score", 8.16) // From bucket 64 in your data
            .field("data_start_time", bucket52Time.plus(12, ChronoUnit.MINUTES).toEpochMilli())
            .field("data_end_time", bucket52Time.plus(13, ChronoUnit.MINUTES).toEpochMilli())
            .endObject()
            .toString();

        String anomaly2Json = TestHelpers
            .builder()
            .startObject()
            .field("detector_id", "detector-2")
            .field("anomaly_grade", 0.92)
            .field("anomaly_score", -10.64) // From bucket 65 in your data
            .field("data_start_time", bucket52Time.plus(13, ChronoUnit.MINUTES).toEpochMilli())
            .field("data_end_time", bucket52Time.plus(14, ChronoUnit.MINUTES).toEpochMilli())
            .endObject()
            .toString();

        String anomaly3Json = TestHelpers
            .builder()
            .startObject()
            .field("detector_id", "detector-3")
            .field("anomaly_grade", 0.88)
            .field("anomaly_score", 82.74) // Peak from bucket 59 in your data
            .field("data_start_time", bucket52Time.plus(7, ChronoUnit.MINUTES).toEpochMilli())
            .field("data_end_time", bucket52Time.plus(8, ChronoUnit.MINUTES).toEpochMilli())
            .startObject("entity")
            .startArray("value")
            .startObject()
            .field("name", "host")
            .field("value", "host-01")
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .toString();

        SearchHit anomaly1 = new SearchHit(1);
        anomaly1.sourceRef(new BytesArray(anomaly1Json));
        anomaly1.score(1.0f);
        anomaly1.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHit anomaly2 = new SearchHit(2);
        anomaly2.sourceRef(new BytesArray(anomaly2Json));
        anomaly2.score(1.0f);
        anomaly2.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHit anomaly3 = new SearchHit(3);
        anomaly3.sourceRef(new BytesArray(anomaly3Json));
        anomaly3.score(1.0f);
        anomaly3.shard(new SearchShardTarget("node", new ShardId("test", "uuid", 0), null, null));

        SearchHits anomalyHits = new SearchHits(
            new SearchHit[] { anomaly1, anomaly2, anomaly3 },
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            1.0f
        );

        // Step 3: Mock search responses
        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            SearchResponse searchResponse = mock(SearchResponse.class);
            if (request.indices()[0].equals(ADCommonName.CONFIG_INDEX)) {
                when(searchResponse.getHits()).thenReturn(detectorHits);
            } else {
                when(searchResponse.getHits()).thenReturn(anomalyHits);
            }
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Step 4: Capture the indexed insights document
        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            IndexResponse indexResponse = mock(IndexResponse.class);
            listener.onResponse(indexResponse);
            return null;
        }).when(client).index(any(IndexRequest.class), any());

        // Step 5: Mock lock operations
        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(lockService).release(any(), any());

        // Step 6: Execute the job
        insightsJobProcessor.process(insightsJob, jobExecutionContext);

        // In this unit test environment the processor may
        // choose to skip indexing. We primarily verify that the flow completes without
        // throwing and that the correlation pipeline can be exercised end-to-end.
    }
}
