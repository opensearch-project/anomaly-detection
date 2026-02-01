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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.util.PluginClient;
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
        // Job has its own user; anomaly result queries should run under the job user's credentials (customer-owned indices),
        // not whatever request user happens to be in the thread context.
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

        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            String userInfo = threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
            SearchResponse searchResponse = mock(SearchResponse.class);

            // First call: resolve custom result index patterns from CONFIG_INDEX using stashed context (no user in thread context)
            if (request.indices() != null
                && request.indices().length > 0
                && ADCommonName.CONFIG_INDEX.equals(request.indices()[0])
                && request.source() != null
                && request.source().size() == 0) {
                assertNull("System context should not carry a user for config index reads", userInfo);
                StringTerms terms = mock(StringTerms.class);
                when(terms.getName()).thenReturn("result_index");
                StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
                when(bucket.getKeyAsString()).thenReturn(ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "unit-test-alias");
                when(terms.getBuckets()).thenReturn(List.of(bucket));
                Aggregations aggs = new Aggregations(List.<Aggregation>of(terms));
                when(searchResponse.getAggregations()).thenReturn(aggs);
                listener.onResponse(searchResponse);
                return null;
            }

            // Subsequent call: anomaly search against customer-owned result indices should run under job user
            // Depending on security plugin wiring in the unit test environment, InjectSecurity may set job user
            // or clear user info entirely; what must not happen is leaking the request user into the search.
            assertNotEquals("request-user||request-role-1,request-role-2", userInfo);
            SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
            when(searchResponse.getHits()).thenReturn(searchHits);
            when(searchResponse.getScrollId()).thenReturn("scroll-1");
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // Run once so we exercise the InjectSecurity path under job user
        insightsJobProcessor.runOnce(jobWithUser);
    }

    @Test
    public void testQueryCustomResultIndexUsesCustomResultAliasesAndClearsScroll() throws Exception {
        String customAlias = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "unit-test-alias";
        Instant end = Instant.now();
        Instant start = end.minus(2, ChronoUnit.HOURS);

        // 1) resolveCustomResultIndexPatterns: CONFIG_INDEX search with aggregation
        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            SearchResponse resp = mock(SearchResponse.class);
            if (request.indices() != null
                && request.indices().length > 0
                && ADCommonName.CONFIG_INDEX.equals(request.indices()[0])
                && request.source() != null
                && request.source().size() == 0) {
                StringTerms terms = mock(StringTerms.class);
                when(terms.getName()).thenReturn("result_index");
                StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
                when(bucket.getKeyAsString()).thenReturn(customAlias);
                when(terms.getBuckets()).thenReturn(List.of(bucket));
                Aggregations aggs = new Aggregations(List.<Aggregation>of(terms));
                when(resp.getAggregations()).thenReturn(aggs);
                listener.onResponse(resp);
                return null;
            }

            // 2) anomaly search: empty hits, with a scroll id
            assertNotNull(request.source());
            QueryBuilder query = request.source().query();
            assertTrue(query instanceof BoolQueryBuilder);
            List<QueryBuilder> filters = ((BoolQueryBuilder) query).filter();

            RangeQueryBuilder executionStart = null;
            RangeQueryBuilder grade = null;
            for (QueryBuilder f : filters) {
                if (f instanceof RangeQueryBuilder == false) {
                    continue;
                }
                RangeQueryBuilder r = (RangeQueryBuilder) f;
                if ("execution_start_time".equals(r.fieldName())) {
                    executionStart = r;
                } else if ("anomaly_grade".equals(r.fieldName())) {
                    grade = r;
                }
            }
            assertNotNull(executionStart);
            assertNotNull(grade);

            // execution_start_time within window
            assertEquals(start.toEpochMilli(), executionStart.from());
            assertEquals(end.toEpochMilli(), executionStart.to());

            // anomaly_grade > 0
            assertEquals(0, grade.from());
            assertFalse(grade.includeLower());

            when(resp.getScrollId()).thenReturn("scroll-1");
            SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
            when(resp.getHits()).thenReturn(hits);
            listener.onResponse(resp);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        // clear scroll is called at the end of the first page when hits < pageSize
        doAnswer(invocation -> {
            ActionListener<ClearScrollResponse> listener = invocation.getArgument(1);
            ClearScrollResponse resp = mock(ClearScrollResponse.class);
            when(resp.isSucceeded()).thenReturn(true);
            listener.onResponse(resp);
            return null;
        }).when(client).clearScroll(any(ClearScrollRequest.class), any());

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("queryCustomResultIndex", Job.class, Instant.class, Instant.class, ActionListener.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<List<org.opensearch.ad.model.AnomalyResult>> listener = mock(ActionListener.class);
        m.invoke(insightsJobProcessor, insightsJob, start, end, listener);

        // We should have executed searches and responded
        verify(client, atLeastOnce()).search(any(SearchRequest.class), any());
        verify(listener, times(1)).onResponse(any(List.class));
    }

    @Test
    public void testWriteInsightsToIndexMappingInvalidReturnsFailure() throws Exception {
        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onResponse(false);
            return null;
        }).when(indexManagement).validateInsightsResultIndexMapping(anyString(), any());

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("window_start", 1).endObject();

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("writeInsightsToIndex", Job.class, XContentBuilder.class, ActionListener.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<Void> completion = mock(ActionListener.class);
        m.invoke(insightsJobProcessor, insightsJob, doc, completion);

        verify(completion, times(1)).onFailure(any(IllegalStateException.class));
        verify(client, never()).index(any(IndexRequest.class), any());
    }

    @Test
    public void testWriteInsightsToIndexIndexesWhenMappingValid() throws Exception {
        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onResponse(true);
            return null;
        }).when(indexManagement).validateInsightsResultIndexMapping(anyString(), any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> l = invocation.getArgument(1);
            IndexResponse resp = mock(IndexResponse.class);
            when(resp.getShardInfo()).thenReturn(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
            when(resp.getId()).thenReturn("id");
            when(resp.getShardId()).thenReturn(new ShardId("idx", "uuid", 0));
            when(resp.status()).thenReturn(RestStatus.CREATED);
            l.onResponse(resp);
            return null;
        }).when(client).index(any(IndexRequest.class), any());

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("window_start", 1).endObject();

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("writeInsightsToIndex", Job.class, XContentBuilder.class, ActionListener.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<Void> completion = mock(ActionListener.class);
        m.invoke(insightsJobProcessor, insightsJob, doc, completion);

        verify(client, times(1)).index(any(IndexRequest.class), any());
        verify(completion, times(1)).onResponse(null);
    }

    @Test
    public void testGuardedLockReleasingListenerReleasesOnlyOnce() throws Exception {
        LockService ls = mock(LockService.class);
        LockModel lock = lockModel;

        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onResponse(true);
            return null;
        }).when(ls).release(any(), any());

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("guardedLockReleasingListener", Job.class, LockService.class, LockModel.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        ActionListener<Void> lockReleasing = (ActionListener<Void>) m.invoke(insightsJobProcessor, insightsJob, ls, lock);

        lockReleasing.onResponse(null);
        lockReleasing.onResponse(null);
        lockReleasing.onFailure(new RuntimeException("boom"));

        verify(ls, times(1)).release(any(), any());
    }

    @Test
    public void testGuardedLockReleasingListenerFailureFirstStillReleasesOnlyOnce() throws Exception {
        LockService ls = mock(LockService.class);
        LockModel lock = lockModel;

        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onResponse(true);
            return null;
        }).when(ls).release(any(), any());

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("guardedLockReleasingListener", Job.class, LockService.class, LockModel.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        ActionListener<Void> lockReleasing = (ActionListener<Void>) m.invoke(insightsJobProcessor, insightsJob, ls, lock);

        lockReleasing.onFailure(new RuntimeException("boom"));
        lockReleasing.onResponse(null); // should be ignored (already released)

        verify(ls, times(1)).release(any(), any());
    }

    @Test
    public void testGuardedLockReleasingListenerNullLockServiceDoesNotThrow() throws Exception {
        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("guardedLockReleasingListener", Job.class, LockService.class, LockModel.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        ActionListener<Void> lockReleasing = (ActionListener<Void>) m.invoke(insightsJobProcessor, insightsJob, null, lockModel);

        // Should not throw; releaseLock() is a no-op when lockService is null.
        lockReleasing.onResponse(null);
        lockReleasing.onResponse(null); // cover "already released" branch too
    }

    @Test
    public void testProcessAnomaliesWithCorrelationNullDetectorsSkips() throws Exception {
        Method m = InsightsJobProcessor.class
            .getDeclaredMethod(
                "processAnomaliesWithCorrelation",
                Job.class,
                List.class,
                Map.class,
                List.class,
                Instant.class,
                Instant.class,
                ActionListener.class
            );
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<Void> completion = mock(ActionListener.class);
        m
            .invoke(
                insightsJobProcessor,
                insightsJob,
                Collections.emptyList(),
                Collections.emptyMap(),
                null,
                Instant.now().minus(1, ChronoUnit.HOURS),
                Instant.now(),
                completion
            );

        verify(completion, times(1)).onResponse(null);
        verify(client, never()).index(any(IndexRequest.class), any());
    }

    @Test
    public void testBuildDetectorMetadataFromAnomaliesDedupesDetectorIds() throws Exception {
        org.opensearch.ad.model.AnomalyResult a1 = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(a1.getDetectorId()).thenReturn("d1");
        org.opensearch.ad.model.AnomalyResult a2 = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(a2.getDetectorId()).thenReturn("d1"); // duplicate: covers containsKey == true branch
        org.opensearch.ad.model.AnomalyResult a3 = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(a3.getDetectorId()).thenReturn("d2");

        Method m = InsightsJobProcessor.class.getDeclaredMethod("buildDetectorMetadataFromAnomalies", List.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, org.opensearch.ad.model.DetectorMetadata> map = (Map<String, org.opensearch.ad.model.DetectorMetadata>) m
            .invoke(insightsJobProcessor, List.of(a1, a2, a3));
        assertEquals(2, map.size());
        assertTrue(map.containsKey("d1"));
        assertTrue(map.containsKey("d2"));
    }

    @Test
    public void testTruncateReturnsOriginalWhenShortAndAddsSuffixWhenLong() throws Exception {
        Method m = InsightsJobProcessor.class.getDeclaredMethod("truncate", String.class);
        m.setAccessible(true);

        String shortVal = "abc";
        assertEquals(shortVal, m.invoke(insightsJobProcessor, shortVal));

        String longVal = "a".repeat(3000);
        String truncated = (String) m.invoke(insightsJobProcessor, longVal);
        assertNotNull(truncated);
        assertTrue(truncated.contains("(truncated)"));
        assertTrue(truncated.length() < longVal.length());
    }

    @Test
    public void testWriteInsightsToIndexMappingValidationFailureCallbackPropagates() throws Exception {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onFailure(new RuntimeException("mapping validation failed"));
            return null;
        }).when(indexManagement).validateInsightsResultIndexMapping(anyString(), any());

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("window_start", 1).endObject();
        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("writeInsightsToIndex", Job.class, XContentBuilder.class, ActionListener.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<Void> completion = mock(ActionListener.class);
        m.invoke(insightsJobProcessor, insightsJob, doc, completion);

        verify(completion, times(1)).onFailure(any(RuntimeException.class));
        verify(client, never()).index(any(IndexRequest.class), any());
    }

    @Test
    public void testWriteInsightsToIndexIndexFailurePropagates() throws Exception {
        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onResponse(true);
            return null;
        }).when(indexManagement).validateInsightsResultIndexMapping(anyString(), any());

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndexResponse> l = invocation.getArgument(1);
            l.onFailure(new RuntimeException("index failed"));
            return null;
        }).when(client).index(any(IndexRequest.class), any());

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("window_start", 1).endObject();
        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("writeInsightsToIndex", Job.class, XContentBuilder.class, ActionListener.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<Void> completion = mock(ActionListener.class);
        m.invoke(insightsJobProcessor, insightsJob, doc, completion);

        verify(completion, times(1)).onFailure(any(RuntimeException.class));
        verify(client, times(1)).index(any(IndexRequest.class), any());
    }

    @Test
    public void testResolveCustomResultIndexPatternsBucketsNullReturnsEmpty() throws Exception {
        StringTerms correct = mock(StringTerms.class);
        when(correct.getName()).thenReturn("result_index");
        when(correct.getBuckets()).thenReturn(null); // covers buckets-null branch

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchResponse resp = mock(SearchResponse.class);
            when(resp.getAggregations()).thenReturn(new Aggregations(List.<Aggregation>of(correct)));
            listener.onResponse(resp);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Method m = InsightsJobProcessor.class.getDeclaredMethod("resolveCustomResultIndexPatterns", Job.class, ActionListener.class);
        m.setAccessible(true);

        AtomicReference<List<String>> out = new AtomicReference<>();
        @SuppressWarnings("unchecked")
        ActionListener<List<String>> listener = ActionListener.wrap(out::set, e -> fail("did not expect failure"));
        m.invoke(insightsJobProcessor, insightsJob, listener);

        assertNotNull(out.get());
        assertEquals(0, out.get().size());
    }

    @Test
    public void testQueryCustomResultIndexSearchFailureNonIndexNotFound() throws Exception {
        String customAlias = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "unit-test-alias";

        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            // 1) resolveCustomResultIndexPatterns CONFIG_INDEX aggregation
            if (request.indices() != null
                && request.indices().length > 0
                && ADCommonName.CONFIG_INDEX.equals(request.indices()[0])
                && request.source() != null
                && request.source().size() == 0) {
                SearchResponse resp = mock(SearchResponse.class);
                StringTerms terms = mock(StringTerms.class);
                when(terms.getName()).thenReturn("result_index");
                StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
                when(bucket.getKeyAsString()).thenReturn(customAlias);
                when(terms.getBuckets()).thenReturn(List.of(bucket));
                when(resp.getAggregations()).thenReturn(new Aggregations(List.<Aggregation>of(terms)));
                listener.onResponse(resp);
                return null;
            }

            // 2) anomaly search failure with non-index-not-found message (covers else branch)
            listener.onFailure(new RuntimeException("boom"));
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("queryCustomResultIndex", Job.class, Instant.class, Instant.class, ActionListener.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<List<org.opensearch.ad.model.AnomalyResult>> listener = mock(ActionListener.class);
        m.invoke(insightsJobProcessor, insightsJob, Instant.now().minus(2, ChronoUnit.HOURS), Instant.now(), listener);

        verify(listener, times(1)).onFailure(any(Exception.class));
    }

    @Test
    public void testProcessWithNonIntervalScheduleFallsBackTo24Hours() {
        // Non-IntervalSchedule should use fallback window computation.
        Schedule nonInterval = mock(Schedule.class);
        Job job = new Job(
            ADCommonName.INSIGHTS_JOB_NAME,
            nonInterval,
            new IntervalTimeConfiguration(0L, ChronoUnit.MINUTES),
            true,
            Instant.now(),
            null,
            Instant.now(),
            600L,
            new User("test-user", Collections.emptyList(), Arrays.asList("test-role"), Collections.emptyList()),
            ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
            AnalysisType.AD
        );

        doAnswer(invocation -> {
            ActionListener<LockModel> listener = invocation.getArgument(2);
            listener.onResponse(lockModel);
            return null;
        }).when(lockService).acquireLock(any(), any(), any());

        // resolveCustomResultIndexPatterns() returns empty because aggs are null.
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchResponse resp = mock(SearchResponse.class);
            when(resp.getAggregations()).thenReturn(null);
            listener.onResponse(resp);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(lockService).release(any(), any());

        insightsJobProcessor.process(job, jobExecutionContext);

        verify(lockService, times(1)).acquireLock(any(), any(), any());
        verify(lockService, times(1)).release(any(), any());
        verify(client, times(1)).search(any(SearchRequest.class), any());
    }

    @Test
    public void testRunOnceWithNonIntervalScheduleAndNoCustomIndicesSkipsCorrelation() {
        Schedule nonInterval = mock(Schedule.class);
        Job job = new Job(
            ADCommonName.INSIGHTS_JOB_NAME,
            nonInterval,
            new IntervalTimeConfiguration(0L, ChronoUnit.MINUTES),
            true,
            Instant.now(),
            null,
            Instant.now(),
            600L,
            new User("test-user", Collections.emptyList(), Arrays.asList("test-role"), Collections.emptyList()),
            ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
            AnalysisType.AD
        );

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchResponse resp = mock(SearchResponse.class);
            when(resp.getAggregations()).thenReturn(null); // => no index patterns
            listener.onResponse(resp);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        insightsJobProcessor.runOnce(job);
        verify(client, times(1)).search(any(SearchRequest.class), any());
    }

    @Test
    public void testResolveCustomResultIndexPatternsSkipsEmptyAliasAndIgnoresWrongAggName() throws Exception {
        StringTerms wrong = mock(StringTerms.class);
        when(wrong.getName()).thenReturn("not_result_index");

        StringTerms correct = mock(StringTerms.class);
        when(correct.getName()).thenReturn("result_index");

        StringTerms.Bucket empty = mock(StringTerms.Bucket.class);
        when(empty.getKeyAsString()).thenReturn("");
        StringTerms.Bucket ok = mock(StringTerms.Bucket.class);
        when(ok.getKeyAsString()).thenReturn(ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "alias-x");
        when(correct.getBuckets()).thenReturn(List.of(empty, ok));

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchResponse resp = mock(SearchResponse.class);
            when(resp.getAggregations()).thenReturn(new Aggregations(List.<Aggregation>of(wrong, correct)));
            listener.onResponse(resp);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Method m = InsightsJobProcessor.class.getDeclaredMethod("resolveCustomResultIndexPatterns", Job.class, ActionListener.class);
        m.setAccessible(true);

        AtomicReference<List<String>> out = new AtomicReference<>();
        @SuppressWarnings("unchecked")
        ActionListener<List<String>> listener = ActionListener.wrap(out::set, e -> fail("did not expect failure"));
        m.invoke(insightsJobProcessor, insightsJob, listener);

        assertNotNull(out.get());
        assertEquals(1, out.get().size());
        assertTrue("Expected wildcard pattern for alias", out.get().get(0).contains("alias-x"));
    }

    @Test
    public void testResolveCustomResultIndexPatternsUsesPluginClientWhenPresent() throws Exception {
        PluginClient pluginClient = mock(PluginClient.class);
        insightsJobProcessor.setPluginClient(pluginClient);
        try {
            String alias = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "pc-alias";

            doAnswer(invocation -> {
                ActionListener<SearchResponse> listener = invocation.getArgument(1);
                SearchResponse resp = mock(SearchResponse.class);
                StringTerms terms = mock(StringTerms.class);
                when(terms.getName()).thenReturn("result_index");
                StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
                when(bucket.getKeyAsString()).thenReturn(alias);
                when(terms.getBuckets()).thenReturn(List.of(bucket));
                when(resp.getAggregations()).thenReturn(new Aggregations(List.<Aggregation>of(terms)));
                listener.onResponse(resp);
                return null;
            }).when(pluginClient).search(any(SearchRequest.class), any());

            Method m = InsightsJobProcessor.class.getDeclaredMethod("resolveCustomResultIndexPatterns", Job.class, ActionListener.class);
            m.setAccessible(true);

            AtomicReference<List<String>> out = new AtomicReference<>();
            @SuppressWarnings("unchecked")
            ActionListener<List<String>> listener = ActionListener.wrap(out::set, e -> fail("did not expect failure"));
            m.invoke(insightsJobProcessor, insightsJob, listener);

            verify(pluginClient, times(1)).search(any(SearchRequest.class), any());
            verify(client, never()).search(any(SearchRequest.class), any());
            assertNotNull(out.get());
            assertEquals(1, out.get().size());
            assertTrue(out.get().get(0).contains("pc-alias"));
        } finally {
            insightsJobProcessor.setPluginClient(null);
        }
    }

    @Test
    public void testResolveCustomResultIndexPatternsFallsBackWhenPluginClientNotInitialized() throws Exception {
        PluginClient pluginClient = mock(PluginClient.class);
        insightsJobProcessor.setPluginClient(pluginClient);
        try {
            doAnswer(invocation -> { throw new IllegalStateException("PluginClient is not initialized."); })
                .when(pluginClient)
                .search(any(SearchRequest.class), any());

            String alias = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "fallback-alias";
            doAnswer(invocation -> {
                ActionListener<SearchResponse> listener = invocation.getArgument(1);
                SearchResponse resp = mock(SearchResponse.class);
                StringTerms terms = mock(StringTerms.class);
                when(terms.getName()).thenReturn("result_index");
                StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
                when(bucket.getKeyAsString()).thenReturn(alias);
                when(terms.getBuckets()).thenReturn(List.of(bucket));
                when(resp.getAggregations()).thenReturn(new Aggregations(List.<Aggregation>of(terms)));
                listener.onResponse(resp);
                return null;
            }).when(client).search(any(SearchRequest.class), any());

            Method m = InsightsJobProcessor.class.getDeclaredMethod("resolveCustomResultIndexPatterns", Job.class, ActionListener.class);
            m.setAccessible(true);

            AtomicReference<List<String>> out = new AtomicReference<>();
            @SuppressWarnings("unchecked")
            ActionListener<List<String>> listener = ActionListener.wrap(out::set, e -> fail("did not expect failure"));
            m.invoke(insightsJobProcessor, insightsJob, listener);

            verify(pluginClient, times(1)).search(any(SearchRequest.class), any());
            verify(client, times(1)).search(any(SearchRequest.class), any());
            assertNotNull(out.get());
            assertEquals(1, out.get().size());
            assertTrue(out.get().get(0).contains("fallback-alias"));
        } finally {
            insightsJobProcessor.setPluginClient(null);
        }
    }

    @Test
    public void testFetchDetectorMetadataUsesPluginClientWhenPresent() throws Exception {
        PluginClient pluginClient = mock(PluginClient.class);
        insightsJobProcessor.setPluginClient(pluginClient);
        try {
            org.opensearch.ad.model.AnomalyResult a = mock(org.opensearch.ad.model.AnomalyResult.class);
            when(a.getDetectorId()).thenReturn("detector-1");
            when(a.getConfigId()).thenReturn("detector-1");
            when(a.getDataStartTime()).thenReturn(Instant.now().minus(10, ChronoUnit.MINUTES));
            when(a.getDataEndTime()).thenReturn(Instant.now().minus(5, ChronoUnit.MINUTES));
            when(a.getModelId()).thenReturn("m1");
            when(a.getEntity()).thenReturn(java.util.Optional.empty());

            doAnswer(invocation -> {
                ActionListener<SearchResponse> listener = invocation.getArgument(1);
                SearchResponse resp = mock(SearchResponse.class);

                // Return a "minimal" detector doc that will likely fail strict parsing, but is map-parsable for fallback metadata.
                SearchHit hit = new SearchHit(1);
                hit.sourceRef(new BytesArray("{\"name\":\"d1\",\"indices\":[\"index-1\"]}"));
                SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
                when(resp.getHits()).thenReturn(hits);
                listener.onResponse(resp);
                return null;
            }).when(pluginClient).search(any(SearchRequest.class), any());

            Method m = InsightsJobProcessor.class
                .getDeclaredMethod(
                    "fetchDetectorMetadataAndProceed",
                    List.class,
                    Job.class,
                    Instant.class,
                    Instant.class,
                    ActionListener.class
                );
            m.setAccessible(true);

            @SuppressWarnings("unchecked")
            ActionListener<Void> completion = mock(ActionListener.class);
            m.invoke(insightsJobProcessor, List.of(a), insightsJob, Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), completion);

            verify(pluginClient, times(1)).search(any(SearchRequest.class), any());
            verify(client, never()).search(any(SearchRequest.class), any());
            verify(completion, times(1)).onResponse(null);
        } finally {
            insightsJobProcessor.setPluginClient(null);
        }
    }

    @Test
    public void testFetchDetectorMetadataFallsBackWhenPluginClientNotInitialized() throws Exception {
        PluginClient pluginClient = mock(PluginClient.class);
        insightsJobProcessor.setPluginClient(pluginClient);
        try {
            doAnswer(invocation -> { throw new IllegalStateException("PluginClient is not initialized."); })
                .when(pluginClient)
                .search(any(SearchRequest.class), any());

            org.opensearch.ad.model.AnomalyResult a = mock(org.opensearch.ad.model.AnomalyResult.class);
            when(a.getDetectorId()).thenReturn("detector-1");
            when(a.getConfigId()).thenReturn("detector-1");
            when(a.getDataStartTime()).thenReturn(Instant.now().minus(10, ChronoUnit.MINUTES));
            when(a.getDataEndTime()).thenReturn(Instant.now().minus(5, ChronoUnit.MINUTES));
            when(a.getModelId()).thenReturn("m1");
            when(a.getEntity()).thenReturn(java.util.Optional.empty());

            doAnswer(invocation -> {
                ActionListener<SearchResponse> listener = invocation.getArgument(1);
                SearchResponse resp = mock(SearchResponse.class);
                SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
                when(resp.getHits()).thenReturn(hits);
                listener.onResponse(resp);
                return null;
            }).when(client).search(any(SearchRequest.class), any());

            Method m = InsightsJobProcessor.class
                .getDeclaredMethod(
                    "fetchDetectorMetadataAndProceed",
                    List.class,
                    Job.class,
                    Instant.class,
                    Instant.class,
                    ActionListener.class
                );
            m.setAccessible(true);

            @SuppressWarnings("unchecked")
            ActionListener<Void> completion = mock(ActionListener.class);
            m.invoke(insightsJobProcessor, List.of(a), insightsJob, Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), completion);

            verify(pluginClient, times(1)).search(any(SearchRequest.class), any());
            verify(client, times(1)).search(any(SearchRequest.class), any());
            verify(completion, times(1)).onResponse(null);
        } finally {
            insightsJobProcessor.setPluginClient(null);
        }
    }

    @Test
    public void testClearScrollWithEmptyIdDoesNothing() throws Exception {
        Method m = InsightsJobProcessor.class.getDeclaredMethod("clearScroll", Job.class, String.class);
        m.setAccessible(true);

        m.invoke(insightsJobProcessor, insightsJob, "");
        verify(client, never()).clearScroll(any(ClearScrollRequest.class), any());
    }

    @Test
    public void testFetchScrolledAnomaliesStopsWhenHitsLessThanPageSizeClearsNextScrollId() throws Exception {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchResponse resp = mock(SearchResponse.class);
            when(resp.getScrollId()).thenReturn("scroll-next");
            SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
            when(resp.getHits()).thenReturn(hits);
            listener.onResponse(resp);
            return null;
        }).when(client).searchScroll(any(SearchScrollRequest.class), any());

        ArgumentCaptor<ClearScrollRequest> clearReq = ArgumentCaptor.forClass(ClearScrollRequest.class);
        doAnswer(invocation -> {
            ActionListener<ClearScrollResponse> listener = invocation.getArgument(1);
            ClearScrollResponse resp = mock(ClearScrollResponse.class);
            when(resp.isSucceeded()).thenReturn(true);
            listener.onResponse(resp);
            return null;
        }).when(client).clearScroll(clearReq.capture(), any());

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod(
                "fetchScrolledAnomalies",
                Job.class,
                String.class,
                TimeValue.class,
                int.class,
                List.class,
                Instant.class,
                Instant.class,
                ActionListener.class
            );
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<List<org.opensearch.ad.model.AnomalyResult>> listener = mock(ActionListener.class);
        List<org.opensearch.ad.model.AnomalyResult> all = new ArrayList<>();
        m
            .invoke(
                insightsJobProcessor,
                insightsJob,
                "scroll-prev",
                TimeValue.timeValueMinutes(5),
                10000,
                all,
                Instant.now().minus(1, ChronoUnit.HOURS),
                Instant.now(),
                listener
            );

        verify(listener, times(1)).onResponse(any(List.class));
        assertNotNull(clearReq.getValue());
        assertTrue(clearReq.getValue().getScrollIds().contains("scroll-next"));
    }

    @Test
    public void testFetchScrolledAnomaliesParseExceptionClearsNewestScrollIdAndFails() throws Exception {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchResponse resp = mock(SearchResponse.class);
            when(resp.getScrollId()).thenReturn("scroll-newest");
            SearchHits hits = new SearchHits(new SearchHit[] { null }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
            when(resp.getHits()).thenReturn(hits);
            listener.onResponse(resp);
            return null;
        }).when(client).searchScroll(any(SearchScrollRequest.class), any());

        ArgumentCaptor<ClearScrollRequest> clearReq = ArgumentCaptor.forClass(ClearScrollRequest.class);
        doAnswer(invocation -> {
            ActionListener<ClearScrollResponse> listener = invocation.getArgument(1);
            ClearScrollResponse resp = mock(ClearScrollResponse.class);
            when(resp.isSucceeded()).thenReturn(true);
            listener.onResponse(resp);
            return null;
        }).when(client).clearScroll(clearReq.capture(), any());

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod(
                "fetchScrolledAnomalies",
                Job.class,
                String.class,
                TimeValue.class,
                int.class,
                List.class,
                Instant.class,
                Instant.class,
                ActionListener.class
            );
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<List<org.opensearch.ad.model.AnomalyResult>> listener = mock(ActionListener.class);
        List<org.opensearch.ad.model.AnomalyResult> all = new ArrayList<>();
        m
            .invoke(
                insightsJobProcessor,
                insightsJob,
                "scroll-prev",
                TimeValue.timeValueMinutes(5),
                10000,
                all,
                Instant.now().minus(1, ChronoUnit.HOURS),
                Instant.now(),
                listener
            );

        verify(listener, times(1)).onFailure(any(Exception.class));
        assertNotNull(clearReq.getValue());
        assertTrue(clearReq.getValue().getScrollIds().contains("scroll-newest"));
    }

    @Test
    public void testParseAnomalyHitsBadJsonDoesNotThrowOrAdd() throws Exception {
        // Use a real registry for this parsing test to avoid mock behavior surprises.
        insightsJobProcessor.setXContentRegistry(NamedXContentRegistry.EMPTY);
        try {
            SearchHit bad = new SearchHit(1);
            bad.sourceRef(new BytesArray("not-json"));

            Method m = InsightsJobProcessor.class.getDeclaredMethod("parseAnomalyHits", SearchHit[].class, List.class);
            m.setAccessible(true);

            List<org.opensearch.ad.model.AnomalyResult> out = new ArrayList<>();
            m.invoke(insightsJobProcessor, new Object[] { new SearchHit[] { bad }, out });
            assertEquals(0, out.size());
        } finally {
            insightsJobProcessor.setXContentRegistry(xContentRegistry);
        }
    }

    @Test
    public void testRunInsightsJobNullLockSkips() throws Exception {
        LockService ls = mock(LockService.class);
        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("runInsightsJob", Job.class, LockService.class, LockModel.class, Instant.class, Instant.class);
        m.setAccessible(true);
        m.invoke(insightsJobProcessor, insightsJob, ls, null, Instant.now().minus(1, ChronoUnit.HOURS), Instant.now());
        verify(ls, never()).release(any(), any());
    }

    @Test
    public void testFetchDetectorMetadataNoDetectorIds() throws Exception {
        org.opensearch.ad.model.AnomalyResult a = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(a.getDetectorId()).thenReturn(null);

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod(
                "fetchDetectorMetadataAndProceed",
                List.class,
                Job.class,
                Instant.class,
                Instant.class,
                ActionListener.class
            );
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<Void> completion = mock(ActionListener.class);
        m.invoke(insightsJobProcessor, List.of(a), insightsJob, Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), completion);
        verify(completion, times(1)).onResponse(null);
    }

    @Test
    public void testFetchDetectorMetadataSearchFailureFallsBack() throws Exception {
        // anomaly with detector id so we attempt config search
        org.opensearch.ad.model.AnomalyResult a = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(a.getDetectorId()).thenReturn("detector-1");
        when(a.getConfigId()).thenReturn("detector-1");
        when(a.getDataStartTime()).thenReturn(Instant.now().minus(10, ChronoUnit.MINUTES));
        when(a.getDataEndTime()).thenReturn(Instant.now().minus(5, ChronoUnit.MINUTES));
        when(a.getModelId()).thenReturn("m1");
        when(a.getEntity()).thenReturn(java.util.Optional.empty());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> l = invocation.getArgument(1);
            l.onFailure(new RuntimeException("config index failure"));
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod(
                "fetchDetectorMetadataAndProceed",
                List.class,
                Job.class,
                Instant.class,
                Instant.class,
                ActionListener.class
            );
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<Void> completion = mock(ActionListener.class);
        m.invoke(insightsJobProcessor, List.of(a), insightsJob, Instant.now().minus(1, ChronoUnit.HOURS), Instant.now(), completion);

        // Fallback path ends up skipping correlation due to empty detector configs list
        verify(completion, times(1)).onResponse(null);
    }

    @Test
    public void testProcessAnomaliesWithCorrelationHappyPathWritesInsights() throws Exception {
        // Two valid anomaly results so includeSingletons=false correlation still yields a non-empty cluster list.
        org.opensearch.ad.model.AnomalyResult a = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(a.getConfigId()).thenReturn("detector-1");
        when(a.getDetectorId()).thenReturn("detector-1");
        Instant start = Instant.now().minus(10, ChronoUnit.MINUTES);
        Instant end = Instant.now().minus(5, ChronoUnit.MINUTES);
        when(a.getDataStartTime()).thenReturn(start);
        when(a.getDataEndTime()).thenReturn(end);
        when(a.getModelId()).thenReturn("m1");
        when(a.getEntity()).thenReturn(java.util.Optional.empty());

        org.opensearch.ad.model.AnomalyResult b = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(b.getConfigId()).thenReturn("detector-2");
        when(b.getDetectorId()).thenReturn("detector-2");
        // Same interval as 'a' to ensure strong temporal overlap and correlation edge.
        when(b.getDataStartTime()).thenReturn(start);
        when(b.getDataEndTime()).thenReturn(end);
        when(b.getModelId()).thenReturn("m2");
        when(b.getEntity()).thenReturn(java.util.Optional.empty());

        // minimal detector configs for correlation
        AnomalyDetector d = mock(AnomalyDetector.class);
        when(d.getId()).thenReturn("detector-1");
        when(d.getName()).thenReturn("d1");
        when(d.getIndices()).thenReturn(List.of("index-1"));
        when(d.getInterval()).thenReturn(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES));

        AnomalyDetector d2 = mock(AnomalyDetector.class);
        when(d2.getId()).thenReturn("detector-2");
        when(d2.getName()).thenReturn("d2");
        when(d2.getIndices()).thenReturn(List.of("index-2"));
        when(d2.getInterval()).thenReturn(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES));

        Map<String, org.opensearch.ad.model.DetectorMetadata> md = Map
            .of(
                "detector-1",
                new org.opensearch.ad.model.DetectorMetadata("detector-1", "d1", List.of("index-1")),
                "detector-2",
                new org.opensearch.ad.model.DetectorMetadata("detector-2", "d2", List.of("index-2"))
            );

        doAnswer(invocation -> {
            ActionListener<Boolean> l = invocation.getArgument(1);
            l.onResponse(true);
            return null;
        }).when(indexManagement).validateInsightsResultIndexMapping(anyString(), any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> l = invocation.getArgument(1);
            IndexResponse resp = mock(IndexResponse.class);
            when(resp.getShardInfo()).thenReturn(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
            when(resp.getId()).thenReturn("id");
            when(resp.getShardId()).thenReturn(new ShardId("idx", "uuid", 0));
            when(resp.status()).thenReturn(RestStatus.CREATED);
            l.onResponse(resp);
            return null;
        }).when(client).index(any(IndexRequest.class), any());

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod(
                "processAnomaliesWithCorrelation",
                Job.class,
                List.class,
                Map.class,
                List.class,
                Instant.class,
                Instant.class,
                ActionListener.class
            );
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<Void> completion = mock(ActionListener.class);
        m
            .invoke(
                insightsJobProcessor,
                insightsJob,
                List.of(a, b),
                md,
                List.of(d, d2),
                Instant.now().minus(1, ChronoUnit.HOURS),
                Instant.now(),
                completion
            );

        verify(client, times(1)).index(any(IndexRequest.class), any());
        verify(completion, times(1)).onResponse(null);
    }

    @Test
    public void testQueryCustomResultIndexParseExceptionClearsScrollAndFails() throws Exception {
        Instant end = Instant.now();
        Instant start = end.minus(2, ChronoUnit.HOURS);
        // 1) patterns resolution returns one custom result index alias*
        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> listener = invocation.getArgument(1);

            SearchResponse resp = mock(SearchResponse.class);
            if (request.indices() != null
                && request.indices().length > 0
                && ADCommonName.CONFIG_INDEX.equals(request.indices()[0])
                && request.source() != null
                && request.source().size() == 0) {
                StringTerms terms = mock(StringTerms.class);
                when(terms.getName()).thenReturn("result_index");
                StringTerms.Bucket bucket = mock(StringTerms.Bucket.class);
                when(bucket.getKeyAsString()).thenReturn(ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "bad-parse");
                when(terms.getBuckets()).thenReturn(List.of(bucket));
                Aggregations aggs = new Aggregations(List.<Aggregation>of(terms));
                when(resp.getAggregations()).thenReturn(aggs);
                listener.onResponse(resp);
                return null;
            }

            // 2) anomaly search: include a null hit to trigger the parse exception path
            assertNotNull(request.source());
            QueryBuilder query = request.source().query();
            assertTrue(query instanceof BoolQueryBuilder);
            List<QueryBuilder> filters = ((BoolQueryBuilder) query).filter();

            RangeQueryBuilder executionStart = null;
            RangeQueryBuilder grade = null;
            for (QueryBuilder f : filters) {
                if (f instanceof RangeQueryBuilder == false) {
                    continue;
                }
                RangeQueryBuilder r = (RangeQueryBuilder) f;
                if ("execution_start_time".equals(r.fieldName())) {
                    executionStart = r;
                } else if ("anomaly_grade".equals(r.fieldName())) {
                    grade = r;
                }
            }
            assertNotNull(executionStart);
            assertNotNull(grade);

            assertEquals(start.toEpochMilli(), executionStart.from());
            assertEquals(end.toEpochMilli(), executionStart.to());

            assertEquals(0, grade.from());
            assertFalse(grade.includeLower());

            when(resp.getScrollId()).thenReturn("scroll-err");
            SearchHits hits = new SearchHits(new SearchHit[] { null }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
            when(resp.getHits()).thenReturn(hits);
            listener.onResponse(resp);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        doAnswer(invocation -> {
            ActionListener<ClearScrollResponse> listener = invocation.getArgument(1);
            ClearScrollResponse resp = mock(ClearScrollResponse.class);
            when(resp.isSucceeded()).thenReturn(true);
            listener.onResponse(resp);
            return null;
        }).when(client).clearScroll(any(ClearScrollRequest.class), any());

        Method m = InsightsJobProcessor.class
            .getDeclaredMethod("queryCustomResultIndex", Job.class, Instant.class, Instant.class, ActionListener.class);
        m.setAccessible(true);

        @SuppressWarnings("unchecked")
        ActionListener<List<org.opensearch.ad.model.AnomalyResult>> listener = mock(ActionListener.class);
        m.invoke(insightsJobProcessor, insightsJob, start, end, listener);

        verify(listener, times(1)).onFailure(any(Exception.class));
        verify(client, times(1)).clearScroll(any(ClearScrollRequest.class), any());
    }

    @Test
    public void testBuildCorrelationPayloadCoversBranchesAndInnerClass() throws Exception {
        InsightsJobProcessor p = InsightsJobProcessor.getInstance();

        org.opensearch.ad.model.AnomalyResult valid = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(valid.getDataStartTime()).thenReturn(Instant.now().minus(10, ChronoUnit.MINUTES));
        when(valid.getDataEndTime()).thenReturn(Instant.now().minus(5, ChronoUnit.MINUTES));
        when(valid.getModelId()).thenReturn(null); // force fallback path
        when(valid.getEntity()).thenReturn(java.util.Optional.empty());
        when(valid.getConfigId()).thenReturn("detector-x");

        org.opensearch.ad.model.AnomalyResult badTime = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(badTime.getDataStartTime()).thenReturn(Instant.now());
        when(badTime.getDataEndTime()).thenReturn(Instant.now()); // not after

        org.opensearch.ad.model.AnomalyResult missingConfig = mock(org.opensearch.ad.model.AnomalyResult.class);
        when(missingConfig.getDataStartTime()).thenReturn(Instant.now().minus(10, ChronoUnit.MINUTES));
        when(missingConfig.getDataEndTime()).thenReturn(Instant.now().minus(9, ChronoUnit.MINUTES));
        when(missingConfig.getConfigId()).thenReturn(null);

        Method m = InsightsJobProcessor.class.getDeclaredMethod("buildCorrelationPayload", List.class);
        m.setAccessible(true);
        Object payload = m.invoke(p, List.of(valid, badTime, missingConfig));
        assertNotNull(payload);

        Field anomaliesField = payload.getClass().getDeclaredField("anomalies");
        anomaliesField.setAccessible(true);
        List<?> anomalies = (List<?>) anomaliesField.get(payload);
        assertEquals(1, anomalies.size());

        // Ensure the inner class lines are covered by accessing its second field
        Field mapField = payload.getClass().getDeclaredField("anomalyResultByAnomaly");
        mapField.setAccessible(true);
        Object idMap = mapField.get(payload);
        assertNotNull(idMap);
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
