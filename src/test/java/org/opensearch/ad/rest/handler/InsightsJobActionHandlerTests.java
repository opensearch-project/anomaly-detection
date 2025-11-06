/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.rest.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Locale;

import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.transport.InsightsJobResponse;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.transport.client.Client;

public class InsightsJobActionHandlerTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;

    @Before
    public void initThreadPool() {
        threadPool = new TestThreadPool(getClass().getSimpleName());
    }

    @After
    public void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 30, java.util.concurrent.TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testStartInsightsJobCreatesNewJob() throws IOException {
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        ADIndexManagement indexManagement = mock(ADIndexManagement.class);
        when(indexManagement.doesJobIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, "insights"));
            return null;
        }).when(indexManagement).initInsightsResultIndexIfAbsent(any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, CommonName.JOB_INDEX));
            return null;
        }).when(indexManagement).initJobIndex(any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            GetResponse response = new GetResponse(
                new GetResult(
                    CommonName.JOB_INDEX,
                    ADCommonName.INSIGHTS_JOB_NAME,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    false,
                    null,
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            listener.onResponse(response);
            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));

        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(mock(IndexResponse.class));
            return null;
        }).when(client).index(indexRequestCaptor.capture(), any(ActionListener.class));

        InsightsJobActionHandler handler = new InsightsJobActionHandler(
            client,
            NamedXContentRegistry.EMPTY,
            indexManagement,
            org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
        );

        ActionListener<InsightsJobResponse> listener = mock(ActionListener.class);
        handler.startInsightsJob("12h", listener);

        verify(indexManagement, times(1)).initInsightsResultIndexIfAbsent(any(ActionListener.class));
        verify(indexManagement, times(1)).initJobIndex(any(ActionListener.class));
        verify(listener, times(1)).onResponse(any(InsightsJobResponse.class));

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertEquals(CommonName.JOB_INDEX, indexRequest.index());

        java.util.Map<String, Object> source = XContentHelper
            .convertToMap(indexRequest.source(), false, indexRequest.getContentType())
            .v2();
        assertEquals(ADCommonName.INSIGHTS_JOB_NAME, source.get("name"));
        assertEquals(Boolean.TRUE, source.get("enabled"));
        assertEquals(AnalysisType.AD.name(), source.get("type"));
        assertEquals(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS, source.get("result_index"));

        java.util.Map<String, Object> schedule = (java.util.Map<String, Object>) source.get("schedule");
        java.util.Map<String, Object> interval = (java.util.Map<String, Object>) schedule.get("interval");
        assertEquals(12, interval.get("period"));
        assertEquals("HOURS", ((String) interval.get("unit")).toUpperCase(Locale.ROOT));
        assertNotNull(interval.get("start_time"));

        java.util.Map<String, Object> windowDelay = (java.util.Map<String, Object>) source.get("window_delay");
        java.util.Map<String, Object> period = (java.util.Map<String, Object>) windowDelay.get("period");
        assertEquals(0L, ((Number) period.get("interval")).longValue());
        assertEquals("MINUTES", ((String) period.get("unit")).toUpperCase(Locale.ROOT));

        long expectedLockSeconds = java.time.Duration.of(12, ChronoUnit.HOURS).getSeconds() * 2;
        assertEquals(expectedLockSeconds, ((Number) source.get("lock_duration_seconds")).longValue());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testStopInsightsJobDisablesExistingJob() throws IOException {
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        ADIndexManagement indexManagement = mock(ADIndexManagement.class);

        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 24, ChronoUnit.HOURS);
        IntervalTimeConfiguration windowDelay = new IntervalTimeConfiguration(0L, ChronoUnit.MINUTES);
        Job existingJob = new Job(
            ADCommonName.INSIGHTS_JOB_NAME,
            schedule,
            windowDelay,
            true,
            Instant.now().minus(1, ChronoUnit.HOURS),
            null,
            Instant.now().minusSeconds(30),
            java.time.Duration.of(24, ChronoUnit.HOURS).getSeconds() * 2,
            null,
            ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
            AnalysisType.AD
        );

        GetResponse getResponse = org.opensearch.timeseries.TestHelpers
            .createGetResponse(existingJob, ADCommonName.INSIGHTS_JOB_NAME, CommonName.JOB_INDEX);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));

        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(mock(IndexResponse.class));
            return null;
        }).when(client).index(indexRequestCaptor.capture(), any(ActionListener.class));

        InsightsJobActionHandler handler = new InsightsJobActionHandler(
            client,
            NamedXContentRegistry.EMPTY,
            indexManagement,
            org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
        );

        ActionListener<InsightsJobResponse> listener = mock(ActionListener.class);
        handler.stopInsightsJob(listener);

        verify(listener, times(1)).onResponse(any(InsightsJobResponse.class));

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        java.util.Map<String, Object> source = XContentHelper
            .convertToMap(indexRequest.source(), false, indexRequest.getContentType())
            .v2();
        assertEquals(Boolean.FALSE, source.get("enabled"));
        assertNotNull(source.get("disabled_time"));
    }

    @SuppressWarnings("unchecked")
    public void testCreateNewJobHandlesJobIndexCreationFailure() {
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        ADIndexManagement indexManagement = mock(ADIndexManagement.class);
        when(indexManagement.doesJobIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, "alias"));
            return null;
        }).when(indexManagement).initInsightsResultIndexIfAbsent(any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new OpenSearchStatusException("boom", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(indexManagement).initJobIndex(any(ActionListener.class));

        InsightsJobActionHandler handler = new InsightsJobActionHandler(
            client,
            NamedXContentRegistry.EMPTY,
            indexManagement,
            org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
        );

        ActionListener<InsightsJobResponse> listener = mock(ActionListener.class);
        handler.startInsightsJob("24h", listener);

        verify(listener, times(1)).onFailure(any(OpenSearchStatusException.class));
    }
}
