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

package org.opensearch.timeseries.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.ad.transport.AnomalyDetectorJobTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class AnomalyDetectorJobActionTests extends OpenSearchIntegTestCase {
    private AnomalyDetectorJobTransportAction action;
    private Task task;
    private JobRequest request;
    private ActionListener<JobResponse> response;
    private NamedWriteableRegistry registry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
        );

        Settings build = Settings.builder().build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        Client client = mock(Client.class);
        org.opensearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, BoolQueryBuilder.NAME, BoolQueryBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, RangeQueryBuilder.NAME, RangeQueryBuilder::new));
        namedWriteables
            .add(
                new NamedWriteableRegistry.Entry(
                    AggregationBuilder.class,
                    ValueCountAggregationBuilder.NAME,
                    ValueCountAggregationBuilder::new
                )
            );
        registry = new NamedWriteableRegistry(namedWriteables);
        action = new AnomalyDetectorJobTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService,
            indexSettings(),
            xContentRegistry(),
            mock(ADIndexJobActionHandler.class),
            registry
        );
        task = mock(Task.class);
        request = new JobRequest(
            "1234",
            ADIndex.CONFIG.getIndexName(),
            new DateRange(Instant.ofEpochMilli(4567), Instant.ofEpochMilli(7890)),
            true,
            "_start"
        );
        response = new ActionListener<JobResponse>() {
            @Override
            public void onResponse(JobResponse adResponse) {
                // Will not be called as there is no detector
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                // Will not be called as there is no detector
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testStartAdJobTransportAction() {
        action.doExecute(task, request, response);
    }

    @Test
    public void testStopAdJobTransportAction() {
        JobRequest stopRequest = new JobRequest(
            "1234",
            ADIndex.CONFIG.getIndexName(),
            new DateRange(Instant.ofEpochMilli(4567), Instant.ofEpochMilli(7890)),
            true,
            "_stop"
        );
        action.doExecute(task, stopRequest, response);
    }

    @Test
    public void testAdJobAction() {
        Assert.assertNotNull(AnomalyDetectorJobAction.INSTANCE.name());
        Assert.assertEquals(AnomalyDetectorJobAction.INSTANCE.name(), AnomalyDetectorJobAction.NAME);
    }

    @Test
    public void testAdJobRequest() throws IOException {
        DateRange detectionDateRange = new DateRange(Instant.MIN, Instant.now());
        request = new JobRequest("1234", ADIndex.CONFIG.getIndexName(), detectionDateRange, false, "_start");

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        JobRequest newRequest = new JobRequest(input);
        Assert.assertEquals(request.getConfigID(), newRequest.getConfigID());
    }

    @Test
    public void testAdJobRequest_NullDetectionDateRange() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        request = new JobRequest("1234", ADIndex.CONFIG.getIndexName(), "_start");
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        JobRequest newRequest = new JobRequest(input);
        Assert.assertEquals(request.getConfigID(), newRequest.getConfigID());
        Assert.assertTrue(!request.isHistorical());
    }

    @Test
    public void testAdJobResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        JobResponse response = new JobResponse("1234");
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        JobResponse newResponse = new JobResponse(input);
        Assert.assertEquals(response.getId(), newResponse.getId());
    }

    @Test
    public void testJobRequestFromActionRequest_SameType() {
        JobRequest request = new JobRequest("1234", ADIndex.CONFIG.getIndexName(), "_start");
        JobRequest result = JobRequest.fromActionRequest(request, registry);
        Assert.assertSame(request, result);
    }

    @Test
    public void testJobRequestFromActionRequest_DifferentType() {
        JobRequest original = new JobRequest("1234", ADIndex.CONFIG.getIndexName(), "_start");

        org.opensearch.action.ActionRequest actionRequest = new org.opensearch.action.ActionRequest() {
            @Override
            public org.opensearch.action.ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) throws java.io.IOException {
                original.writeTo(out);
            }
        };

        JobRequest result = JobRequest.fromActionRequest(actionRequest, registry);
        Assert.assertEquals(original.getConfigID(), result.getConfigID());
    }

    @Test
    public void testJobResponseFromActionResponse_SameType() {
        JobResponse response = new JobResponse("1234");
        JobResponse result = JobResponse.fromActionResponse(response, registry);
        Assert.assertSame(response, result);
    }

    @Test
    public void testJobResponseFromActionResponse_DifferentType() {
        JobResponse original = new JobResponse("1234");

        org.opensearch.core.action.ActionResponse actionResponse = new org.opensearch.core.action.ActionResponse() {
            @Override
            public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) throws java.io.IOException {
                original.writeTo(out);
            }
        };

        JobResponse result = JobResponse.fromActionResponse(actionResponse, registry);
        Assert.assertEquals(original.getId(), result.getId());
    }
}
