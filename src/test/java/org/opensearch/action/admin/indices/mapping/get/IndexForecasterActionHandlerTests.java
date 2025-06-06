/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.action.admin.indices.mapping.get;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.opensearch.security.spi.resources.FeatureConfigConstants.OPENSEARCH_RESOURCE_SHARING_ENABLED;
import static org.opensearch.security.spi.resources.ResourceAccessLevels.PLACE_HOLDER;
import static org.opensearch.timeseries.TestHelpers.randomUser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.rest.handler.IndexForecasterActionHandler;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.IndexForecasterResponse;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.security.spi.resources.client.ResourceSharingClient;
import org.opensearch.security.spi.resources.sharing.Recipient;
import org.opensearch.security.spi.resources.sharing.Recipients;
import org.opensearch.security.spi.resources.sharing.ShareWith;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.resources.ResourceSharingClientAccessor;
import org.opensearch.timeseries.rest.handler.AggregationPrep;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

public class IndexForecasterActionHandlerTests extends AbstractForecasterActionHandlerTestCase {
    protected IndexForecasterActionHandler handler;

    public void testCreateOrUpdateConfigException() throws InterruptedException, IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<CreateIndexResponse> listner = (ActionListener<CreateIndexResponse>) args[0];
            listner.onFailure(new IllegalArgumentException());
            return null;
        }).when(forecastISM).initConfigIndex(any());
        when(forecastISM.doesConfigIndexExist()).thenReturn(false);

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientMock,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            null,
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof IllegalArgumentException);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
    }

    public void testUpdateConfigException() throws InterruptedException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    try {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                        );
                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(GetAction.INSTANCE)) {
                    listener.onFailure(new IllegalArgumentException());
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.PUT;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            null,
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof IllegalArgumentException);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(clientSpy, times(1)).execute(eq(GetAction.INSTANCE), any(), any());
    }

    public void testGetConfigNotExists() throws InterruptedException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    try {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                        );
                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(GetAction.INSTANCE)) {
                    GetResult notFoundResult = new GetResult("ab", "_doc", UNASSIGNED_SEQ_NO, 0, -1, false, null, null, null);
                    listener.onResponse((Response) new GetResponse(notFoundResult));
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.PUT;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            null,
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof OpenSearchStatusException);
            OpenSearchStatusException statusException = (OpenSearchStatusException) e;
            assertTrue(statusException.getMessage().contains(CommonMessages.FAIL_TO_FIND_CONFIG_MSG));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(clientSpy, times(1)).execute(eq(GetAction.INSTANCE), any(), any());
    }

    public void testFailToParse() throws InterruptedException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    try {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                        );
                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(GetAction.INSTANCE)) {
                    try {
                        listener
                            .onResponse(
                                (Response) TestHelpers
                                    .createGetResponse(AllocationId.newInitializing(), forecaster.getId(), ForecastCommonName.CONFIG_INDEX)
                            );
                    } catch (IOException e) {
                        LOG.error(e);
                    }
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);
        clusterName = mock(ClusterName.class);
        when(clusterService.getClusterName()).thenReturn(clusterName);
        when(clusterName.value()).thenReturn("test");

        method = RestRequest.Method.PUT;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof OpenSearchStatusException);
            OpenSearchStatusException statusException = (OpenSearchStatusException) e;
            assertTrue(statusException.getMessage().contains("Failed to parse config"));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(clientSpy, times(1)).execute(eq(GetAction.INSTANCE), any(), any());
    }

    public void testSearchHCForecasterException() throws InterruptedException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    try {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                        );
                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(SearchAction.INSTANCE)) {
                    listener.onFailure(new IllegalArgumentException());
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.POST;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof IllegalArgumentException);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(clientSpy, times(1)).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testSearchSingleStreamForecasterException() throws InterruptedException, IOException {
        forecaster = TestHelpers.ForecasterBuilder
            .newInstance()
            .setConfigId(forecasterId)
            .setTimeField("timestamp")
            .setIndices(ImmutableList.of("test-index"))
            .setCategoryFields(Arrays.asList())
            .build();

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    GetFieldMappingsRequest getMappingsRequest = (GetFieldMappingsRequest) request;
                    try {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                        );

                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(SearchAction.INSTANCE)) {
                    listener.onFailure(new IllegalArgumentException());
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.POST;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof IllegalArgumentException);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(clientSpy, times(1)).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testValidateCategoricalFieldException() throws InterruptedException, IOException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    GetFieldMappingsRequest getMappingsRequest = (GetFieldMappingsRequest) request;
                    try {
                        GetFieldMappingsResponse response = null;
                        if (getMappingsRequest.fields()[0].equals(categoricalField)) {
                            listener.onFailure(new IllegalArgumentException());
                        } else {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                            );
                        }

                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(SearchAction.INSTANCE)) {
                    Histogram histogram = mock(Histogram.class);
                    when(histogram.getName()).thenReturn(AggregationPrep.AGGREGATION);
                    Aggregations aggs = new Aggregations(Arrays.asList(histogram));
                    SearchResponseSections sections = new SearchResponseSections(
                        new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                        aggs,
                        null,
                        false,
                        null,
                        null,
                        1
                    );
                    listener
                        .onResponse(
                            (Response) new SearchResponse(
                                sections,
                                null,
                                0,
                                0,
                                0,
                                0L,
                                ShardSearchFailure.EMPTY_ARRAY,
                                SearchResponse.Clusters.EMPTY
                            )
                        );
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);
        clusterName = mock(ClusterName.class);
        when(clusterService.getClusterName()).thenReturn(clusterName);
        when(clusterName.value()).thenReturn("test");

        method = RestRequest.Method.POST;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            String message = String.format(Locale.ROOT, CommonMessages.FAIL_TO_GET_MAPPING_MSG, forecaster.getIndices());
            assertTrue("actual: " + e, e instanceof IllegalArgumentException);
            assertTrue("actual: " + message, e.getMessage().contains(message));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        // once for timestamp, once for categorical field
        verify(clientSpy, times(2)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(clientSpy, times(1)).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testSearchConfigInputException() throws InterruptedException, IOException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    GetFieldMappingsRequest getMappingsRequest = (GetFieldMappingsRequest) request;
                    try {
                        GetFieldMappingsResponse response = null;
                        if (getMappingsRequest.fields()[0].equals(categoricalField)) {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), categoricalField, "keyword")
                            );
                        } else {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                            );
                        }

                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(SearchAction.INSTANCE)) {
                    LOG.info(Thread.currentThread().getName());
                    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                    for (StackTraceElement element : stackTrace) {
                        LOG.info(element);
                    }
                    SearchRequest searchRequest = (SearchRequest) request;
                    if (searchRequest.indices()[0].equals(ForecastCommonName.CONFIG_INDEX)) {
                        Histogram histogram = mock(Histogram.class);
                        when(histogram.getName()).thenReturn(AggregationPrep.AGGREGATION);
                        Aggregations aggs = new Aggregations(Arrays.asList(histogram));
                        SearchResponseSections sections = new SearchResponseSections(
                            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                            aggs,
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                        listener
                            .onResponse(
                                (Response) new SearchResponse(
                                    sections,
                                    null,
                                    0,
                                    0,
                                    0,
                                    0L,
                                    ShardSearchFailure.EMPTY_ARRAY,
                                    SearchResponse.Clusters.EMPTY
                                )
                            );
                    } else {
                        listener.onFailure(new IllegalArgumentException());
                    }

                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.POST;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof IllegalArgumentException);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        // once for timestamp, once for categorical field
        verify(clientSpy, times(2)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        // validateAgainstExistingHCConfig, validateCategoricalField/searchConfigInputIndices
        verify(clientSpy, times(2)).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testCheckConfigNameExistsException() throws InterruptedException, IOException {
        forecaster = TestHelpers.ForecasterBuilder
            .newInstance()
            .setConfigId(forecasterId)
            .setTimeField("timestamp")
            .setIndices(ImmutableList.of("test-index"))
            .setFeatureAttributes(Arrays.asList())
            .setCategoryFields(Arrays.asList(categoricalField))
            .setNullImputationOption()
            .build();

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    GetFieldMappingsRequest getMappingsRequest = (GetFieldMappingsRequest) request;
                    try {
                        GetFieldMappingsResponse response = null;
                        if (getMappingsRequest.fields()[0].equals(categoricalField)) {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), categoricalField, "keyword")
                            );
                        } else {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                            );
                        }

                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(SearchAction.INSTANCE)) {
                    SearchRequest searchRequest = (SearchRequest) request;
                    Histogram histogram = mock(Histogram.class);
                    when(histogram.getName()).thenReturn(AggregationPrep.AGGREGATION);
                    Aggregations aggs = new Aggregations(Arrays.asList(histogram));
                    SearchResponseSections sections = null;
                    if (searchRequest.indices()[0].equals(ForecastCommonName.CONFIG_INDEX)) {
                        BoolQueryBuilder boolQuery = (BoolQueryBuilder) searchRequest.source().query();
                        if (boolQuery.must() != null && boolQuery.must().size() > 0) {
                            // checkConfigNameExists
                            listener.onFailure(new IllegalArgumentException());
                            return;
                        } else {
                            // validateAgainstExistingHCConfig
                            sections = new SearchResponseSections(
                                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                                aggs,
                                null,
                                false,
                                null,
                                null,
                                1
                            );
                        }
                    } else {
                        SearchHit[] hits = new SearchHit[1];
                        hits[0] = new SearchHit(randomIntBetween(1, Integer.MAX_VALUE));

                        sections = new SearchResponseSections(
                            new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0),
                            aggs,
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    }
                    listener
                        .onResponse(
                            (Response) new SearchResponse(
                                sections,
                                null,
                                0,
                                0,
                                0,
                                0L,
                                ShardSearchFailure.EMPTY_ARRAY,
                                SearchResponse.Clusters.EMPTY
                            )
                        );
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.POST;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof IllegalArgumentException);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        // once for timestamp, once for categorical field
        verify(clientSpy, times(2)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        // validateAgainstExistingHCConfig, checkConfigNameExists, validateCategoricalField/searchConfigInputIndices
        verify(clientSpy, times(3)).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testRedundantNames() throws InterruptedException, IOException {
        forecaster = TestHelpers.ForecasterBuilder
            .newInstance()
            .setConfigId(forecasterId)
            .setTimeField("timestamp")
            .setIndices(ImmutableList.of("test-index"))
            .setFeatureAttributes(Arrays.asList())
            .setCategoryFields(Arrays.asList(categoricalField))
            .setNullImputationOption()
            .build();

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    GetFieldMappingsRequest getMappingsRequest = (GetFieldMappingsRequest) request;
                    try {
                        GetFieldMappingsResponse response = null;
                        if (getMappingsRequest.fields()[0].equals(categoricalField)) {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), categoricalField, "keyword")
                            );
                        } else {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                            );
                        }

                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(SearchAction.INSTANCE)) {
                    SearchRequest searchRequest = (SearchRequest) request;
                    Histogram histogram = mock(Histogram.class);
                    when(histogram.getName()).thenReturn(AggregationPrep.AGGREGATION);
                    Aggregations aggs = new Aggregations(Arrays.asList(histogram));
                    SearchResponseSections sections = null;
                    if (searchRequest.indices()[0].equals(ForecastCommonName.CONFIG_INDEX)) {
                        BoolQueryBuilder boolQuery = (BoolQueryBuilder) searchRequest.source().query();
                        if (boolQuery.must() != null && boolQuery.must().size() > 0) {
                            // checkConfigNameExists
                            SearchHit[] hits = new SearchHit[1];
                            hits[0] = new SearchHit(randomIntBetween(1, Integer.MAX_VALUE));

                            sections = new SearchResponseSections(
                                new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0),
                                aggs,
                                null,
                                false,
                                null,
                                null,
                                1
                            );
                        } else {
                            // validateAgainstExistingHCConfig
                            sections = new SearchResponseSections(
                                new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                                aggs,
                                null,
                                false,
                                null,
                                null,
                                1
                            );
                        }
                    } else {
                        SearchHit[] hits = new SearchHit[1];
                        hits[0] = new SearchHit(randomIntBetween(1, Integer.MAX_VALUE));

                        sections = new SearchResponseSections(
                            new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0),
                            aggs,
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    }
                    listener
                        .onResponse(
                            (Response) new SearchResponse(
                                sections,
                                null,
                                0,
                                0,
                                0,
                                0L,
                                ShardSearchFailure.EMPTY_ARRAY,
                                SearchResponse.Clusters.EMPTY
                            )
                        );
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.POST;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof OpenSearchStatusException);
            String error = handler.getDuplicateConfigErrorMsg(forecaster.getName());
            assertTrue("actual: " + e.getMessage(), e.getMessage().contains(error));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        // once for timestamp, once for categorical field
        verify(clientSpy, times(2)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        // validateAgainstExistingHCConfig, checkConfigNameExists, validateCategoricalField/searchConfigInputIndices
        verify(clientSpy, times(3)).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testIndexConfigVersionConflict() throws InterruptedException, IOException {
        forecaster = TestHelpers.ForecasterBuilder
            .newInstance()
            .setConfigId(forecasterId)
            .setTimeField("timestamp")
            .setIndices(ImmutableList.of("test-index"))
            .setFeatureAttributes(Arrays.asList())
            .setCategoryFields(Arrays.asList(categoricalField))
            .setNullImputationOption()
            .build();

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    GetFieldMappingsRequest getMappingsRequest = (GetFieldMappingsRequest) request;
                    try {
                        GetFieldMappingsResponse response = null;
                        if (getMappingsRequest.fields()[0].equals(categoricalField)) {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), categoricalField, "keyword")
                            );
                        } else {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                            );
                        }

                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(SearchAction.INSTANCE)) {
                    SearchRequest searchRequest = (SearchRequest) request;
                    Histogram histogram = mock(Histogram.class);
                    when(histogram.getName()).thenReturn(AggregationPrep.AGGREGATION);
                    Aggregations aggs = new Aggregations(Arrays.asList(histogram));
                    SearchResponseSections sections = null;
                    if (searchRequest.indices()[0].equals(ForecastCommonName.CONFIG_INDEX)) {
                        sections = new SearchResponseSections(
                            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                            aggs,
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    } else {
                        SearchHit[] hits = new SearchHit[1];
                        hits[0] = new SearchHit(randomIntBetween(1, Integer.MAX_VALUE));

                        sections = new SearchResponseSections(
                            new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0),
                            aggs,
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    }
                    listener
                        .onResponse(
                            (Response) new SearchResponse(
                                sections,
                                null,
                                0,
                                0,
                                0,
                                0L,
                                ShardSearchFailure.EMPTY_ARRAY,
                                SearchResponse.Clusters.EMPTY
                            )
                        );
                } else if (action.equals(IndexAction.INSTANCE)) {
                    listener.onFailure(new IllegalArgumentException("version conflict"));
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.POST;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            1L,
            1L,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof IllegalArgumentException);
            String error = "There was a problem updating the config:[" + forecaster.getId() + "]";
            assertTrue("actual: " + e.getMessage(), e.getMessage().contains(error));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        // once for timestamp, once for categorical field
        verify(clientSpy, times(2)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        // validateAgainstExistingHCConfig, checkConfigNameExists, validateCategoricalField/searchConfigInputIndices
        verify(clientSpy, times(3)).execute(eq(SearchAction.INSTANCE), any(), any());
        // indexConfig
        verify(clientSpy, times(1)).execute(eq(IndexAction.INSTANCE), any(), any());
    }

    public void testIndexConfigException() throws InterruptedException, IOException {
        forecaster = TestHelpers.ForecasterBuilder
            .newInstance()
            .setConfigId(forecasterId)
            .setTimeField("timestamp")
            .setIndices(ImmutableList.of("test-index"))
            .setFeatureAttributes(Arrays.asList())
            .setCategoryFields(Arrays.asList(categoricalField))
            .setNullImputationOption()
            .build();

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    GetFieldMappingsRequest getMappingsRequest = (GetFieldMappingsRequest) request;
                    try {
                        GetFieldMappingsResponse response = null;
                        if (getMappingsRequest.fields()[0].equals(categoricalField)) {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), categoricalField, "keyword")
                            );
                        } else {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                            );
                        }

                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(SearchAction.INSTANCE)) {
                    SearchRequest searchRequest = (SearchRequest) request;
                    Histogram histogram = mock(Histogram.class);
                    when(histogram.getName()).thenReturn(AggregationPrep.AGGREGATION);
                    Aggregations aggs = new Aggregations(Arrays.asList(histogram));
                    SearchResponseSections sections = null;
                    if (searchRequest.indices()[0].equals(ForecastCommonName.CONFIG_INDEX)) {
                        sections = new SearchResponseSections(
                            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                            aggs,
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    } else {
                        SearchHit[] hits = new SearchHit[1];
                        hits[0] = new SearchHit(randomIntBetween(1, Integer.MAX_VALUE));

                        sections = new SearchResponseSections(
                            new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0),
                            aggs,
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    }
                    listener
                        .onResponse(
                            (Response) new SearchResponse(
                                sections,
                                null,
                                0,
                                0,
                                0,
                                0L,
                                ShardSearchFailure.EMPTY_ARRAY,
                                SearchResponse.Clusters.EMPTY
                            )
                        );
                } else if (action.equals(IndexAction.INSTANCE)) {
                    listener.onFailure(new IllegalArgumentException());
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.POST;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            1L,
            1L,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof IllegalArgumentException);
            assertEquals("actual: " + e.getMessage(), null, e.getMessage());
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        // once for timestamp, once for categorical field
        verify(clientSpy, times(2)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        // validateAgainstExistingHCConfig, checkConfigNameExists, validateCategoricalField/searchConfigInputIndices
        verify(clientSpy, times(3)).execute(eq(SearchAction.INSTANCE), any(), any());
        // indexConfig
        verify(clientSpy, times(1)).execute(eq(IndexAction.INSTANCE), any(), any());
    }

    public void testIndexShardFailure() throws InterruptedException, IOException {
        forecaster = TestHelpers.ForecasterBuilder
            .newInstance()
            .setConfigId(forecasterId)
            .setTimeField("timestamp")
            .setIndices(ImmutableList.of("test-index"))
            .setFeatureAttributes(Arrays.asList())
            .setCategoryFields(Arrays.asList(categoricalField))
            .setNullImputationOption()
            .build();

        IndexResponse.Builder notCreatedResponse = new IndexResponse.Builder();
        notCreatedResponse.setResult(Result.CREATED);
        notCreatedResponse.setShardId(new ShardId("index", "_uuid", 0));
        notCreatedResponse.setId("blah");
        notCreatedResponse.setVersion(1L);

        ReplicationResponse.ShardInfo.Failure[] failures = new ReplicationResponse.ShardInfo.Failure[1];
        failures[0] = new ReplicationResponse.ShardInfo.Failure(
            new ShardId("index", "_uuid", 1),
            null,
            new Exception("shard failed"),
            RestStatus.GATEWAY_TIMEOUT,
            false
        );
        notCreatedResponse.setShardInfo(new ShardInfo(2, 1, failures));
        IndexResponse indexResponse = notCreatedResponse.build();

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    GetFieldMappingsRequest getMappingsRequest = (GetFieldMappingsRequest) request;
                    try {
                        GetFieldMappingsResponse response = null;
                        if (getMappingsRequest.fields()[0].equals(categoricalField)) {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), categoricalField, "keyword")
                            );
                        } else {
                            response = new GetFieldMappingsResponse(
                                TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                            );
                        }

                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(SearchAction.INSTANCE)) {
                    SearchRequest searchRequest = (SearchRequest) request;
                    Histogram histogram = mock(Histogram.class);
                    when(histogram.getName()).thenReturn(AggregationPrep.AGGREGATION);
                    Aggregations aggs = new Aggregations(Arrays.asList(histogram));
                    SearchResponseSections sections = null;
                    if (searchRequest.indices()[0].equals(ForecastCommonName.CONFIG_INDEX)) {
                        sections = new SearchResponseSections(
                            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                            aggs,
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    } else {
                        SearchHit[] hits = new SearchHit[1];
                        hits[0] = new SearchHit(randomIntBetween(1, Integer.MAX_VALUE));

                        sections = new SearchResponseSections(
                            new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0),
                            aggs,
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    }
                    listener
                        .onResponse(
                            (Response) new SearchResponse(
                                sections,
                                null,
                                0,
                                0,
                                0,
                                0L,
                                ShardSearchFailure.EMPTY_ARRAY,
                                SearchResponse.Clusters.EMPTY
                            )
                        );
                } else if (action.equals(IndexAction.INSTANCE)) {

                    listener.onResponse((Response) indexResponse);
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.POST;

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            1L,
            1L,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            mock(ForecastTaskManager.class),
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof OpenSearchStatusException);
            String errorMsg = handler.checkShardsFailure(indexResponse);
            assertEquals("actual: " + e.getMessage(), errorMsg, e.getMessage());
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        // once for timestamp, once for categorical field
        verify(clientSpy, times(2)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        // validateAgainstExistingHCConfig, checkConfigNameExists, validateCategoricalField/searchConfigInputIndices
        verify(clientSpy, times(3)).execute(eq(SearchAction.INSTANCE), any(), any());
        // indexConfig
        verify(clientSpy, times(1)).execute(eq(IndexAction.INSTANCE), any(), any());
    }

    public void testCreateMappingException() throws InterruptedException, IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<CreateIndexResponse> listner = (ActionListener<CreateIndexResponse>) args[0];
            listner.onResponse(new CreateIndexResponse(false, false, "blah"));
            return null;
        }).when(forecastISM).initConfigIndex(any());
        when(forecastISM.doesIndexExist(anyString())).thenReturn(false);
        when(forecastISM.doesAliasExist(anyString())).thenReturn(false);
        when(forecastISM.doesConfigIndexExist()).thenReturn(false);

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientMock,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            null,
            searchFeatureDao,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof OpenSearchStatusException);
            assertEquals(
                "actual: " + e.getMessage(),
                "Created " + ForecastCommonName.CONFIG_INDEX + "with mappings call not acknowledged.",
                e.getMessage()
            );
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
    }

    public void testShareWithResourceAuthzBranch() throws InterruptedException, IOException {
        Settings testSettings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES.getKey(), true)
            .put(OPENSEARCH_RESOURCE_SHARING_ENABLED, true)
            .build();

        User user = randomUser();

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    try {
                        var response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(forecaster.getIndices().get(0), "timestamp", "date")
                        );
                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        listener.onFailure(e);
                    }
                } else if (action.equals(GetAction.INSTANCE)) {
                    GetResult notFound = new GetResult(
                        ForecastIndex.CONFIG.getIndexName(),       // index name
                        forecaster.getId(),            // id
                        UNASSIGNED_SEQ_NO,
                        0,
                        -1,
                        false,
                        null,
                        null,
                        null
                    );
                    listener.onResponse((Response) new GetResponse(notFound));
                } else if (action.equals(SearchAction.INSTANCE)) {
                    SearchRequest searchRequest = (SearchRequest) request;

                    SearchResponseSections sections;
                    if ("test-index".equals(searchRequest.indices()[0])) {
                        // Simulate “existing config” → return ONE hit
                        SearchHit hit = new SearchHit(randomIntBetween(0, Integer.MAX_VALUE));
                        sections = new SearchResponseSections(
                            new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0),
                            mock(Aggregations.class),
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    } else {
                        // For any other index, return ZERO hits
                        sections = new SearchResponseSections(
                            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
                            mock(Aggregations.class),
                            null,
                            false,
                            null,
                            null,
                            1
                        );
                    }

                    SearchResponse resp = new SearchResponse(
                        sections,
                        /* scrollId */ null,
                        /* totalShards */ 1,
                        /* successful */ 1,
                        /* skipped */ 0,
                        /* tookInMillis */0L,
                        ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY
                    );
                    listener.onResponse((Response) resp);
                } else if (action.equals(IndexAction.INSTANCE)) {
                    IndexRequest ir = (IndexRequest) request;
                    // simulate that indexing created a new config doc
                    ShardId sid = new ShardId(ir.index(), UUIDs.randomBase64UUID(), 0);
                    IndexResponse irsp = new IndexResponse(
                        sid,
                        ir.id(),
                        /*seqNo*/ 1L,
                        /*primaryTerm*/ 1L,
                        /*version*/ 1L,
                        /*created*/ true
                    );
                    irsp.setShardInfo(new ShardInfo(1, 1));
                    listener.onResponse((Response) irsp);
                } else {
                    fail("Unexpected action: " + action.name());
                }
            }
        };
        NodeClient clientSpy = spy(client);
        forecaster = TestHelpers.ForecasterBuilder
            .newInstance()
            .setConfigId(forecasterId)
            .setTimeField("timestamp")
            .setIndices(ImmutableList.of("test-index"))
            .setFeatureAttributes(Collections.emptyList())
            .setCategoryFields(Collections.emptyList())
            .setNullImputationOption()
            .build();

        handler = new IndexForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            mock(TransportService.class),
            forecastISM,
            forecaster.getId(),
            seqNo,
            primaryTerm,
            refreshPolicy,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            user,
            null,
            searchFeatureDao,
            testSettings
        );

        ResourceSharingClient mockClient = mock(ResourceSharingClient.class);
        ResourceSharingClientAccessor.getInstance().setResourceSharingClient(mockClient);

        // capture when the final onResponse() is called
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        @SuppressWarnings("unchecked")
        ActionListener<IndexForecasterResponse> testListener = new ActionListener<>() {
            @Override
            public void onResponse(IndexForecasterResponse r) {
                responseCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("should not fail: " + e);
            }
        };

        handler.start(testListener);

        // verify share(...) was invoked with the right arguments
        @SuppressWarnings("unchecked")
        var idCaptor = ArgumentCaptor.forClass(String.class);
        @SuppressWarnings("unchecked")
        var idxCaptor = ArgumentCaptor.forClass(String.class);
        var shareWithCaptor = ArgumentCaptor.forClass(ShareWith.class);
        @SuppressWarnings("unchecked")
        var listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        verify(mockClient).share(idCaptor.capture(), idxCaptor.capture(), shareWithCaptor.capture(), listenerCaptor.capture());

        assertEquals("123", idCaptor.getValue());
        assertEquals(ForecastIndex.CONFIG.getIndexName(), idxCaptor.getValue());

        ShareWith sw = shareWithCaptor.getValue();
        Recipients rec = sw.atAccessLevel(PLACE_HOLDER);
        // the Recipients should carry exactly the user's backend roles
        assertEquals(Set.copyOf(user.getBackendRoles()), rec.getRecipients().get(Recipient.BACKEND_ROLES));

        // simulate a successful share callback
        listenerCaptor.getValue().onResponse(null);

        // testListener should have fired
        assertTrue(responseCalled.get());
    }
}
