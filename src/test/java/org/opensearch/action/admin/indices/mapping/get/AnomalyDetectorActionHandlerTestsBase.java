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

package org.opensearch.action.admin.indices.mapping.get;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.feature.SearchFeatureDao;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;

public class AnomalyDetectorActionHandlerTestsBase<T extends ActionResponse> extends AbstractADTest {

    static ThreadPool threadPool;
    protected String TEXT_FIELD_TYPE = "text";
    protected AbstractAnomalyDetectorActionHandler<T> handler;
    protected ClusterService clusterService;
    protected ActionListener<T> channel;
    protected NodeClient clientMock;
    protected TransportService transportService;
    protected AnomalyDetectionIndices anomalyDetectionIndices;
    protected String detectorId;
    protected Long seqNo;
    protected Long primaryTerm;
    protected AnomalyDetector detector;
    protected WriteRequest.RefreshPolicy refreshPolicy;
    protected TimeValue requestTimeout;
    protected Integer maxSingleEntityAnomalyDetectors;
    protected Integer maxMultiEntityAnomalyDetectors;
    protected Integer maxAnomalyFeatures;
    protected Settings settings;
    protected RestRequest.Method method;
    protected ADTaskManager adTaskManager;
    protected SearchFeatureDao searchFeatureDao;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("IndexAnomalyDetectorJobActionHandlerTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        settings = Settings.EMPTY;
        clusterService = mock(ClusterService.class);
        channel = mock(ActionListener.class);
        clientMock = spy(new NodeClient(settings, null));
        transportService = mock(TransportService.class);

        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        when(anomalyDetectionIndices.doesAnomalyDetectorIndexExist()).thenReturn(true);

        detectorId = "123";
        seqNo = 0L;
        primaryTerm = 0L;

        refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;

        String field = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        requestTimeout = new TimeValue(1000L);

        maxSingleEntityAnomalyDetectors = 1000;

        maxMultiEntityAnomalyDetectors = 10;

        maxAnomalyFeatures = 5;

        method = RestRequest.Method.POST;

        adTaskManager = mock(ADTaskManager.class);

        searchFeatureDao = mock(SearchFeatureDao.class);
    }

    @SuppressWarnings("unchecked")
    public NodeClient getMockClient(
        SearchResponse expectedSearchDetectorCountResponse,
        SearchResponse expectedDuplicateDetectorNameCountResponse,
        SearchResponse expectedSearchDataSourceResponse,
        boolean featureQueryFailed,
        boolean invalidFeatureQuery,
        GetResponse expectedGetDetectorResponse,
        String categoryField,
        String filedTypeName
    ) {
        return new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(SearchAction.INSTANCE)) {
                        assertTrue(request instanceof SearchRequest);
                        SearchRequest searchRequest = (SearchRequest) request;
                        if (searchRequest.indices()[0].equals(ANOMALY_DETECTORS_INDEX)) {
                            if (searchRequest.source().query().equals(QueryBuilders.matchAllQuery())) {
                                // check all detectors count
                                listener.onResponse((Response) expectedSearchDetectorCountResponse);
                            } else {
                                if (searchRequest.source().size() == 0) {
                                    // check multi entity detectors count
                                    listener.onResponse((Response) expectedSearchDetectorCountResponse);
                                } else {
                                    // check detector name exists
                                    listener.onResponse((Response) expectedDuplicateDetectorNameCountResponse);
                                }
                            }
                        } else {
                            // search against data source index
                            if (searchRequest.source().aggregations() != null && featureQueryFailed) {
                                // when aggregation is not null, it is making feature query search
                                if (invalidFeatureQuery) {
                                    listener
                                        .onFailure(new SearchPhaseExecutionException("phaseName", "message", new ShardSearchFailure[0]));
                                } else {
                                    listener.onFailure(new RuntimeException());
                                }

                            } else {
                                listener.onResponse((Response) expectedSearchDataSourceResponse);
                            }
                        }
                    } else if (action.equals(GetAction.INSTANCE)) {
                        assertTrue(request instanceof GetRequest);
                        listener.onResponse((Response) expectedGetDetectorResponse);
                    } else {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), categoryField, filedTypeName)
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}
