/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.action.admin.indices.mapping.get;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableList;

public class AbstractForecasterActionHandlerTestCase extends AbstractTimeSeriesTest {

    protected ClusterService clusterService;
    protected ActionListener<ValidateConfigResponse> channel;
    protected TransportService transportService;
    protected ForecastIndexManagement forecastISM;
    protected String forecasterId;
    protected Long seqNo;
    protected Long primaryTerm;
    protected Forecaster forecaster;
    protected WriteRequest.RefreshPolicy refreshPolicy;
    protected TimeValue requestTimeout;
    protected Integer maxSingleStreamForecasters;
    protected Integer maxHCForecasters;
    protected Integer maxForecastFeatures;
    protected Integer maxCategoricalFields;
    protected Settings settings;
    protected RestRequest.Method method;
    protected TaskManager<TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement> forecastTaskManager;
    protected SearchFeatureDao searchFeatureDao;
    protected Clock clock;
    @Mock
    protected Client clientMock;
    @Mock
    protected ThreadPool threadPool;
    protected ThreadContext threadContext;
    protected SecurityClientUtil clientUtil;
    protected String categoricalField;
    // @Mock
    protected ClusterName clusterName;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);

        settings = Settings.EMPTY;

        clusterService = mock(ClusterService.class);
        ClusterName clusterName = new ClusterName("test");
        clusterName = mock(ClusterName.class);
        when(clusterService.getClusterName()).thenReturn(clusterName);
        when(clusterName.value()).thenReturn("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        channel = mock(ActionListener.class);
        transportService = mock(TransportService.class);

        forecastISM = mock(ForecastIndexManagement.class);
        when(forecastISM.doesConfigIndexExist()).thenReturn(true);

        forecasterId = "123";
        seqNo = 0L;
        primaryTerm = 0L;
        clock = mock(Clock.class);

        refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;

        categoricalField = "a";
        forecaster = TestHelpers.ForecasterBuilder
            .newInstance()
            .setConfigId(forecasterId)
            .setTimeField("timestamp")
            .setIndices(ImmutableList.of("test-index"))
            .setCategoryFields(Arrays.asList(categoricalField))
            .build();

        requestTimeout = new TimeValue(1000L);
        maxSingleStreamForecasters = 1000;
        maxHCForecasters = 10;
        maxForecastFeatures = 5;
        maxCategoricalFields = 10;
        method = RestRequest.Method.POST;
        forecastTaskManager = mock(ForecastTaskManager.class);
        searchFeatureDao = mock(SearchFeatureDao.class);

        threadContext = new ThreadContext(settings);
        Mockito.doReturn(threadPool).when(clientMock).threadPool();
        Mockito.doReturn(threadContext).when(threadPool).getThreadContext();

        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, settings);
    }

}
