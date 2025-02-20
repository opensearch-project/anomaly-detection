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

package org.opensearch.timeseries.feature;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.MultiSearchResponse.Item;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;
import org.opensearch.script.TemplateScript.Factory;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.dataprocessor.Imputer;
import org.opensearch.timeseries.dataprocessor.LinearUniformImputer;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * Due to https://tinyurl.com/2y265s2w, tests with and without @Parameters annotation
 * are incompatible with each other. This class tests SearchFeatureDao using @Parameters,
 * while SearchFeatureDaoTests do not use @Parameters.
 *
 */
@RunWith(JUnitParamsRunner.class)
public class SearchFeatureDaoParamTests {

    private SearchFeatureDao searchFeatureDao;

    @Mock
    private Client client;
    @Mock
    private ScriptService scriptService;
    @Mock
    private NamedXContentRegistry xContent;
    private SecurityClientUtil clientUtil;

    @Mock
    private Factory factory;
    @Mock
    private TemplateScript templateScript;
    @Mock
    private ActionFuture<SearchResponse> searchResponseFuture;
    @Mock
    private ActionFuture<MultiSearchResponse> multiSearchResponseFuture;
    @Mock
    private SearchResponse searchResponse;
    @Mock
    private MultiSearchResponse multiSearchResponse;
    @Mock
    private Item multiSearchResponseItem;
    @Mock
    private Aggregations aggs;
    @Mock
    private Max max;
    @Mock
    private NodeStateManager stateManager;

    @Mock
    private AnomalyDetector detector;

    @Mock
    private ThreadPool threadPool;

    @Mock
    private ClusterService clusterService;

    @Mock
    private Clock clock;

    private SearchRequest searchRequest;
    private MultiSearchRequest multiSearchRequest;
    private IntervalTimeConfiguration detectionInterval;
    private String detectorId;
    private Imputer imputer;
    private Settings settings;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        // PowerMockito.mockStatic(ParseUtils.class);

        imputer = new LinearUniformImputer(false);

        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        settings = Settings.EMPTY;

        when(client.threadPool()).thenReturn(threadPool);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        clientUtil = new SecurityClientUtil(nodeStateManager, settings);
        searchFeatureDao = spy(new SearchFeatureDao(client, xContent, clientUtil, settings, null, TimeSeriesSettings.NUM_SAMPLES_PER_TREE));

        detectionInterval = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
        detectorId = "123";

        when(detector.getId()).thenReturn(detectorId);
        when(detector.getTimeField()).thenReturn("testTimeField");
        when(detector.getIndices()).thenReturn(Arrays.asList("testIndices"));
        when(detector.getInterval()).thenReturn(detectionInterval);
        when(detector.getFilterQuery()).thenReturn(QueryBuilders.matchAllQuery());
        when(detector.getCategoryFields()).thenReturn(Collections.singletonList("a"));

        searchRequest = new SearchRequest(detector.getIndices().toArray(new String[0]));

        when(max.getName()).thenReturn(CommonName.AGG_NAME_MAX_TIME);
        List<Aggregation> list = new ArrayList<>();
        list.add(max);
        Aggregations aggregations = new Aggregations(list);
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1f);
        when(searchResponse.getHits()).thenReturn(hits);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(eq(searchRequest), any());
        when(searchResponse.getAggregations()).thenReturn(aggregations);

        multiSearchRequest = new MultiSearchRequest();
        SearchRequest request = new SearchRequest(detector.getIndices().toArray(new String[0]));
        multiSearchRequest.add(request);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<MultiSearchResponse> listener = (ActionListener<MultiSearchResponse>) args[1];
            listener.onResponse(multiSearchResponse);
            return null;
        }).when(client).multiSearch(eq(multiSearchRequest), any());
        when(multiSearchResponse.getResponses()).thenReturn(new Item[] { multiSearchResponseItem });
        when(multiSearchResponseItem.getResponse()).thenReturn(searchResponse);
    }

    @Test
    @Parameters(method = "getFeaturesForPeriodData")
    @SuppressWarnings("unchecked")
    public void getFeaturesForPeriod_returnExpectedToListener(List<Aggregation> aggs, List<String> featureIds, double[] expected)
        throws Exception {

        long start = 100L;
        long end = 200L;
        when(searchResponse.getAggregations()).thenReturn(new Aggregations(aggs));
        when(detector.getEnabledFeatureIds()).thenReturn(featureIds);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        ActionListener<Optional<double[]>> listener = mock(ActionListener.class);
        searchFeatureDao.getFeaturesForPeriod(detector, start, end, listener);

        ArgumentCaptor<Optional<double[]>> captor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(captor.capture());
        Optional<double[]> result = captor.getValue();
        assertTrue(Arrays.equals(expected, result.orElse(null)));
    }

    @SuppressWarnings("unchecked")
    private Object[] getFeaturesForPeriodData() {
        String maxName = "max";
        double maxValue = 2;
        Max max = mock(Max.class);
        when(max.value()).thenReturn(maxValue);
        when(max.getName()).thenReturn(maxName);

        String percentileName = "percentile";
        double percentileValue = 1;
        InternalTDigestPercentiles percentiles = mock(InternalTDigestPercentiles.class);
        Iterator<Percentile> percentilesIterator = mock(Iterator.class);
        Percentile percentile = mock(Percentile.class);
        when(percentiles.iterator()).thenReturn(percentilesIterator);
        when(percentilesIterator.hasNext()).thenReturn(true);
        when(percentilesIterator.next()).thenReturn(percentile);
        when(percentile.getValue()).thenReturn(percentileValue);
        when(percentiles.getName()).thenReturn(percentileName);

        String missingName = "missing";
        Max missing = mock(Max.class);
        when(missing.value()).thenReturn(Double.NaN);
        when(missing.getName()).thenReturn(missingName);

        String infinityName = "infinity";
        Max infinity = mock(Max.class);
        when(infinity.value()).thenReturn(Double.POSITIVE_INFINITY);
        when(infinity.getName()).thenReturn(infinityName);

        String emptyName = "empty";
        InternalTDigestPercentiles empty = mock(InternalTDigestPercentiles.class);
        Iterator<Percentile> emptyIterator = mock(Iterator.class);
        when(empty.iterator()).thenReturn(emptyIterator);
        when(emptyIterator.hasNext()).thenReturn(false);
        when(empty.getName()).thenReturn(emptyName);

        return new Object[] {
            new Object[] { asList(max), asList(maxName), new double[] { maxValue }, },
            new Object[] { asList(percentiles), asList(percentileName), new double[] { percentileValue } },
            // we keep missing data
            new Object[] { asList(missing), asList(missingName), new double[] { Double.NaN } },
            new Object[] { asList(infinity), asList(infinityName), new double[] { Double.NaN } },
            new Object[] { asList(max, percentiles), asList(maxName, percentileName), new double[] { maxValue, percentileValue } },
            new Object[] { asList(max, percentiles), asList(percentileName, maxName), new double[] { percentileValue, maxValue } },
            new Object[] {
                asList(max, percentiles, missing),
                asList(maxName, percentileName, missingName),
                new double[] { maxValue, percentileValue, Double.NaN } }, };
    }

    private Object[] getFeaturesForSampledPeriodsData() {
        long endTime = 300_000;
        int maxStride = 4;
        return new Object[] {

            // No data

            new Object[] { new Long[0][0], new double[0][0], endTime, 1, 1, Optional.empty() },

            // 1 data point

            new Object[] {
                new Long[][] { { 240_000L, 300_000L } },
                new double[][] { { 1, 2 } },
                endTime,
                1,
                1,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 } }, 1)) },

            new Object[] {
                new Long[][] { { 240_000L, 300_000L } },
                new double[][] { { 1, 2 } },
                endTime,
                1,
                3,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 } }, 1)) },

            // 2 data points

            new Object[] {
                new Long[][] { { 180_000L, 240_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 2, 4 } },
                endTime,
                1,
                2,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 2, 4 } }, 1)) },

            new Object[] {
                new Long[][] { { 180_000L, 240_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 2, 4 } },
                endTime,
                1,
                1,
                Optional.of(new SimpleEntry<>(new double[][] { { 2, 4 } }, 1)) },

            new Object[] {
                new Long[][] { { 180_000L, 240_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 2, 4 } },
                endTime,
                4,
                2,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 2, 4 } }, 1)) },

            new Object[] {
                new Long[][] { { 0L, 60_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 2, 4 } },
                endTime,
                4,
                2,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 2, 4 } }, 4)) },

            // 5 data points

            new Object[] {
                new Long[][] {
                    { 0L, 60_000L },
                    { 60_000L, 120_000L },
                    { 120_000L, 180_000L },
                    { 180_000L, 240_000L },
                    { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } },
                endTime,
                4,
                10,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } }, 1)) },

            new Object[] {
                new Long[][] { { 0L, 60_000L }, { 60_000L, 120_000L }, { 180_000L, 240_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 3, 4 }, { 7, 8 }, { 9, 10 } },
                endTime,
                4,
                10,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } }, 1)) },

            new Object[] {
                new Long[][] { { 0L, 60_000L }, { 120_000L, 180_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 5, 6 }, { 9, 10 } },
                endTime,
                4,
                10,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } }, 1)) },

            new Object[] {
                new Long[][] { { 0L, 60_000L }, { 240_000L, 300_000L } },
                new double[][] { { 1, 2 }, { 9, 10 } },
                endTime,
                4,
                10,
                Optional.of(new SimpleEntry<>(new double[][] { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 }, { 9, 10 } }, 1)) }, };
    }
}
