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

package org.opensearch.ad.feature;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.dataprocessor.LinearUniformImputer;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.Features;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("unchecked")
public class FeatureManagerTests {

    // configuration
    private int trainSampleTimeRangeInHours;
    private int minTrainSamples;
    private int shingleSize;
    private double maxMissingPointsRate;
    private int maxNeighborDistance;
    private double previewSampleRate;
    private int maxPreviewSamples;
    private long intervalInMilliseconds;

    @Mock
    private AnomalyDetector detector;

    @Mock
    private SearchFeatureDao searchFeatureDao;

    @Mock
    private Clock clock;

    @Mock
    private ThreadPool threadPool;

    private FeatureManager featureManager;

    private String detectorId;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        trainSampleTimeRangeInHours = 1;
        minTrainSamples = 4;
        shingleSize = 3;
        maxMissingPointsRate = 0.67;
        maxNeighborDistance = 2;
        previewSampleRate = 0.5;
        maxPreviewSamples = 2;

        detectorId = "id";
        when(detector.getId()).thenReturn(detectorId);
        when(detector.getShingleSize()).thenReturn(shingleSize);
        IntervalTimeConfiguration detectorIntervalTimeConfig = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
        intervalInMilliseconds = detectorIntervalTimeConfig.toDuration().toMillis();
        when(detector.getIntervalInMilliseconds()).thenReturn(intervalInMilliseconds);

        ExecutorService executorService = mock(ExecutorService.class);

        when(threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        this.featureManager = spy(
            new FeatureManager(
                searchFeatureDao,
                new LinearUniformImputer(false),
                trainSampleTimeRangeInHours,
                minTrainSamples,
                maxMissingPointsRate,
                maxNeighborDistance,
                previewSampleRate,
                maxPreviewSamples,
                threadPool
            )
        );
    }

    private Object[] getColdStartDataTestData() {
        double[][] samples = new double[][] { { 1.0 } };
        return new Object[] {
            new Object[] { 1L, new SimpleEntry<>(samples, 1), 1, samples },
            new Object[] { 1L, null, 1, null },
            new Object[] { null, new SimpleEntry<>(samples, 1), 1, null },
            new Object[] { null, null, 1, null }, };
    }

    private Object[] getTrainDataTestData() {
        List<Entry<Long, Long>> ranges = asList(
            entry(0L, 900_000L),
            entry(900_000L, 1_800_000L),
            entry(1_800_000L, 2_700_000L),
            entry(2_700_000L, 3_600_000L)
        );
        return new Object[] {
            new Object[] { 3_600_000L, ranges, asList(ar(1), ar(2), ar(3), ar(4)), new double[][] { { 1, 2, 3, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(), ar(2), ar(3), ar(4)), new double[][] { { 2, 2, 3, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(1), ar(), ar(3), ar(4)), new double[][] { { 1, 3, 3, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(1), ar(2), ar(), ar(4)), new double[][] { { 1, 2, 4, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(1), ar(), ar(), ar(4)), new double[][] { { 1, 1, 4, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(), ar(2), ar(), ar(4)), new double[][] { { 2, 2, 4, 4 } } },
            new Object[] { 3_600_000L, ranges, asList(ar(), ar(), ar(3), ar(4)), null },
            new Object[] { 3_600_000L, ranges, asList(ar(1), empty(), empty(), empty()), null },
            new Object[] { 3_600_000L, ranges, asList(empty(), empty(), empty(), ar(4)), null },
            new Object[] { 3_600_000L, ranges, asList(empty(), empty(), empty(), empty()), null },
            new Object[] { null, null, null, null } };
    }

    @Test
    @Parameters(method = "getTrainDataTestData")
    public void getColdStartData_returnExpectedToListener(
        Long latestTime,
        List<Entry<Long, Long>> sampleRanges,
        List<Optional<double[]>> samples,
        double[][] expected
    ) throws Exception {
        long detectionInterval = (new IntervalTimeConfiguration(15, ChronoUnit.MINUTES)).toDuration().toMillis();
        when(detector.getIntervalInMilliseconds()).thenReturn(detectionInterval);
        when(detector.getShingleSize()).thenReturn(4);
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.ofNullable(latestTime));
            return null;
        }).when(searchFeatureDao).getLatestDataTime(eq(detector), eq(Optional.empty()), eq(AnalysisType.AD), any(ActionListener.class));
        if (latestTime != null) {
            doAnswer(invocation -> {
                ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(3);
                listener.onResponse(samples);
                return null;
            })
                .when(searchFeatureDao)
                .getFeatureSamplesForPeriods(eq(detector), eq(sampleRanges), eq(AnalysisType.AD), any(ActionListener.class));
        }

        ActionListener<Optional<double[][]>> listener = mock(ActionListener.class);
        featureManager = spy(
            new FeatureManager(
                searchFeatureDao,
                new LinearUniformImputer(false),
                trainSampleTimeRangeInHours,
                minTrainSamples,
                0.5, /*maxMissingPointsRate*/
                1, /*maxNeighborDistance*/
                previewSampleRate,
                maxPreviewSamples,
                threadPool
            )
        );
        featureManager.getColdStartData(detector, listener);

        ArgumentCaptor<Optional<double[][]>> captor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(captor.capture());
        Optional<double[][]> result = captor.getValue();
        assertTrue(Arrays.deepEquals(expected, result.orElse(null)));
    }

    @Test
    public void getColdStartData_throwToListener_whenSearchFail() {
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(searchFeatureDao).getLatestDataTime(eq(detector), eq(Optional.empty()), eq(AnalysisType.AD), any(ActionListener.class));

        ActionListener<Optional<double[][]>> listener = mock(ActionListener.class);
        featureManager.getColdStartData(detector, listener);

        verify(listener).onFailure(any(Exception.class));
    }

    @Test
    public void getColdStartData_throwToListener_onQueryCreationError() throws Exception {
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.ofNullable(0L));
            return null;
        }).when(searchFeatureDao).getLatestDataTime(eq(detector), eq(Optional.empty()), eq(AnalysisType.AD), any(ActionListener.class));
        doThrow(IOException.class)
            .when(searchFeatureDao)
            .getFeatureSamplesForPeriods(eq(detector), any(), eq(AnalysisType.AD), any(ActionListener.class));

        ActionListener<Optional<double[][]>> listener = mock(ActionListener.class);
        featureManager.getColdStartData(detector, listener);

        verify(listener).onFailure(any(EndRunException.class));
    }

    private Object[] batchShingleData() {
        return new Object[] {
            new Object[] { new double[][] { { 1.0 } }, 1, new double[][] { { 1.0 } } },
            new Object[] { new double[][] { { 1.0, 2.0 } }, 1, new double[][] { { 1.0, 2.0 } } },
            new Object[] { new double[][] { { 1.0 }, { 2, 0 }, { 3.0 } }, 1, new double[][] { { 1.0 }, { 2.0 }, { 3.0 } } },
            new Object[] { new double[][] { { 1.0 }, { 2, 0 }, { 3.0 } }, 2, new double[][] { { 1.0, 2.0 }, { 2.0, 3.0 } } },
            new Object[] { new double[][] { { 1.0 }, { 2, 0 }, { 3.0 } }, 3, new double[][] { { 1.0, 2.0, 3.0 } } },
            new Object[] { new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } }, 1, new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } } },
            new Object[] { new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 } }, 2, new double[][] { { 1.0, 2.0, 3.0, 4.0 } } },
            new Object[] {
                new double[][] { { 1.0, 2.0 }, { 3.0, 4.0 }, { 5.0, 6.0 } },
                3,
                new double[][] { { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 } } } };
    };

    @Test
    @Parameters(method = "batchShingleData")
    public void batchShingle_returnExpected(double[][] points, int shingleSize, double[][] expected) {
        assertTrue(Arrays.deepEquals(expected, featureManager.batchShingle(points, shingleSize)));
    }

    private Object[] batchShingleIllegalArgumentData() {
        return new Object[] {
            new Object[] { new double[][] { { 1.0 } }, 0 },
            new Object[] { new double[][] { { 1.0 } }, 2 },
            new Object[] { new double[][] { { 1.0, 2.0 } }, 0 },
            new Object[] { new double[][] { { 1.0, 2.0 } }, 2 },
            new Object[] { new double[][] { { 1.0 }, { 2.0 } }, 0 },
            new Object[] { new double[][] { { 1.0 }, { 2.0 } }, 3 },
            new Object[] { new double[][] {}, 0 },
            new Object[] { new double[][] {}, 1 },
            new Object[] { new double[][] { {}, {} }, 0 },
            new Object[] { new double[][] { {}, {} }, 1 },
            new Object[] { new double[][] { {}, {} }, 2 },
            new Object[] { new double[][] { {}, {} }, 3 }, };
    };

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "batchShingleIllegalArgumentData")
    public void batchShingle_throwExpected_forInvalidInput(double[][] points, int shingleSize) {
        featureManager.batchShingle(points, shingleSize);
    }

    private Optional<double[]> getCurrentFeatures(AnomalyDetector detector, long start, long end) throws IOException {
        ActionListener<Optional<double[]>> listener = mock(ActionListener.class);
        ArgumentCaptor<Optional<double[]>> captor = ArgumentCaptor.forClass(Optional.class);
        featureManager.getCurrentFeatures(detector, start, end, AnalysisType.AD, listener);
        verify(listener).onResponse(captor.capture());
        return captor.getValue();
    }

    private Exception getCurrentFeaturesOnFailure(AnomalyDetector detector, long start, long end) throws IOException {
        ActionListener<Optional<double[]>> listener = mock(ActionListener.class);
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        featureManager.getCurrentFeatures(detector, start, end, AnalysisType.AD, listener);
        verify(listener).onFailure(captor.capture());
        return captor.getValue();
    }

    private void getPreviewFeaturesTemplate(List<Optional<double[]>> samplesResults, boolean querySuccess, boolean previewSuccess)
        throws IOException {
        long start = 0L;
        long end = 240_000L;
        long detectionInterval = (new IntervalTimeConfiguration(1, ChronoUnit.MINUTES)).toDuration().toMillis();
        when(detector.getIntervalInMilliseconds()).thenReturn(detectionInterval);

        List<Entry<Long, Long>> sampleRanges = Arrays.asList(new SimpleEntry<>(0L, 60_000L), new SimpleEntry<>(120_000L, 180_000L));
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();

            ActionListener<List<Optional<double[]>>> listener = null;

            if (args[3] instanceof ActionListener) {
                listener = (ActionListener<List<Optional<double[]>>>) args[3];
            }

            if (querySuccess) {
                listener.onResponse(samplesResults);
            } else {
                listener.onFailure(new RuntimeException());
            }

            return null;
        }).when(searchFeatureDao).getFeatureSamplesForPeriods(eq(detector), eq(sampleRanges), eq(AnalysisType.AD), any());

        ActionListener<Features> listener = mock(ActionListener.class);
        featureManager.getPreviewFeatures(detector, start, end, listener);

        if (previewSuccess) {
            Features expected = new Features(
                asList(new SimpleEntry<>(0L, 60_000L), new SimpleEntry<>(60_000L, 120_000L), new SimpleEntry<>(120_000L, 180_000L)),
                new double[][] { { 1 }, { 2 }, { 3 } }
            );
            verify(listener).onResponse(expected);
        } else {
            verify(listener).onFailure(any(Exception.class));
        }

    }

    @Test
    public void getPreviewFeatures_returnExpectedToListener() throws IOException {
        getPreviewFeaturesTemplate(asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 3 })), true, true);
    }

    @Test
    public void getPreviewFeatures_returnExceptionToListener_whenNoDataToPreview() throws IOException {
        getPreviewFeaturesTemplate(asList(), true, false);
    }

    @Test
    public void getPreviewFeatures_returnExceptionToListener_whenQueryFail() throws IOException {
        getPreviewFeaturesTemplate(asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 3 })), false, false);
    }

    @Test
    public void getPreviewFeatureForEntity() throws IOException {
        long start = 0L;
        long end = 240_000L;
        Entity entity = Entity.createSingleAttributeEntity("fieldName", "value");

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 10.0 }));
        coldStartSamples.add(Optional.of(new double[] { 30.0 }));

        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        ActionListener<Features> listener = mock(ActionListener.class);

        featureManager.getPreviewFeaturesForEntity(detector, entity, start, end, listener);

        Features expected = new Features(
            asList(new SimpleEntry<>(0L, 60_000L), new SimpleEntry<>(60_000L, 120_000L), new SimpleEntry<>(120_000L, 180_000L)),
            new double[][] { { 10 }, { 20 }, { 30 } }
        );
        verify(listener).onResponse(expected);
    }

    @Test
    public void getPreviewFeatureForEntity_noDataToPreview() throws IOException {
        long start = 0L;
        long end = 240_000L;
        Entity entity = Entity.createSingleAttributeEntity("fieldName", "value");

        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(new ArrayList<>());
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        ActionListener<Features> listener = mock(ActionListener.class);

        featureManager.getPreviewFeaturesForEntity(detector, entity, start, end, listener);

        verify(listener).onFailure(any(IllegalArgumentException.class));
    }

    @Test
    public void getPreviewEntities() {
        long start = 0L;
        long end = 240_000L;

        Entity entity1 = Entity.createSingleAttributeEntity("fieldName", "value1");
        Entity entity2 = Entity.createSingleAttributeEntity("fieldName", "value2");
        List<Entity> entities = asList(entity1, entity2);
        doAnswer(invocation -> {
            ActionListener<List<Entity>> listener = invocation.getArgument(3);
            listener.onResponse(entities);
            return null;
        }).when(searchFeatureDao).getHighestCountEntities(any(), anyLong(), anyLong(), any());

        ActionListener<List<Entity>> listener = mock(ActionListener.class);

        featureManager.getPreviewEntities(detector, start, end, listener);

        verify(listener).onResponse(entities);
    }

    private void setupSearchFeatureDaoForGetCurrentFeatures(
        List<Optional<double[]>> preQueryResponse,
        Optional<List<Optional<double[]>>> testQueryResponse
    ) throws IOException {
        AtomicBoolean isPreQuery = new AtomicBoolean(true);

        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> daoListener = invocation.getArgument(3);
            if (isPreQuery.get()) {
                isPreQuery.set(false);
                daoListener.onResponse(preQueryResponse);
            } else {
                if (testQueryResponse.isPresent()) {
                    daoListener.onResponse(testQueryResponse.get());
                } else {
                    daoListener.onFailure(new IOException());
                }
            }
            return null;
        })
            .when(searchFeatureDao)
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), eq(AnalysisType.AD), any(ActionListener.class));
    }

    private Object[] getCurrentFeaturesTestData_whenAfterQueryResultsFormFullShingle() {
        return new Object[] {
            new Object[] { asList(Optional.empty()), 3, Optional.of(asList(Optional.of(new double[] { 3 }))), new double[] { 3 } },
            new Object[] {
                asList(Optional.of(new double[] { 5 })),
                1,
                Optional.of(asList(Optional.of(new double[] { 3 }))),
                new double[] { 3 } },
            new Object[] {
                asList(Optional.of(new double[] { 1, 2 })),
                2,
                Optional.of(asList(Optional.of(new double[] { 5, 6 }))),
                new double[] { 5, 6 } }, };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenAfterQueryResultsFormFullShingle")
    public void getCurrentFeatures_returnExpectedProcessedFeatures_whenAfterQueryResultsFormFullShingle(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse,
        double[] expectedProcessedFeatures
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = shingleSize * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, AnalysisType.AD, mock(ActionListener.class));

        // Start test
        Optional<double[]> listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), eq(AnalysisType.AD), any(ActionListener.class));
        assertTrue(listenerResponse.isPresent());

        double[] actualProcessedFeatures = listenerResponse.get();
        for (int i = 0; i < expectedProcessedFeatures.length; i++) {
            assertEquals(expectedProcessedFeatures[i], actualProcessedFeatures[i], 0);
        }
    }

    private Object[] getCurrentFeaturesTestData_IOException() {
        return new Object[] {
            new Object[] { asList(Optional.of(new double[] { 3 })), new double[] { 3 } },
            new Object[] { asList(Optional.of(new double[] { 5, 6 })), new double[] { 5, 6 } } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_IOException")
    public void getCurrentFeatures_returnExpectedProcessedFeatures_IOException(
        List<Optional<double[]>> preQueryResponse,
        double[] expectedProcessedFeatures
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long start = shingleSize * intervalInMilliseconds;
        long end = start + intervalInMilliseconds;

        // Set up. 2nd call throws IOException
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, Optional.empty());
        featureManager.getCurrentFeatures(detector, start, end, AnalysisType.AD, mock(ActionListener.class));

        // Start test
        Exception listenerResponse = getCurrentFeaturesOnFailure(detector, start, end);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), eq(AnalysisType.AD), any(ActionListener.class));
        assertTrue(listenerResponse instanceof IOException);
    }

    private Object[] getCurrentFeaturesTestData_whenAfterQueryResultsAllowImputedShingle() {
        return new Object[] {
            new Object[] { asList(Optional.empty()), 3, Optional.of(asList(Optional.of(new double[] { 1 }))), new double[] { 1 } },
            new Object[] {
                asList(Optional.of(new double[] { 1 })),
                1,
                Optional.of(asList(Optional.of(new double[] { 3 }))),
                new double[] { 3 } },
            new Object[] { asList(Optional.empty()), 1, Optional.of(asList(Optional.of(new double[] { 2 }))), new double[] { 2 } },
            new Object[] {
                asList(Optional.of(new double[] { 1 })),
                2,
                Optional.of(asList(Optional.of(new double[] { 2 }))),
                new double[] { 2 } },
            new Object[] {
                asList(Optional.of(Optional.empty())),
                2,
                Optional.of(asList(Optional.of(new double[] { 1, 2 }))),
                new double[] { 1, 2 } } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenAfterQueryResultsAllowImputedShingle")
    public void getCurrentFeatures_returnExpectedProcessedFeatures_whenAfterQueryResultsAllowImputedShingle(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse,
        double[] expectedProcessedFeatures
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = (shingleSize + 1) * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + (intervalOffsetFromPreviousQuery * intervalInMilliseconds);
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, AnalysisType.AD, mock(ActionListener.class));

        // Start test
        Optional<double[]> listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), eq(AnalysisType.AD), any(ActionListener.class));
        assertTrue(listenerResponse.isPresent());
    }

    private Object[] getCurrentFeaturesTestData_whenMissingCurrentDataPoint() {
        return new Object[] {
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.empty()),
                3,
                Optional.of(asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 3 }), Optional.empty())), },
            new Object[] {
                asList(Optional.of(new double[] { 1 }), Optional.of(new double[] { 1 }), Optional.empty()),
                1,
                Optional.of(asList(Optional.empty())), },
            new Object[] {
                asList(Optional.empty(), Optional.empty(), Optional.of(new double[] { 1, 2, 3 })),
                2,
                Optional.of(asList(Optional.of(new double[] { 4, 5, 6 }), Optional.empty())), } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenMissingCurrentDataPoint")
    public void getCurrentFeatures_returnNoProcessedOrUnprocessedFeatures_whenMissingCurrentDataPoint(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = shingleSize * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, AnalysisType.AD, mock(ActionListener.class));

        // Start test
        Optional<double[]> listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), eq(AnalysisType.AD), any(ActionListener.class));
        assertFalse(listenerResponse.isPresent());
    }

    private Object[] getCurrentFeaturesTestData_whenAfterQueryResultsCannotBeShingled() {
        return new Object[] {
            new Object[] { asList(Optional.of(Optional.of(new double[] { 3 }))), 3, Optional.of(asList(Optional.of(new double[] { 4 }))), },
            new Object[] { asList(Optional.of(Optional.empty())), 1, Optional.of(asList(Optional.of(new double[] { 3, 4 }))), } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenAfterQueryResultsCannotBeShingled")
    public void getCurrentFeatures_returnNoProcessedFeatures_whenAfterQueryResultsCannotBeShingled(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = shingleSize * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, AnalysisType.AD, mock(ActionListener.class));

        // Start test
        Optional<double[]> listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), eq(AnalysisType.AD), any(ActionListener.class));
        assertTrue(listenerResponse.isPresent());
    }

    private Object[] getCurrentFeaturesTestData_whenQueryThrowsIOException() {
        return new Object[] {
            new Object[] { asList(Optional.empty(), Optional.empty(), Optional.empty()), 3 },
            new Object[] { asList(Optional.empty(), Optional.of(new double[] { 1, 2 }), Optional.of(new double[] { 3, 4 })), 1 } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_whenQueryThrowsIOException")
    public void getCurrentFeatures_returnExceptionToListener_whenQueryThrowsIOException(
        List<Optional<double[]>> preQueryResponse,
        long intervalOffsetFromPreviousQuery
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = shingleSize * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        long testStartTime = previousStartTime + intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, Optional.empty());
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, AnalysisType.AD, mock(ActionListener.class));

        // Start test
        ActionListener<Optional<double[]>> listener = mock(ActionListener.class);
        featureManager.getCurrentFeatures(detector, testStartTime, testEndTime, AnalysisType.AD, listener);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), eq(AnalysisType.AD), any(ActionListener.class));
        verify(listener).onFailure(any(IOException.class));
    }

    private Object[] getCurrentFeaturesTestData_cacheMissingData() {
        return new Object[] {
            new Object[] { asList(Optional.empty()), Optional.of(asList(Optional.of(new double[] { 1 }))), },
            new Object[] {
                asList(Optional.of(new double[] { 1, 2 }), Optional.empty(), Optional.of(new double[] { 3, 4 })),
                Optional.of(asList(Optional.of(new double[] { 5, 6 }))), } };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_cacheMissingData")
    public void getCurrentFeatures_returnExpectedFeatures_cacheMissingData(
        List<Optional<double[]>> firstQueryResponseToBeCached,
        Optional<List<Optional<double[]>>> secondQueryResponse
    ) throws IOException {
        long firstStartTime = shingleSize * intervalInMilliseconds;
        long firstEndTime = firstStartTime + intervalInMilliseconds;
        long secondStartTime = firstEndTime;
        long secondEndTime = secondStartTime + intervalInMilliseconds;

        setupSearchFeatureDaoForGetCurrentFeatures(firstQueryResponseToBeCached, secondQueryResponse);

        // first call to cache missing points
        featureManager.getCurrentFeatures(detector, firstStartTime, firstEndTime, AnalysisType.AD, mock(ActionListener.class));
        verify(searchFeatureDao, times(1))
            .getFeatureSamplesForPeriods(eq(detector), argThat(list -> list.size() == 1), eq(AnalysisType.AD), any(ActionListener.class));

        // second call should only fetch current point even if previous points missing
        Optional<double[]> listenerResponse = getCurrentFeatures(detector, secondStartTime, secondEndTime);
        verify(searchFeatureDao, times(2))
            .getFeatureSamplesForPeriods(eq(detector), argThat(list -> list.size() == 1), eq(AnalysisType.AD), any(ActionListener.class));

        assertTrue(listenerResponse.isPresent());
    }

    private Object[] getCurrentFeaturesTestData_withTimeJitterUpToHalfInterval() {
        return new Object[] {
            new Object[] {
                asList(Optional.of(new double[] { 1 })),
                2.1,
                Optional.of(asList(Optional.of(new double[] { 3 }))),
                new double[] { 3 } },
            new Object[] {
                asList(Optional.of(Optional.of(new double[] { 5 }))),
                0.8,
                Optional.of(asList(Optional.of(new double[] { 3 }))),
                new double[] { 3 } },
            new Object[] {
                asList(Optional.of(new double[] { 1 })),
                1.49,
                Optional.of(asList(Optional.of(new double[] { 2 }))),
                new double[] { 2 } },
            new Object[] {
                asList(Optional.of(new double[] { 1 })),
                1.51,
                Optional.of(asList(Optional.of(new double[] { 2 }))),
                new double[] { 2 } },
            new Object[] {
                asList(Optional.of(new double[] { 1 })),
                2.49,
                Optional.of(asList(Optional.of(new double[] { 2 }))),
                new double[] { 2 } },
            new Object[] {
                asList(Optional.of(Optional.of(new double[] { 5, 6 }))),
                2.5,
                Optional.of(asList(Optional.of(new double[] { 11, 12 }))),
                new double[] { 11, 12 } }, };
    }

    @Test
    @Parameters(method = "getCurrentFeaturesTestData_withTimeJitterUpToHalfInterval")
    public void getCurrentFeatures_returnExpectedFeatures_withTimeJitterUpToHalfInterval(
        List<Optional<double[]>> preQueryResponse,
        double intervalOffsetFromPreviousQuery,
        Optional<List<Optional<double[]>>> testQueryResponse,
        double[] expectedProcessedFeatures
    ) throws IOException {
        int expectedNumQueriesToSearchFeatureDao = 2;
        long previousStartTime = (shingleSize + 1) * intervalInMilliseconds;
        long previousEndTime = previousStartTime + intervalInMilliseconds;
        double millisecondsOffset = intervalOffsetFromPreviousQuery * intervalInMilliseconds;
        long testStartTime = previousStartTime + (long) millisecondsOffset;
        long testEndTime = testStartTime + intervalInMilliseconds;

        // Set up
        setupSearchFeatureDaoForGetCurrentFeatures(preQueryResponse, testQueryResponse);
        featureManager.getCurrentFeatures(detector, previousStartTime, previousEndTime, AnalysisType.AD, mock(ActionListener.class));

        // Start test
        Optional<double[]> listenerResponse = getCurrentFeatures(detector, testStartTime, testEndTime);
        verify(searchFeatureDao, times(expectedNumQueriesToSearchFeatureDao))
            .getFeatureSamplesForPeriods(eq(detector), any(List.class), eq(AnalysisType.AD), any(ActionListener.class));
        assertTrue(listenerResponse.isPresent());
    }

    private <K, V> Entry<K, V> entry(K key, V value) {
        return new SimpleEntry<>(key, value);
    }

    private Optional<double[]> ar(double... values) {
        if (values.length == 0) {
            return Optional.empty();
        } else {
            return Optional.of(values);
        }
    }

    private Object[] getCurrentFeaturesTestData_setsShingleSizeFromDetectorConfig() {
        return new Object[] { new Object[] { 1 }, new Object[] { 4 }, new Object[] { 8 }, new Object[] { 20 } };
    }
}
