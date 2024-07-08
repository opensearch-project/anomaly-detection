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

package org.opensearch.ad.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.config.ForestMode;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.collect.ImmutableList;

import test.org.opensearch.ad.util.LabelledAnomalyGenerator;
import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.MultiDimDataWithTime;

public class EntityColdStarterTests extends AbstractCosineDataTest {

    @BeforeClass
    public static void initOnce() {
        ClusterService clusterService = mock(ClusterService.class);

        Set<Setting<?>> settingSet = ADEnabledSetting.settings.values().stream().collect(Collectors.toSet());

        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, settingSet));

        ADEnabledSetting.getInstance().init(clusterService);
    }

    @AfterClass
    public static void clearOnce() {
        // restore to default value
        ADEnabledSetting.getInstance().setSettingValue(ADEnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED, false);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ADEnabledSetting.getInstance().setSettingValue(ADEnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED, Boolean.TRUE);
    }

    @Override
    public void tearDown() throws Exception {
        ADEnabledSetting.getInstance().setSettingValue(ADEnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED, Boolean.FALSE);
        super.tearDown();
    }

    // train using samples directly
    public void testTrainUsingSamples() throws InterruptedException, IOException {
        Deque<Sample> samples = MLUtil.createQueueSamples(numMinSamples);
        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );
        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            samples.peek().getValueList(),
            samples.peek().getDataStartTime().toEpochMilli(),
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        assertTrue(modelState.getModel().isPresent());
        ThresholdedRandomCutForest ercf = modelState.getModel().get();
        assertEquals(numMinSamples, ercf.getForest().getTotalUpdates());

        checkSemaphoreRelease();
    }

    public void testColdStart() throws InterruptedException, IOException {
        // By default startNormalization is 10, thus we won't see total updates until the 10th point
        numMinSamples = 10;
        shingleSize = 8;
        entityColdStarter = new ADColdStart(
            clock,
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            // settings,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            1
        );
        Deque<Sample> samples = MLUtil.createQueueSamples(1);
        modelState = new ModelState<>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );

        long startTime = 1602269260000L;
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(startTime));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            if (i == 3) {
                coldStartSamples.add(Optional.empty());
            } else {
                coldStartSamples.add(Optional.of(new double[] { i }));
            }
        }

        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            samples.peek().getValueList(),
            startTime + coldStartSamples.size() * detector.getIntervalInMilliseconds(),
            entity,
            "123"
        );
        resetListener();
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();

        assertTrue(modelState.getModel().isPresent());
        ThresholdedRandomCutForest ercf = modelState.getModel().get();

        assertEquals(coldStartSamples.size(), ercf.getForest().getTotalUpdates());
        assertTrue("size: " + modelState.getSamples().size(), modelState.getSamples().isEmpty());

        List<Sample> expectedColdStartData = new ArrayList<>();
        long currentStartTimeMillis = startTime;
        for (int i = 0; i < 11; i++) {
            if (i != 3) {
                expectedColdStartData
                    .add(
                        new Sample(
                            new double[] { i },
                            Instant.ofEpochMilli(currentStartTimeMillis),
                            Instant.ofEpochMilli(currentStartTimeMillis + detector.getIntervalInMilliseconds())
                        )
                    );
            }
            currentStartTimeMillis += detector.getIntervalInMilliseconds();
        }

        diffTesting(modelState, expectedColdStartData);

        for (int i = 0; i <= TimeSeriesSettings.COLD_START_DOOR_KEEPER_COUNT_THRESHOLD; i++) {
            resetListener();
            modelState = createStateForCacheRelease();
            entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
            checkSemaphoreRelease();
        }

        // model is not trained as the door keeper remembers it after TimeSeriesSettings.DOOR_KEEPER_COUNT_THRESHOLD retries and won't retry
        // training
        assertTrue(modelState.getModel().isEmpty());

        // the samples is not touched since cold start does not happen
        assertEquals("size: " + modelState.getSamples().size(), 1, modelState.getSamples().size());
    }

    // min max: miss one
    public void testMissMin() throws IOException, InterruptedException {
        Deque<Sample> samples = MLUtil.createQueueSamples(1);
        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.empty());
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            samples.peek().getValueList(),
            samples.peek().getDataStartTime().toEpochMilli(),
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);

        verify(searchFeatureDao, never()).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        assertTrue(modelState.getModel().isEmpty());
        checkSemaphoreRelease();
    }

    /**
     * Performan differential testing using trcf model with input cold start data and the modelState
     * @param modelState an initialized model state
     * @param coldStartData cold start data that initialized the modelState
     */
    private void diffTesting(ModelState<ThresholdedRandomCutForest> modelState, List<Sample> coldStartData) {
        int inputDimension = detector.getEnabledFeatureIds().size();

        ThresholdedRandomCutForest.Builder refTRcfBuilder = ThresholdedRandomCutForest
            .builder()
            .compact(true)
            .dimensions(inputDimension * detector.getShingleSize())
            .precision(Precision.FLOAT_32)
            .randomSeed(rcfSeed)
            .numberOfTrees(TimeSeriesSettings.NUM_TREES)
            .shingleSize(detector.getShingleSize())
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .timeDecay(detector.getTimeDecay())
            .transformDecay(detector.getTimeDecay())
            .outputAfter(Math.max(detector.getShingleSize(), numMinSamples))
            .initialAcceptFraction(numMinSamples * 1.0d / TimeSeriesSettings.NUM_SAMPLES_PER_TREE)
            .parallelExecutionEnabled(false)
            .sampleSize(TimeSeriesSettings.NUM_SAMPLES_PER_TREE)
            .internalShinglingEnabled(true)
            .anomalyRate(1 - TimeSeriesSettings.THRESHOLD_MIN_PVALUE)
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true);

        if (detector.getShingleSize() > 1) {
            refTRcfBuilder.forestMode(ForestMode.STREAMING_IMPUTE);
            refTRcfBuilder = ModelColdStart.applyImputationMethod(detector, refTRcfBuilder);
        } else {
            // imputation with shingle size 1 is not meaningful
            refTRcfBuilder.forestMode(ForestMode.STANDARD);
        }

        double[] rules = new double[inputDimension];
        for (int i = 0; i < inputDimension; i++) {
            rules[i] = 0.2;
        }
        refTRcfBuilder.ignoreNearExpectedFromAboveByRatio(rules);
        refTRcfBuilder.ignoreNearExpectedFromBelowByRatio(rules);

        ThresholdedRandomCutForest refTRcf = refTRcfBuilder.build();

        long lastSampleEndTime = 0;
        for (int i = 0; i < coldStartData.size(); i++) {
            Sample sample = coldStartData.get(i);
            lastSampleEndTime = sample.getDataEndTime().getEpochSecond();
            refTRcf.process(sample.getValueList(), lastSampleEndTime);
        }

        assertEquals(
            refTRcf.getForest().getTotalUpdates() + " != " + modelState.getModel().get().getForest().getTotalUpdates(),
            refTRcf.getForest().getTotalUpdates(),
            modelState.getModel().get().getForest().getTotalUpdates()
        );

        Random r = new Random();

        // ThresholdedRandomCutForest refTRcf3 = modelState.getModel().get();
        // make sure we trained the expected models
        for (int i = 0; i < 100; i++) {
            lastSampleEndTime += detector.getIntervalInSeconds();
            double[] point = r.ints(inputDimension, 0, 50).asDoubleStream().toArray();
            assertEquals(refTRcf.getForest().getTotalUpdates(), modelState.getModel().get().getForest().getTotalUpdates());
            AnomalyDescriptor descriptor = refTRcf.process(point, lastSampleEndTime);
            Sample sample = new Sample(
                point,
                Instant.ofEpochSecond(lastSampleEndTime - detector.getIntervalInSeconds()),
                Instant.ofEpochSecond(lastSampleEndTime)
            );
            ThresholdingResult result = modelManager.getResult(sample, modelState, modelId, detector, "123");
            assertEquals(descriptor.getRCFScore(), result.getRcfScore(), 1e-10);
            assertEquals(descriptor.getAnomalyGrade(), result.getGrade(), 1e-10);
        }
    }

    // two segments of samples, one segment has 3 samples, while another one has only 1
    public void testTwoSegmentsWithSingleSample() throws InterruptedException, IOException {
        // By default startNormalization is 10, thus we won't see total updates until the 10th point
        numMinSamples = 10;
        shingleSize = 8;
        entityColdStarter = new ADColdStart(
            clock,
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            // settings,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            1
        );

        Deque<Sample> samples = MLUtil.createQueueSamples(0);
        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );

        long startTime = 1602269260000L;
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(startTime));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();

        for (int i = 0; i < 11; i++) {
            if (i == 3) {
                coldStartSamples.add(Optional.empty());
            } else {
                coldStartSamples.add(Optional.of(new double[] { i }));
            }
        }
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            new double[] { 0 },
            startTime + coldStartSamples.size() * detector.getIntervalInMilliseconds(),
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();
        assertTrue(modelState.getModel().isPresent());

        // imputed value are counted as one update. So we have 11 values in total including the one missing value.
        assertEquals(11, modelState.getModel().get().getForest().getTotalUpdates());

        List<Sample> expectedColdStartData = new ArrayList<>();
        long currentStartTimeMillis = startTime;
        for (int i = 0; i < 11; i++) {
            if (i != 3) {
                expectedColdStartData
                    .add(
                        new Sample(
                            new double[] { i },
                            Instant.ofEpochMilli(currentStartTimeMillis),
                            Instant.ofEpochMilli(currentStartTimeMillis + detector.getIntervalInMilliseconds())
                        )
                    );
            }
            currentStartTimeMillis += detector.getIntervalInMilliseconds();
        }

        diffTesting(modelState, expectedColdStartData);
    }

    // two segments of samples, one segment has 3 samples, while another one 2 samples
    public void testTwoSegments() throws InterruptedException, IOException {
        // By default startNormalization is 10, thus we won't see total updates until the 10th point
        numMinSamples = 10;
        shingleSize = 8;
        entityColdStarter = new ADColdStart(
            clock,
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            // settings,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            1
        );
        Deque<Sample> samples = MLUtil.createQueueSamples(1);
        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );
        long startTime = 1602269260000L;

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(startTime));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            if (i == 3) {
                coldStartSamples.add(Optional.empty());
            } else {
                coldStartSamples.add(Optional.of(new double[] { i }));
            }
        }
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            samples.peek().getValueList(),
            startTime + coldStartSamples.size() * detector.getIntervalInMilliseconds(),
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();

        assertTrue(modelState.getModel().isPresent());
        ThresholdedRandomCutForest ercf = modelState.getModel().get();
        assertEquals(coldStartSamples.size(), ercf.getForest().getTotalUpdates());
        checkSemaphoreRelease();

        List<Sample> expectedColdStartData = new ArrayList<>();
        long currentStartTimeMillis = startTime;
        for (int i = 0; i < 11; i++) {
            if (i != 3) {
                expectedColdStartData
                    .add(
                        new Sample(
                            new double[] { i },
                            Instant.ofEpochMilli(currentStartTimeMillis),
                            Instant.ofEpochMilli(currentStartTimeMillis + detector.getIntervalInMilliseconds())
                        )
                    );
            }
            currentStartTimeMillis += detector.getIntervalInMilliseconds();
        }
        diffTesting(modelState, expectedColdStartData);
    }

    public void testThrottledColdStart() throws InterruptedException {
        Deque<Sample> samples = MLUtil.createQueueSamples(1);
        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onFailure(new OpenSearchRejectedExecutionException(""));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            samples.peek().getValueList(),
            samples.peek().getDataStartTime().toEpochMilli(),
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);

        entityColdStarter.trainModel(featureRequest, "456", modelState, listener);

        // only the first one makes the call
        verify(searchFeatureDao, times(1)).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());
        checkSemaphoreRelease();
    }

    public void testColdStartException() throws InterruptedException {
        Deque<Sample> samples = MLUtil.createQueueSamples(1);
        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );

        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onFailure(new TimeSeriesException(detectorId, ""));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            samples.peek().getValueList(),
            samples.peek().getDataStartTime().toEpochMilli(),
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);

        assertTrue(stateManager.fetchExceptionAndClear(detectorId).isPresent());
        checkSemaphoreRelease();
    }

    @SuppressWarnings("unchecked")
    public void testNotEnoughSamples() throws InterruptedException, IOException {
        Deque<Sample> samples = MLUtil.createQueueSamples(1);
        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );

        doAnswer(invocation -> {
            GetRequest request = invocation.getArgument(0);
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, CommonName.CONFIG_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        long startTime = 1602269260000L;
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(startTime));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            samples.peek().getValueList(),
            startTime + coldStartSamples.size() * detector.getIntervalInMilliseconds(),
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();

        assertTrue(modelState.getModel().isEmpty());
        // not enough smples to train. We keep them in the sample array of model state.
        Deque<Sample> currentSamples = modelState.getSamples();
        assertEquals("real sample size is " + currentSamples.size(), 2, currentSamples.size());
        int j = 0;
        while (!currentSamples.isEmpty()) {
            double[] element = currentSamples.poll().getValueList();
            assertEquals(1, element.length);
            if (j == 0 || j == 2) {
                assertEquals(57, element[0], 1e-10);
            } else {
                assertEquals(1, element[0], 1e-10);
            }
            j++;
        }
    }

    @SuppressWarnings("unchecked")
    public void testEmptyDataRange() throws InterruptedException {
        // the min-max range has 5 samples is too small and thus no model can be initialized as we require at least 32
        numMinSamples = 32;
        entityColdStarter = new ADColdStart(
            clock,
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            // settings,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            1
        );

        Deque<Sample> samples = MLUtil.createQueueSamples(1);
        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );

        // when(clock.millis()).thenReturn(894057860000L);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(TestHelpers.createGetResponse(detector, detector.getId(), CommonName.CONFIG_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        long startTime = 894056973000L;
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(startTime));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();

        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };

        coldStartSamples.add(Optional.of(sample1));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(sample2));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(sample3));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            samples.peek().getValueList(),
            startTime + coldStartSamples.size() * detector.getIntervalInMilliseconds(),
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();

        assertTrue(modelState.getModel().isEmpty());
        // the min-max range is too small and thus not enough training data
        // 3 from history
        assertEquals("real sample size is " + modelState.getSamples().size(), 3, modelState.getSamples().size());
    }

    public void testTrainModelFromExistingSamplesEnoughSamples() throws IOException {
        // less than 10 will make rcf results undeterministic even though two rcf models have the same rcfSeed
        numMinSamples = 10;
        int inputDimension = 2;

        detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance(inputDimension)
            .setDetectionInterval(new IntervalTimeConfiguration(detectorInterval, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .setShingleSize(shingleSize)
            .build();

        // reinitialize entityColdStarter and modelManager using new numMinSamples
        entityColdStarter = new ADColdStart(
            clock,
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            // settings,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            1
        );
        modelManager = new ADModelManager(
            mock(ADCheckpointDao.class),
            mock(Clock.class),
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
            entityColdStarter,
            mock(FeatureManager.class),
            mock(MemoryTracker.class),
            settings,
            clusterService
        );

        ThresholdedRandomCutForest.Builder rcfConfig = ThresholdedRandomCutForest
            .builder()
            .compact(true)
            .dimensions(inputDimension * detector.getShingleSize())
            .precision(Precision.FLOAT_32)
            .randomSeed(rcfSeed)
            .numberOfTrees(TimeSeriesSettings.NUM_TREES)
            .shingleSize(detector.getShingleSize())
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .timeDecay(detector.getTimeDecay())
            .transformDecay(detector.getTimeDecay())
            .outputAfter(Math.max(detector.getShingleSize(), numMinSamples))
            .initialAcceptFraction(numMinSamples * 1.0d / TimeSeriesSettings.NUM_SAMPLES_PER_TREE)
            .parallelExecutionEnabled(false)
            .sampleSize(TimeSeriesSettings.NUM_SAMPLES_PER_TREE)
            .internalShinglingEnabled(true)
            .anomalyRate(1 - TimeSeriesSettings.THRESHOLD_MIN_PVALUE)
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true);

        if (detector.getShingleSize() > 1) {
            rcfConfig.forestMode(ForestMode.STREAMING_IMPUTE);
            rcfConfig = ModelColdStart.applyImputationMethod(detector, rcfConfig);
        } else {
            // imputation with shingle size 1 is not meaningful
            rcfConfig.forestMode(ForestMode.STANDARD);
        }
        Tuple<Deque<Sample>, ThresholdedRandomCutForest> models = MLUtil
            .prepareModel(inputDimension, rcfConfig, detector.getIntervalInMilliseconds());
        Deque<Sample> samples = models.v1();
        ThresholdedRandomCutForest rcf = models.v2();

        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );

        Random r = new Random();

        // make sure we trained the expected models
        Instant currentTime = samples.getLast().getDataEndTime().plusMillis(detector.getIntervalInMilliseconds());
        for (int i = 0; i < 100; i++) {
            double[] point = r.ints(inputDimension, 0, 50).asDoubleStream().toArray();
            AnomalyDescriptor descriptor = rcf.process(point, currentTime.getEpochSecond());
            Sample sample = new Sample(point, currentTime.minusMillis(detector.getIntervalInMilliseconds()), currentTime);
            ThresholdingResult result = modelManager.getResult(sample, modelState, modelId, detector, "123");
            assertEquals(descriptor.getRCFScore(), result.getRcfScore(), 1e-10);
            assertEquals(descriptor.getAnomalyGrade(), result.getGrade(), 1e-10);
            currentTime = currentTime.plusMillis(detector.getIntervalInMilliseconds());
        }
    }

    public void testTrainModelFromExistingSamplesNotEnoughSamples() {
        Deque<Sample> samples = new ArrayDeque<>();
        modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );
        entityColdStarter.trainModelFromExistingSamples(modelState, detector, "123");
        assertTrue(modelState.getModel().isEmpty());
    }

    /**
     * A template to perform precision/recall test by simulating HCAD logic with only one entity.
     * Check if one pass of all data by AD has at least precisionThreshold and recallThreshold. Retry 20 times.
     *
     * @param detectorIntervalMins Detector interval
     * @param precisionThreshold precision threshold
     * @param recallThreshold recall threshold
     * @throws Exception when failing to create anomaly detector or creating training data
     */
    @SuppressWarnings("unchecked")
    private void accuracyTemplate(int detectorIntervalMins, float precisionThreshold, float recallThreshold) throws Exception {
        int dataSize = 20 * TimeSeriesSettings.NUM_SAMPLES_PER_TREE;
        int trainTestSplit = 300;
        // detector interval
        int delta = 60000 * detectorIntervalMins;
        int inputDimension = 2;
        int numberOfTrials = 20;
        double prec = 0;
        double recall = 0;

        for (int z = 0; z < numberOfTrials; z++) {
            long seed = new Random().nextLong();
            LOG.info("seed = " + seed);
            detector = TestHelpers.AnomalyDetectorBuilder
                .newInstance(inputDimension)
                .setDetectionInterval(new IntervalTimeConfiguration(detectorIntervalMins, ChronoUnit.MINUTES))
                .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
                .setShingleSize(shingleSize)
                .build();
            // create labelled data
            MultiDimDataWithTime dataWithKeys = LabelledAnomalyGenerator
                .getMultiDimData(
                    dataSize + detector.getShingleSize() - 1,
                    50,
                    100,
                    5,
                    seed,
                    inputDimension,
                    false,
                    trainTestSplit,
                    delta,
                    false
                );
            long[] timestamps = dataWithKeys.timestampsMs;
            double[][] data = dataWithKeys.data;
            when(clock.millis()).thenReturn(timestamps[trainTestSplit - 1]);

            // training data ranges from timestamps[0] ~ timestamps[trainTestSplit-1]
            doAnswer(invocation -> {
                ActionListener<GetResponse> listener = invocation.getArgument(2);

                listener.onResponse(TestHelpers.createGetResponse(detector, detector.getId(), CommonName.CONFIG_INDEX));
                return null;
            }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

            doAnswer(invocation -> {
                ActionListener<Optional<Long>> listener = invocation.getArgument(3);
                listener.onResponse(Optional.of(timestamps[0]));
                return null;
            }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

            doAnswer(invocation -> {
                List<Entry<Long, Long>> ranges = invocation.getArgument(1);
                List<Optional<double[]>> coldStartSamples = new ArrayList<>();

                Collections.sort(ranges, new Comparator<Entry<Long, Long>>() {
                    @Override
                    public int compare(Entry<Long, Long> p1, Entry<Long, Long> p2) {
                        return Long.compare(p1.getKey(), p2.getKey());
                    }
                });
                for (int j = 0; j < ranges.size(); j++) {
                    Entry<Long, Long> range = ranges.get(j);
                    Long start = range.getKey();
                    int valueIndex = searchInsert(timestamps, start);
                    coldStartSamples.add(Optional.of(data[valueIndex]));
                }

                ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
                listener.onResponse(coldStartSamples);
                return null;
            }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

            modelState = new ModelState<ThresholdedRandomCutForest>(
                null,
                modelId,
                detectorId,
                ModelManager.ModelType.TRCF.getName(),
                clock,
                priority,
                Optional.of(entity),
                new ArrayDeque<>()
            );

            released = new AtomicBoolean();

            inProgressLatch = new CountDownLatch(1);
            listener = ActionListener.wrap(() -> {
                released.set(true);
                inProgressLatch.countDown();
            });

            FeatureRequest featureRequest = new FeatureRequest(
                Instant.now().toEpochMilli(),
                detectorId,
                RequestPriority.MEDIUM,
                new double[] { 1.3 },
                Instant.now().toEpochMilli(),
                entity,
                "123"
            );
            entityColdStarter.trainModel(featureRequest, detector.getId(), modelState, listener);

            checkSemaphoreRelease();
            assertTrue(modelState.getModel().isPresent());

            int tp = 0;
            int fp = 0;
            int fn = 0;
            long[] changeTimestamps = dataWithKeys.changeTimeStampsMs;

            for (int j = trainTestSplit + 1; j < data.length; j++) {
                Sample sample = new Sample(data[j], Instant.ofEpochMilli(timestamps[j] - delta), Instant.ofEpochMilli(timestamps[j]));
                ThresholdingResult result = modelManager.getResult(sample, modelState, modelId, detector, "123");
                if (result.getGrade() > 0) {
                    if (changeTimestamps[j] == 0) {
                        fp++;
                    } else {
                        tp++;
                    }
                } else {
                    if (changeTimestamps[j] != 0) {
                        fn++;
                    }
                    // else ok
                }
            }

            if (tp + fp == 0) {
                prec = 1;
            } else {
                prec = tp * 1.0 / (tp + fp);
            }

            if (tp + fn == 0) {
                recall = 1;
            } else {
                recall = tp * 1.0 / (tp + fn);
            }

            // there are randomness involved; keep trying for a limited times
            if (prec >= precisionThreshold && recall >= recallThreshold) {
                break;
            }
        }

        assertTrue("precision is " + prec, prec >= precisionThreshold);
        assertTrue("recall is " + recall, recall >= recallThreshold);
    }

    public void testAccuracyTenMinuteInterval() throws Exception {
        accuracyTemplate(10, 0.5f, 0.5f);
    }

    public void testAccuracyThirteenMinuteInterval() throws Exception {
        accuracyTemplate(13, 0.5f, 0.5f);
    }

    public void testAccuracyOneMinuteInterval() throws Exception {
        accuracyTemplate(1, 0.5f, 0.5f);
    }

    private ModelState<ThresholdedRandomCutForest> createStateForCacheRelease() {
        inProgressLatch = new CountDownLatch(1);
        releaseSemaphore = () -> {
            released.set(true);
            inProgressLatch.countDown();
        };
        listener = ActionListener.wrap(releaseSemaphore);
        Deque<Sample> samples = MLUtil.createQueueSamples(1);
        return new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            samples
        );
    }

    public void testCacheReleaseAfterMaintenance() throws IOException, InterruptedException {
        ModelState<ThresholdedRandomCutForest> modelState = createStateForCacheRelease();
        long minTime = 1602269260000L;
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(minTime));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();

        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };

        coldStartSamples.add(Optional.of(sample1));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(sample2));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(sample3));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            new double[] { 1.3 },
            // detectorInterval is of minutes, need to convert to milliseconds
            minTime + coldStartSamples.size() * detectorInterval * 60000,
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();
        assertTrue(modelState.getModel().isPresent());

        for (int i = 0; i <= TimeSeriesSettings.COLD_START_DOOR_KEEPER_COUNT_THRESHOLD; i++) {
            resetListener();
            modelState = createStateForCacheRelease();
            entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
            checkSemaphoreRelease();
        }

        // model is not trained as the door keeper remembers it after TimeSeriesSettings.DOOR_KEEPER_COUNT_THRESHOLD retries and won't retry
        // training
        assertTrue(modelState.getModel().isEmpty());

        // make sure when the next maintenance coming, current door keeper gets reset
        // note our detector interval is 1 minute and the door keeper will expire in 60 intervals, which are 60 minutes
        when(clock.instant()).thenReturn(Instant.now().plus(TimeSeriesSettings.DOOR_KEEPER_MAINTENANCE_FREQ + 1, ChronoUnit.MINUTES));
        entityColdStarter.maintenance();

        modelState = createStateForCacheRelease();
        featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            new double[] { 1.3 },
            // detectorInterval is of minutes, need to convert to milliseconds
            // important to let the test pass as we only mocked to return 5 results including empty ones.
            // We have to match the start and end time of training data
            minTime + coldStartSamples.size() * detectorInterval * 60000,
            entity,
            "123"
        );
        resetListener();
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();
        // model is trained as the door keeper gets reset
        assertTrue(modelState.getModel().isPresent());
    }

    public void testCacheReleaseAfterClear() throws IOException, InterruptedException {
        long startTime = 1602269260000L;
        ModelState<ThresholdedRandomCutForest> modelState = createStateForCacheRelease();
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(startTime));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();

        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };

        coldStartSamples.add(Optional.of(sample1));
        coldStartSamples.add(Optional.of(sample2));
        coldStartSamples.add(Optional.of(sample3));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            new double[] { 1.3 },
            startTime + coldStartSamples.size() * detector.getIntervalInMilliseconds(),
            entity,
            "123"
        );

        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();
        assertTrue(modelState.getModel().isPresent());

        entityColdStarter.clear(detectorId);

        modelState = createStateForCacheRelease();
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();
        // model is trained as the door keeper is regenerated after clearance
        assertTrue(modelState.getModel().isPresent());
    }

    public void testNotEnoughTrainingData() throws IOException, InterruptedException {
        // we only have 3 samples and thus it is not enough to initialize the model.
        numMinSamples = 4;

        entityColdStarter = new ADColdStart(
            clock,
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            // settings,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            1
        );

        ModelState<ThresholdedRandomCutForest> modelState = createStateForCacheRelease();
        long minTime = 1602269260000L;
        doAnswer(invocation -> {
            ActionListener<Optional<Long>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(minTime));
            return null;
        }).when(searchFeatureDao).getMinDataTime(any(), any(), eq(AnalysisType.AD), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();

        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };

        coldStartSamples.add(Optional.of(sample1));
        coldStartSamples.add(Optional.of(sample2));
        coldStartSamples.add(Optional.of(sample3));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(5);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), eq(AnalysisType.AD), any());

        FeatureRequest featureRequest = new FeatureRequest(
            Instant.now().toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            new double[] { 1.3 },
            // detectorInterval is of minutes, need to convert to milliseconds
            minTime + coldStartSamples.size() * detectorInterval * 60000,
            entity,
            "123"
        );
        entityColdStarter.trainModel(featureRequest, detectorId, modelState, listener);
        checkSemaphoreRelease();
        assertTrue(modelState.getModel().isEmpty());
    }
}
