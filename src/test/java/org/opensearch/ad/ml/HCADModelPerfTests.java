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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.tests.util.TimeUnits;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.google.common.collect.ImmutableList;

import test.org.opensearch.ad.util.LabelledAnomalyGenerator;
import test.org.opensearch.ad.util.MultiDimDataWithTime;

@TimeoutSuite(millis = 60 * TimeUnits.MINUTE) // rcf may be slow due to bounding box cache disabled
public class HCADModelPerfTests extends AbstractCosineDataTest {

    /**
     * A template to perform precision/recall test by simulating HCAD logic with only one entity.
     * We rerun AD for 10 times and check if the average precision/recall surpass the given threshold.
     *
     * @param detectorIntervalMins Detector interval
     * @param precisionThreshold precision threshold
     * @param recallThreshold recall threshold
     * @param baseDimension the number of dimensions
     * @param anomalyIndependent whether anomalies in each dimension is generated independently
     * @throws Exception when failing to create anomaly detector or creating training data
     */
    @SuppressWarnings("unchecked")
    private void averageAccuracyTemplate(
        int detectorIntervalMins,
        float precisionThreshold,
        float recallThreshold,
        int baseDimension,
        boolean anomalyIndependent
    ) throws Exception {
        int dataSize = 20 * TimeSeriesSettings.NUM_SAMPLES_PER_TREE;
        int trainTestSplit = 300;
        // detector interval
        int interval = detectorIntervalMins;
        int delta = 60000 * interval;

        int numberOfTrials = 10;
        double prec = 0;
        double recall = 0;
        double totalPrec = 0;
        double totalRecall = 0;

        // training data ranges from timestamps[0] ~ timestamps[trainTestSplit-1]
        // set up detector
        detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance(baseDimension)
            .setDetectionInterval(new IntervalTimeConfiguration(interval, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .setShingleSize(TimeSeriesSettings.DEFAULT_SHINGLE_SIZE)
            .setRules(null)
            .build();

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(TestHelpers.createGetResponse(detector, detector.getId(), ADCommonName.CONFIG_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        for (int z = 1; z <= numberOfTrials; z++) {
            long seed = z;
            LOG.info("seed = " + seed);
            // recreate in each loop; otherwise, we will have heap overflow issue.
            searchFeatureDao = spy(
                new SearchFeatureDao(
                    client,
                    xContentRegistry(), // Important. Without this, ParseUtils cannot parse anything
                    securityCientUtil,
                    clusterService,
                    TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
                    clock,
                    1,
                    1,
                    60_000L
                )
            );

            featureManager = new FeatureManager(
                searchFeatureDao,
                imputer,
                TimeSeriesSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
                TimeSeriesSettings.MIN_TRAIN_SAMPLES,
                AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
                AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
                AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
                AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
                threadPool
            );

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
                TimeSeriesSettings.HOURLY_MAINTENANCE,
                seed,
                TimeSeriesSettings.MAX_COLD_START_ROUNDS,
                1,
                0
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

            // create labelled data
            MultiDimDataWithTime dataWithKeys = LabelledAnomalyGenerator
                .getMultiDimData(
                    dataSize + detector.getShingleSize() - 1,
                    50,
                    100,
                    5,
                    seed,
                    baseDimension,
                    false,
                    trainTestSplit,
                    delta,
                    anomalyIndependent
                );

            long[] timestamps = dataWithKeys.timestampsMs;
            double[][] data = dataWithKeys.data;
            long dataStartTimeMs = timestamps[trainTestSplit - 1];
            when(clock.millis()).thenReturn(dataStartTimeMs);

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

            entity = Entity.createSingleAttributeEntity("field", entityName + z);
            ModelState<ThresholdedRandomCutForest> modelState = new ModelState<>(
                null,
                entity.getModelId(detectorId).get(),
                detector.getId(),
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

            entityColdStarter
                .trainModel(
                    new FeatureRequest(
                        dataStartTimeMs + 60000,
                        detector.getId(),
                        RequestPriority.MEDIUM,
                        new double[] {},
                        dataStartTimeMs,
                        entity,
                        null
                    ),
                    detector.getId(),
                    modelState,
                    listener
                );

            checkSemaphoreRelease();
            assertTrue(modelState.getModel().isPresent());

            int tp = 0;
            int fp = 0;
            int fn = 0;
            long[] changeTimestamps = dataWithKeys.changeTimeStampsMs;

            for (int j = trainTestSplit; j < data.length; j++) {
                Instant time = Instant.ofEpochMilli(timestamps[j]);
                ThresholdingResult result = modelManager.getResult(new Sample(data[j], time, time), modelState, modelId, detector, null);
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

            totalPrec += prec;
            totalRecall += recall;
            modelState = null;
            dataWithKeys = null;
            reset(searchFeatureDao);
            searchFeatureDao = null;
            clusterService = null;
        }

        double avgPrec = totalPrec / numberOfTrials;
        double avgRecall = totalRecall / numberOfTrials;
        LOG.info("{} features, Interval {}, Precision: {}, recall: {}", baseDimension, detectorIntervalMins, avgPrec, avgRecall);
        assertTrue("average precision is " + avgPrec, avgPrec >= precisionThreshold);
        assertTrue("average recall is " + avgRecall, avgRecall >= recallThreshold);
    }

    /**
     * Split average accuracy tests into two in case of time out per test.
     * @throws Exception when failing to perform tests
     */
    public void testAverageAccuracyDependent() throws Exception {
        LOG.info("Anomalies are injected dependently");

        // 10 minute interval, 4 features
        averageAccuracyTemplate(10, 0.5f, 0.2f, 4, false);

        // 10 minute interval, 2 features
        averageAccuracyTemplate(10, 0.5f, 0.3f, 2, false);

        // 10 minute interval, 1 features
        averageAccuracyTemplate(10, 0.4f, 0.4f, 1, false);

        // 5 minute interval, 4 features
        averageAccuracyTemplate(5, 0.4f, 0.1f, 4, false);

        // 5 minute interval, 2 features
        averageAccuracyTemplate(5, 0.4f, 0.3f, 2, false);

        // 5 minute interval, 1 features
        averageAccuracyTemplate(5, 0.4f, 0.4f, 1, false);
    }

    /**
     * Split average accuracy tests into two in case of time out per test.
     * @throws Exception when failing to perform tests
     */
    public void testAverageAccuracyIndependent() throws Exception {
        LOG.info("Anomalies are injected independently");

        // 10 minute interval, 4 features
        averageAccuracyTemplate(10, 0.4f, 0.1f, 4, true);

        // 10 minute interval, 2 features
        averageAccuracyTemplate(10, 0.6f, 0.4f, 2, true);

        // 10 minute interval, 1 features
        averageAccuracyTemplate(10, 0.5f, 0.3f, 1, true);

        // 5 minute interval, 4 features
        averageAccuracyTemplate(5, 0.2f, 0.1f, 4, true);

        // 5 minute interval, 2 features
        averageAccuracyTemplate(5, 0.5f, 0.3f, 2, true);

        // 5 minute interval, 1 features
        averageAccuracyTemplate(5, 0.3f, 0.3f, 1, true);
    }
}
