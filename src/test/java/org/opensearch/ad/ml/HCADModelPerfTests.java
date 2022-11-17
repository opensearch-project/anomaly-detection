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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.LabelledAnomalyGenerator;
import org.opensearch.ad.util.MultiDimDataWithTime;

import com.google.common.collect.ImmutableList;

public class HCADModelPerfTests extends AbstractCosineDataTest {
    /**
     * A template to perform precision/recall test.
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
        int dataSize = 20 * AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
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
            .newInstance()
            .setDetectionInterval(new IntervalTimeConfiguration(interval, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .setShingleSize(AnomalyDetectorSettings.DEFAULT_MULTI_ENTITY_SHINGLE)
            .build();

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        for (int z = 1; z <= numberOfTrials; z++) {
            long seed = z;
            LOG.info("seed = " + seed);

            searchFeatureDao = mock(SearchFeatureDao.class);

            featureManager = new FeatureManager(
                searchFeatureDao,
                interpolator,
                clock,
                AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
                AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
                AnomalyDetectorSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
                AnomalyDetectorSettings.MIN_TRAIN_SAMPLES,
                AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
                AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
                AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
                AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
                AnomalyDetectorSettings.HOURLY_MAINTENANCE,
                threadPool,
                AnomalyDetectorPlugin.AD_THREAD_POOL_NAME
            );

            entityColdStarter = new EntityColdStarter(
                clock,
                threadPool,
                stateManager,
                AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
                AnomalyDetectorSettings.MULTI_ENTITY_NUM_TREES,
                AnomalyDetectorSettings.TIME_DECAY,
                numMinSamples,
                AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
                AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
                interpolator,
                searchFeatureDao,
                AnomalyDetectorSettings.DEFAULT_MULTI_ENTITY_SHINGLE,
                AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
                AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
                AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
                AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
                AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
                AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES,
                featureManager,
                AnomalyDetectorSettings.HOURLY_MAINTENANCE,
                AnomalyDetectorSettings.MAX_SMALL_STATES,
                checkpoint,
                settings,
                seed
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
            when(clock.millis()).thenReturn(timestamps[trainTestSplit - 1]);

            doAnswer(invocation -> {
                ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
                listener.onResponse(new SimpleImmutableEntry<>(Optional.of(timestamps[0]), Optional.of(timestamps[trainTestSplit - 1])));
                return null;
            }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

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

                ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
                listener.onResponse(coldStartSamples);
                return null;
            }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

            String entityNamez = entityName + z;
            String modelId = detector.getDetectorId() + "_entity_" + entityNamez;
            EntityModel model = new EntityModel(modelId, new ArrayDeque<>(), null, null);
            ModelState<EntityModel> modelState = new ModelState<>(
                model,
                modelId,
                detector.getDetectorId(),
                ModelType.ENTITY.getName(),
                clock,
                priority
            );

            entityColdStarter.trainModel(new ArrayDeque<>(), modelId, entityNamez, detector.getDetectorId(), modelState);

            waitForColdStartFinish();
            assertTrue(model.getRcf() != null);

            int tp = 0;
            int fp = 0;
            int fn = 0;
            long[] changeTimestamps = dataWithKeys.changeTimeStampsMs;

            for (int j = trainTestSplit; j < data.length; j++) {
                ThresholdingResult result = modelManager
                    .getAnomalyResultForEntity(detector.getDetectorId(), data[j], entityNamez, modelState, modelId);
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
        }

        double avgPrec = totalPrec / numberOfTrials;
        double avgRecall = totalRecall / numberOfTrials;
        LOG.info("{} features, Interval {}, Precision: {}, recall: {}", baseDimension, detectorIntervalMins, avgPrec, avgRecall);
        assertTrue("average precision is " + avgPrec, avgPrec >= precisionThreshold);
        assertTrue("average recall is " + avgRecall, avgRecall >= recallThreshold);
    }

    public void testAverageAccuracy() throws Exception {
        for (final boolean anomalyIndependent : new boolean[] { false, true }) {
            LOG.info("Anomalies independence: " + anomalyIndependent);
            // 10 minute interval, 4 features
            averageAccuracyTemplate(10, 0.8f, 0.1f, 4, anomalyIndependent);

            // 10 minute interval, 2 features
            averageAccuracyTemplate(10, 0.7f, 0.2f, 2, anomalyIndependent);

            // 10 minute interval, 1 features
            averageAccuracyTemplate(10, 0.5f, 0.2f, 1, anomalyIndependent);

            // 5 minute interval, 4 features
            averageAccuracyTemplate(5, 0.3f, 0.1f, 4, anomalyIndependent);

            // 5 minute interval, 2 features
            averageAccuracyTemplate(5, 0.7f, 0.2f, 2, anomalyIndependent);

            // 5 minute interval, 1 features
            averageAccuracyTemplate(5, 0.5f, 0.2f, 1, anomalyIndependent);
        }
    }
}
