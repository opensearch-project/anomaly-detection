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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.IgnoreSimilarExtractor.ThresholdArrays;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ModelUtil;
import org.opensearch.timeseries.util.ParseUtils;

import com.amazon.randomcutforest.config.ForestMode;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Training models for HCAD detectors
 *
 */
public class ADColdStart extends ModelColdStart<ThresholdedRandomCutForest, ADIndex, ADIndexManagement, AnomalyResult> {
    private static final Logger logger = LogManager.getLogger(ADColdStart.class);

    /**
     * Constructor
     *
     * @param clock UTC clock
     * @param threadPool Accessor to different threadpools
     * @param nodeStateManager Storing node state
     * @param rcfSampleSize The sample size used by stream samplers in this forest
     * @param numberOfTrees The number of trees in this forest.
     * @param numMinSamples The number of points required by stream samplers before
     *  results are returned.
     * @param defaultSampleStride default sample distances measured in detector intervals.
     * @param defaultTrainSamples Default train samples to collect.
     * @param searchFeatureDao Used to issue OS queries.
     * @param thresholdMinPvalue min P-value for thresholding
     * @param featureManager Used to create features for models.
     * @param modelTtl time-to-live before last access time of the cold start cache.
     *   We have a cache to record entities that have run cold starts to avoid
     *   repeated unsuccessful cold start.
     * @param rcfSeed rcf random seed
     * @param maxRoundofColdStart max number of rounds of cold start
     * @param coolDownMinutes cool down minutes when OpenSearch is overloaded
     */
    public ADColdStart(
        Clock clock,
        ThreadPool threadPool,
        NodeStateManager nodeStateManager,
        int rcfSampleSize,
        int numberOfTrees,
        int numMinSamples,
        int defaultSampleStride,
        int defaultTrainSamples,
        SearchFeatureDao searchFeatureDao,
        double thresholdMinPvalue,
        FeatureManager featureManager,
        Duration modelTtl,
        long rcfSeed,
        int maxRoundofColdStart,
        int coolDownMinutes,
        int resultSchemaVersion
    ) {
        super(
            modelTtl,
            coolDownMinutes,
            clock,
            threadPool,
            numMinSamples,
            rcfSeed,
            numberOfTrees,
            rcfSampleSize,
            thresholdMinPvalue,
            nodeStateManager,
            defaultSampleStride,
            defaultTrainSamples,
            searchFeatureDao,
            featureManager,
            maxRoundofColdStart,
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
            AnalysisType.AD,
            resultSchemaVersion
        );
    }

    public ADColdStart(
        Clock clock,
        ThreadPool threadPool,
        NodeStateManager nodeStateManager,
        int rcfSampleSize,
        int numberOfTrees,
        int numMinSamples,
        int maxSampleStride,
        int maxTrainSamples,
        SearchFeatureDao searchFeatureDao,
        double thresholdMinPvalue,
        FeatureManager featureManager,
        Duration modelTtl,
        int maxRoundofColdStart,
        int coolDownMinutes,
        int resultSchemaVersion
    ) {
        this(
            clock,
            threadPool,
            nodeStateManager,
            rcfSampleSize,
            numberOfTrees,
            numMinSamples,
            maxSampleStride,
            maxTrainSamples,
            searchFeatureDao,
            thresholdMinPvalue,
            featureManager,
            modelTtl,
            -1,
            maxRoundofColdStart,
            coolDownMinutes,
            resultSchemaVersion
        );
    }

    /**
     * Train model using given data points and save the trained model.
     *
     * @param pointSamples A pair consisting of a queue of continuous data points,
     *  in ascending order of timestamps and last seen sample.
     * @param entityState Entity state associated with the model Id
     * @return the training samples.  We can save the
     * training data in result index so that the frontend can plot it.
     */
    @Override
    protected List<AnomalyResult> trainModelFromDataSegments(
        List<Sample> pointSamples,
        ModelState<ThresholdedRandomCutForest> entityState,
        Config config,
        String taskId
    ) {
        if (pointSamples == null || pointSamples.size() == 0) {
            logger.info("Return early since data points must not be empty.");
            return null;
        }

        double[] firstPoint = pointSamples.get(0).getValueList();
        if (firstPoint == null || firstPoint.length == 0) {
            logger.info("Return early since the first data point must not be empty.");
            return null;
        }

        int shingleSize = config.getShingleSize();
        int baseDimension = firstPoint.length;
        int dimensions = baseDimension * shingleSize;
        ThresholdedRandomCutForest.Builder rcfBuilder = ThresholdedRandomCutForest
            .builder()
            .dimensions(dimensions)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(config.getTimeDecay())
            .transformDecay(config.getTimeDecay())
            // allow enough samples before emitting scores to park service
            .outputAfter(Math.max(shingleSize, numMinSamples))
            .initialAcceptFraction(initialAcceptFraction)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            // same with dimension for opportunistic memory saving
            // Usually, we use it as shingleSize(dimension). When a new point comes in, we will
            // look at the point store if there is any overlapping. Say the previously-stored
            // vector is x1, x2, x3, x4, now we add x3, x4, x5, x6. RCF will recognize
            // overlapping x3, x4, and only store x5, x6.
            .shingleSize(shingleSize)
            .internalShinglingEnabled(true)
            .anomalyRate(1 - this.thresholdMinPvalue)
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true);

        if (shingleSize > 1) {
            rcfBuilder.forestMode(ForestMode.STREAMING_IMPUTE);
            rcfBuilder = applyImputationMethod(config, rcfBuilder);
        } else {
            // imputation with shingle size 1 is not meaningful
            rcfBuilder.forestMode(ForestMode.STANDARD);
        }

        if (rcfSeed > 0) {
            rcfBuilder.randomSeed(rcfSeed);
        }

        AnomalyDetector detector = (AnomalyDetector) config;
        applyRule(rcfBuilder, detector);

        // use build instead of new TRCF(Builder) because build method did extra validation and initialization
        ThresholdedRandomCutForest trcf = rcfBuilder.build();

        // Prepare for sequential processing
        double[][] sequentialData = new double[pointSamples.size()][];
        long[] timestamps = new long[pointSamples.size()];
        List<Pair<Instant, Instant>> sequentialTime = new ArrayList<>();

        // Convert the list of Sample objects into a 2D array + a parallel list of time pairs
        for (int i = 0; i < pointSamples.size(); i++) {
            Sample dataSample = pointSamples.get(i);
            double[] dataValue = dataSample.getValueList();

            sequentialData[i] = dataValue;
            timestamps[i] = dataSample.getDataEndTime().getEpochSecond();
            // Store start and end times together
            sequentialTime.add(Pair.of(dataSample.getDataStartTime(), dataSample.getDataEndTime()));
        }

        // Process data in one go. We need the timestamps for STREAMING_IMPUTE mode
        final List<AnomalyDescriptor> descriptors;
        try {
            descriptors = trcf.processSequentially(sequentialData, timestamps, x -> true);
        } catch (Exception e) {
            // e.g., out of order timestamps
            logger.error("Error while running processSequentially", e);
            // abort and return no results if the sequence processing fails
            return null;
        }
        // Check for size mismatch
        if (descriptors.size() != sequentialTime.size()) {
            logger
                .warn(
                    "processSequentially returned a different size than expected: got [{}], expected [{}].",
                    descriptors.size(),
                    sequentialTime.size()
                );
            return null;
        }

        // Build anomaly results from sequential descriptors
        List<AnomalyResult> results = new ArrayList<>();
        for (int i = 0; i < descriptors.size(); i++) {
            AnomalyDescriptor descriptor = descriptors.get(i);
            double[] dataValue = sequentialData[i];
            Pair<Instant, Instant> time = sequentialTime.get(i);

            // Convert the descriptor into a thresholding result, or anomaly result
            ThresholdingResult thresholdingResult = ModelUtil.toResult(trcf.getForest(), descriptor, dataValue, false, config);

            Instant now = Instant.now();
            results
                .addAll(
                    thresholdingResult
                        .toIndexableResults(
                            config,
                            time.getLeft(),            // Data start time
                            time.getRight(),           // Data end time
                            now,
                            now,
                            ParseUtils.getFeatureData(dataValue, config),
                            entityState.getEntity(),
                            resultMappingVersion,
                            entityState.getModelId(),
                            taskId,
                            null
                        )
                );
        }

        // - lastUsedTime is now updated inside ModelState.setModel()/getModel()/setPriority.
        // - checkpointing is intentionally deferred; ColdStartWorker will persist once score samples
        // are available so long-frequency models checkpoint a more current snapshot.

        entityState.setModel(trcf);

        return results;
    }

    public static void applyRule(ThresholdedRandomCutForest.Builder rcfBuilder, AnomalyDetector detector) {
        ThresholdArrays thresholdArrays = IgnoreSimilarExtractor.processDetectorRules(detector);

        if (thresholdArrays != null) {
            if (thresholdArrays.ignoreSimilarFromAbove != null && thresholdArrays.ignoreSimilarFromAbove.length > 0) {
                rcfBuilder.ignoreNearExpectedFromAbove(thresholdArrays.ignoreSimilarFromAbove);
            }

            if (thresholdArrays.ignoreSimilarFromBelow != null && thresholdArrays.ignoreSimilarFromBelow.length > 0) {
                rcfBuilder.ignoreNearExpectedFromBelow(thresholdArrays.ignoreSimilarFromBelow);
            }

            if (thresholdArrays.ignoreSimilarFromAboveByRatio != null && thresholdArrays.ignoreSimilarFromAboveByRatio.length > 0) {
                rcfBuilder.ignoreNearExpectedFromAboveByRatio(thresholdArrays.ignoreSimilarFromAboveByRatio);
            }

            if (thresholdArrays.ignoreSimilarFromBelowByRatio != null && thresholdArrays.ignoreSimilarFromBelowByRatio.length > 0) {
                rcfBuilder.ignoreNearExpectedFromBelowByRatio(thresholdArrays.ignoreSimilarFromBelowByRatio);
            }
        }
    }
}
