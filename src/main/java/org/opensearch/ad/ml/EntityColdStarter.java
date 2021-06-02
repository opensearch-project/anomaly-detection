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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.ml;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.dataprocessor.Interpolator;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

import com.amazon.randomcutforest.RandomCutForest;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Training models for multi-entity detectors
 *
 */
public class EntityColdStarter {
    private static final Logger logger = LogManager.getLogger(EntityColdStarter.class);
    private final Clock clock;
    private final ThreadPool threadPool;
    private final NodeStateManager nodeStateManager;
    private final int rcfSampleSize;
    private final int numberOfTrees;
    private final double rcfTimeDecay;
    private final int numMinSamples;
    private final double thresholdMinPvalue;
    private final double thresholdMaxRankError;
    private final double thresholdMaxScore;
    private final int thresholdNumLogNormalQuantiles;
    private final int thresholdDownsamples;
    private final long thresholdMaxSamples;
    private final int maxSampleStride;
    private final int maxTrainSamples;
    private final Interpolator interpolator;
    private final SearchFeatureDao searchFeatureDao;
    private final int shingleSize;
    private Instant lastThrottledColdStartTime;
    private final FeatureManager featureManager;
    private final Cache<String, Instant> lastColdStartTime;
    private final CheckpointDao checkpointDao;
    private int coolDownMinutes;

    /**
     * Constructor
     *
     * @param clock UTC clock
     * @param threadPool Accessor to different threadpools
     * @param nodeStateManager Storing node state
     * @param rcfSampleSize The sample size used by stream samplers in this forest
     * @param numberOfTrees The number of trees in this forest.
     * @param rcfTimeDecay rcf samples time decay constant
     * @param numMinSamples The number of points required by stream samplers before
     *  results are returned.
     * @param maxSampleStride Sample distances measured in detector intervals.
     * @param maxTrainSamples Max train samples to collect.
     * @param interpolator Used to generate data points between samples.
     * @param searchFeatureDao Used to issue ES queries.
     * @param shingleSize The size of a data point window that appear consecutively.
     * @param thresholdMinPvalue min P-value for thresholding
     * @param thresholdMaxRankError  max rank error for thresholding
     * @param thresholdMaxScore max RCF score to thresholding
     * @param thresholdNumLogNormalQuantiles num of lognormal quantiles for thresholding
     * @param thresholdDownsamples the number of samples to keep during downsampling
     * @param thresholdMaxSamples the max number of samples before downsampling
     * @param featureManager Used to create features for models.
     * @param lastColdStartTimestampTtl max time to retain last cold start timestamp
     * @param maxCacheSize max cache size
     * @param checkpointDao utility to interact with the checkpoint index
     * @param settings ES settings accessor
     */
    public EntityColdStarter(
        Clock clock,
        ThreadPool threadPool,
        NodeStateManager nodeStateManager,
        int rcfSampleSize,
        int numberOfTrees,
        double rcfTimeDecay,
        int numMinSamples,
        int maxSampleStride,
        int maxTrainSamples,
        Interpolator interpolator,
        SearchFeatureDao searchFeatureDao,
        int shingleSize,
        double thresholdMinPvalue,
        double thresholdMaxRankError,
        double thresholdMaxScore,
        int thresholdNumLogNormalQuantiles,
        int thresholdDownsamples,
        long thresholdMaxSamples,
        FeatureManager featureManager,
        Duration lastColdStartTimestampTtl,
        long maxCacheSize,
        CheckpointDao checkpointDao,
        Settings settings
    ) {
        this.clock = clock;
        this.lastThrottledColdStartTime = Instant.MIN;
        this.threadPool = threadPool;
        this.nodeStateManager = nodeStateManager;
        this.rcfSampleSize = rcfSampleSize;
        this.numberOfTrees = numberOfTrees;
        this.rcfTimeDecay = rcfTimeDecay;
        this.numMinSamples = numMinSamples;
        this.maxSampleStride = maxSampleStride;
        this.maxTrainSamples = maxTrainSamples;
        this.interpolator = interpolator;
        this.searchFeatureDao = searchFeatureDao;
        this.shingleSize = shingleSize;
        this.thresholdMinPvalue = thresholdMinPvalue;
        this.thresholdMaxRankError = thresholdMaxRankError;
        this.thresholdMaxScore = thresholdMaxScore;
        this.thresholdNumLogNormalQuantiles = thresholdNumLogNormalQuantiles;
        this.thresholdDownsamples = thresholdDownsamples;
        this.thresholdMaxSamples = thresholdMaxSamples;
        this.featureManager = featureManager;

        this.lastColdStartTime = CacheBuilder
            .newBuilder()
            .expireAfterAccess(lastColdStartTimestampTtl.toHours(), TimeUnit.HOURS)
            .maximumSize(maxCacheSize)
            .concurrencyLevel(1)
            .build();
        this.checkpointDao = checkpointDao;
        this.coolDownMinutes = (int) (COOLDOWN_MINUTES.get(settings).getMinutes());
    }

    /**
     * Training model for an entity
     * @param modelId model Id corresponding to the entity
     * @param entityName the entity's name
     * @param detectorId the detector Id corresponding to the entity
     * @param modelState model state associated with the entity
     */
    private void coldStart(String modelId, String entityName, String detectorId, ModelState<EntityModel> modelState) {
        // Rate limiting: if last cold start of the detector is not finished, we don't trigger another one.
        if (nodeStateManager.isColdStartRunning(detectorId)) {
            return;
        }

        // Won't retry cold start within one hour for an entity; if threadpool queue is full, won't retry within 5 minutes
        // 5 minutes is derived by 1000 (threadpool queue size) / 4 (1 cold start per 4 seconds according to the Http logs
        // experiment) = 250 seconds.
        if (lastColdStartTime.getIfPresent(modelId) == null
            && lastThrottledColdStartTime.plus(Duration.ofMinutes(coolDownMinutes)).isBefore(clock.instant())) {

            final Releasable coldStartFinishingCallback = nodeStateManager.markColdStartRunning(detectorId);

            logger.debug("Trigger cold start for {}", modelId);

            ActionListener<Optional<List<double[][]>>> nestedListener = ActionListener.wrap(trainingData -> {
                if (trainingData.isPresent()) {
                    List<double[][]> dataPoints = trainingData.get();
                    // only train models if we have enough samples
                    if (hasEnoughSample(dataPoints, modelState) == false) {
                        combineTrainSamples(dataPoints, modelId, modelState);
                    } else {
                        trainModelFromDataSegments(dataPoints, modelId, modelState);
                    }
                    logger.info("Succeeded in training entity: {}", modelId);
                } else {
                    logger.info("Cannot get training data for {}", modelId);
                }
            }, exception -> {
                Throwable cause = Throwables.getRootCause(exception);
                if (cause instanceof RejectedExecutionException) {
                    logger.error("too many requests");
                    lastThrottledColdStartTime = Instant.now();
                } else if (cause instanceof AnomalyDetectionException || exception instanceof AnomalyDetectionException) {
                    // e.g., cannot find anomaly detector
                    nodeStateManager.setLastColdStartException(detectorId, (AnomalyDetectionException) exception);
                } else {
                    logger.error(new ParameterizedMessage("Error while cold start {}", modelId), exception);
                }
            });

            final ActionListener<Optional<List<double[][]>>> listenerWithReleaseCallback = ActionListener
                .runAfter(nestedListener, coldStartFinishingCallback::close);

            threadPool
                .executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)
                .execute(
                    () -> getEntityColdStartData(
                        detectorId,
                        entityName,
                        shingleSize,
                        new ThreadedActionListener<>(
                            logger,
                            threadPool,
                            AnomalyDetectorPlugin.AD_THREAD_POOL_NAME,
                            listenerWithReleaseCallback,
                            false
                        )
                    )
                );

            lastColdStartTime.put(modelId, Instant.now());
        }
    }

    /**
     * Train model using given data points.
     *
     * @param dataPoints List of continuous data points, in ascending order of timestamps
     * @param modelId The model Id
     * @param entityState Entity state associated with the model Id
     */
    private void trainModelFromDataSegments(List<double[][]> dataPoints, String modelId, ModelState<EntityModel> entityState) {
        if (dataPoints == null || dataPoints.size() == 0 || dataPoints.get(0) == null || dataPoints.get(0).length == 0) {
            throw new IllegalArgumentException("Data points must not be empty.");
        }

        int rcfNumFeatures = dataPoints.get(0)[0].length;
        RandomCutForest rcf = RandomCutForest
            .builder()
            .dimensions(rcfNumFeatures)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .lambda(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .build();
        List<double[]> allScores = new ArrayList<>();
        int totalLength = 0;
        // get continuous data points and send for training
        for (double[][] continuousDataPoints : dataPoints) {
            double[] scores = trainRCFModel(continuousDataPoints, modelId, rcf);
            allScores.add(scores);
            totalLength += scores.length;
        }

        EntityModel model = entityState.getModel();
        if (model == null) {
            model = new EntityModel(modelId, new ArrayDeque<>(), null, null);
        }
        model.setRcf(rcf);
        double[] joinedScores = new double[totalLength];

        int destStart = 0;
        for (double[] scores : allScores) {
            System.arraycopy(scores, 0, joinedScores, destStart, scores.length);
            destStart += scores.length;
        }

        // Train thresholding model
        ThresholdingModel threshold = new HybridThresholdingModel(
            thresholdMinPvalue,
            thresholdMaxRankError,
            thresholdMaxScore,
            thresholdNumLogNormalQuantiles,
            thresholdDownsamples,
            thresholdMaxSamples
        );
        threshold.train(joinedScores);
        model.setThreshold(threshold);

        entityState.setLastUsedTime(clock.instant());

        // save to checkpoint
        checkpointDao.write(entityState, modelId, true);
    }

    /**
     * Train the RCF model using given data points
     * @param dataPoints Data points
     * @param modelId The model Id
     * @param rcf RCF model to be trained
     * @return scores returned by RCF models
     */
    private double[] trainRCFModel(double[][] dataPoints, String modelId, RandomCutForest rcf) {
        if (dataPoints.length == 0 || dataPoints[0].length == 0) {
            throw new IllegalArgumentException("Data points must not be empty.");
        }

        double[] scores = new double[dataPoints.length];

        for (int j = 0; j < dataPoints.length; j++) {
            scores[j] = rcf.getAnomalyScore(dataPoints[j]);
            rcf.update(dataPoints[j]);
        }

        return DoubleStream.of(scores).filter(score -> score > 0).toArray();
    }

    /**
     * Get training data for an entity.
     *
     * We first note the maximum and minimum timestamp, and sample at most 24 points
     * (with 60 points apart between two neighboring samples) between those minimum
     * and maximum timestamps.  Samples can be missing.  We only interpolate points
     * between present neighboring samples. We then transform samples and interpolate
     * points to shingles. Finally, full shingles will be used for cold start.
     *
     * @param detectorId detector Id
     * @param entityName entity's name
     * @param entityShingleSize model's shingle size
     * @param listener listener to return training data
     */
    private void getEntityColdStartData(
        String detectorId,
        String entityName,
        int entityShingleSize,
        ActionListener<Optional<List<double[][]>>> listener
    ) {
        ActionListener<Optional<AnomalyDetector>> getDetectorListener = ActionListener.wrap(detectorOp -> {
            if (!detectorOp.isPresent()) {
                nodeStateManager
                    .setLastColdStartException(detectorId, new EndRunException(detectorId, "AnomalyDetector is not available.", true));
                return;
            }
            List<double[][]> coldStartData = new ArrayList<>();
            AnomalyDetector detector = detectorOp.get();

            ActionListener<Entry<Optional<Long>, Optional<Long>>> minMaxTimeListener = ActionListener.wrap(minMaxDateTime -> {
                Optional<Long> earliest = minMaxDateTime.getKey();
                Optional<Long> latest = minMaxDateTime.getValue();
                if (earliest.isPresent() && latest.isPresent()) {
                    long startTimeMs = earliest.get().longValue();
                    long endTimeMs = latest.get().longValue();
                    List<Entry<Long, Long>> sampleRanges = getTrainSampleRanges(
                        detector,
                        startTimeMs,
                        endTimeMs,
                        maxSampleStride,
                        maxTrainSamples
                    );

                    if (sampleRanges.isEmpty()) {
                        listener.onResponse(Optional.empty());
                        return;
                    }

                    ActionListener<List<Optional<double[]>>> getFeaturelistener = ActionListener.wrap(featureSamples -> {
                        ArrayList<double[]> continuousSampledFeatures = new ArrayList<>(maxTrainSamples);

                        // featuresSamples are in ascending order of time.
                        for (int i = 0; i < featureSamples.size(); i++) {
                            Optional<double[]> featuresOptional = featureSamples.get(i);
                            if (featuresOptional.isPresent()) {
                                continuousSampledFeatures.add(featuresOptional.get());
                            } else if (!continuousSampledFeatures.isEmpty()) {
                                double[][] continuousSampledArray = continuousSampledFeatures.toArray(new double[0][0]);
                                double[][] points = featureManager
                                    .transpose(
                                        interpolator
                                            .interpolate(
                                                featureManager.transpose(continuousSampledArray),
                                                maxSampleStride * (continuousSampledArray.length - 1) + 1
                                            )
                                    );
                                coldStartData.add(featureManager.batchShingle(points, entityShingleSize));
                                continuousSampledFeatures.clear();
                            }
                        }
                        if (!continuousSampledFeatures.isEmpty()) {
                            double[][] continuousSampledArray = continuousSampledFeatures.toArray(new double[0][0]);
                            double[][] points = featureManager
                                .transpose(
                                    interpolator
                                        .interpolate(
                                            featureManager.transpose(continuousSampledArray),
                                            maxSampleStride * (continuousSampledArray.length - 1) + 1
                                        )
                                );
                            coldStartData.add(featureManager.batchShingle(points, entityShingleSize));
                        }
                        if (coldStartData.isEmpty()) {
                            listener.onResponse(Optional.empty());
                        } else {
                            listener.onResponse(Optional.of(coldStartData));
                        }
                    }, listener::onFailure);

                    searchFeatureDao
                        .getColdStartSamplesForPeriods(
                            detector,
                            sampleRanges,
                            entityName,
                            false,
                            new ThreadedActionListener<>(
                                logger,
                                threadPool,
                                AnomalyDetectorPlugin.AD_THREAD_POOL_NAME,
                                getFeaturelistener,
                                false
                            )
                        );
                } else {
                    listener.onResponse(Optional.empty());
                }

            }, listener::onFailure);

            // TODO: use current data time as max time and current data as last data point
            searchFeatureDao
                .getEntityMinMaxDataTime(
                    detector,
                    entityName,
                    new ThreadedActionListener<>(logger, threadPool, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME, minMaxTimeListener, false)
                );

        }, listener::onFailure);

        nodeStateManager
            .getAnomalyDetector(
                detectorId,
                new ThreadedActionListener<>(logger, threadPool, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME, getDetectorListener, false)
            );
    }

    /**
     * Get train samples within a time range.
     *
     * @param detector accessor to detector config
     * @param startMilli range start
     * @param endMilli range end
     * @param stride the number of intervals between two samples
     * @param maxTrainSamples maximum training samples to fetch
     * @return list of sample time ranges
     */
    private List<Entry<Long, Long>> getTrainSampleRanges(
        AnomalyDetector detector,
        long startMilli,
        long endMilli,
        int stride,
        int maxTrainSamples
    ) {
        long bucketSize = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMillis();
        int numBuckets = (int) Math.floor((endMilli - startMilli) / (double) bucketSize);
        // adjust if numStrides is more than the max samples
        int numStrides = Math.min((int) Math.floor(numBuckets / (double) stride), maxTrainSamples);
        List<Entry<Long, Long>> sampleRanges = Stream
            .iterate(endMilli, i -> i - stride * bucketSize)
            .limit(numStrides)
            .map(time -> new SimpleImmutableEntry<>(time - bucketSize, time))
            .collect(Collectors.toList());
        return sampleRanges;
    }

    /**
     * Train models for the given entity
     * @param samples Recent sample history
     * @param modelId Model Id
     * @param entityName The entity's name
     * @param detectorId Detector Id
     * @param modelState Model state associated with the entity
     */
    public void trainModel(
        Queue<double[]> samples,
        String modelId,
        String entityName,
        String detectorId,
        ModelState<EntityModel> modelState
    ) {
        if (samples.size() < this.numMinSamples) {
            // we cannot get last RCF score since cold start happens asynchronously
            coldStart(modelId, entityName, detectorId, modelState);
        } else {
            double[][] trainData = featureManager.batchShingle(samples.toArray(new double[0][0]), this.shingleSize);
            trainModelFromDataSegments(Collections.singletonList(trainData), modelId, modelState);
        }
    }

    /**
     * TODO: make it work for shingle.
     *
     * @param dataPoints training data generated from cold start
     * @param entityState entity State
     * @return whether the total available sample size meets our minimum sample requirement
     */
    private boolean hasEnoughSample(List<double[][]> dataPoints, ModelState<EntityModel> entityState) {
        int totalSize = 0;
        for (double[][] consecutivePoints : dataPoints) {
            totalSize += consecutivePoints.length;
        }
        EntityModel model = entityState.getModel();
        if (model != null) {
            totalSize += model.getSamples().size();
        }

        return totalSize >= this.numMinSamples;
    }

    /**
     * TODO: make it work for shingle
     * Precondition: we don't have enough training data.
     * Combine training data with existing sample data.  Existing samples either
     * predates or coincide with cold start data.  In either case, combining them
     * without reorder based on timestamp is fine.  RCF on one-dimensional datapoints
     * without shingling is similar to just using CDF sketch on the values.  We
     * are just finding extreme values.
     *
     * @param coldstartDatapoints training data generated from cold start
     * @param entityState entity State
     */
    private void combineTrainSamples(List<double[][]> coldstartDatapoints, String modelId, ModelState<EntityModel> entityState) {
        EntityModel model = entityState.getModel();
        if (model == null) {
            model = new EntityModel(modelId, new ArrayDeque<>(), null, null);
        }
        for (double[][] consecutivePoints : coldstartDatapoints) {
            for (int i = 0; i < consecutivePoints.length; i++) {
                model.addSample(consecutivePoints[i]);
            }
        }
        // save to checkpoint
        checkpointDao.write(entityState, modelId, true);
    }
}
