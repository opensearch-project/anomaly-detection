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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
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
import org.opensearch.ad.MaintenanceState;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.caching.DoorKeeper;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.dataprocessor.Interpolator;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.ratelimit.CheckpointWriteWorker;
import org.opensearch.ad.ratelimit.RequestPriority;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

import com.amazon.randomcutforest.RandomCutForest;

/**
 * Training models for HCAD detectors
 *
 */
public class EntityColdStarter implements MaintenanceState {
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
    private int coolDownMinutes;
    // A bloom filter checked before cold start to ensure we don't repeatedly
    // retry cold start of the same model.
    // keys are detector ids.
    private Map<String, DoorKeeper> doorKeepers;
    private final Duration modelTtl;
    private final CheckpointWriteWorker checkpointWriteQueue;

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
     * @param settings ES settings accessor
     * @param modelTtl time-to-live before last access time of the cold start cache.
     *   We have a cache to record entities that have run cold starts to avoid
     *   repeated unsuccessful cold start.
     * @param checkpointWriteQueue queue to insert model checkpoints
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
        Settings settings,
        Duration modelTtl,
        CheckpointWriteWorker checkpointWriteQueue
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
        this.coolDownMinutes = (int) (COOLDOWN_MINUTES.get(settings).getMinutes());
        this.doorKeepers = new ConcurrentHashMap<>();
        this.modelTtl = modelTtl;
        this.checkpointWriteQueue = checkpointWriteQueue;
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        String modelId,
        Entity entity,
        String detectorId,
        ModelState<EntityModel> modelState,
        ActionListener<Void> listener
    ) {
        return ActionListener.wrap(detectorOptional -> {
            boolean earlyExit = true;
            try {
                if (false == detectorOptional.isPresent()) {
                    logger.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                    return;
                }

                AnomalyDetector detector = detectorOptional.get();

                DoorKeeper doorKeeper = doorKeepers
                    .computeIfAbsent(
                        detectorId,
                        id -> {
                            // reset every 60 intervals
                            return new DoorKeeper(
                                AnomalyDetectorSettings.DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION,
                                AnomalyDetectorSettings.DOOR_KEEPER_FAULSE_POSITIVE_RATE,
                                detector.getDetectionIntervalDuration().multipliedBy(AnomalyDetectorSettings.DOOR_KEEPER_MAINTENANCE_FREQ),
                                clock
                            );
                        }
                    );

                // Won't retry cold start within 60 intervals for an entity
                if (doorKeeper.mightContain(modelId)) {
                    return;
                }

                doorKeeper.put(modelId);

                ActionListener<Optional<List<double[][]>>> coldStartCallBack = ActionListener.wrap(trainingData -> {
                    try {
                        if (trainingData.isPresent()) {
                            List<double[][]> dataPoints = trainingData.get();
                            // only train models if we have enough samples
                            if (hasEnoughSample(dataPoints, modelState) == false) {
                                combineTrainSamples(dataPoints, modelId, modelState);
                            } else {
                                trainModelFromDataSegments(dataPoints, entity, modelState);
                            }
                            logger.info("Succeeded in training entity: {}", modelId);
                        } else {
                            logger.info("Cannot get training data for {}", modelId);
                        }
                    } finally {
                        listener.onResponse(null);
                    }

                }, exception -> {
                    try {
                        logger.error(new ParameterizedMessage("Error while cold start {}", modelId), exception);
                        Throwable cause = Throwables.getRootCause(exception);
                        if (ExceptionUtil.isOverloaded(cause)) {
                            logger.error("too many requests");
                            lastThrottledColdStartTime = Instant.now();
                        } else if (cause instanceof AnomalyDetectionException || exception instanceof AnomalyDetectionException) {
                            // e.g., cannot find anomaly detector
                            nodeStateManager.setException(detectorId, exception);
                        } else {
                            nodeStateManager.setException(detectorId, new AnomalyDetectionException(detectorId, cause));
                        }
                    } finally {
                        listener.onFailure(exception);
                    }
                });

                threadPool
                    .executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)
                    .execute(
                        () -> getEntityColdStartData(
                            detectorId,
                            entity,
                            shingleSize,
                            new ThreadedActionListener<>(
                                logger,
                                threadPool,
                                AnomalyDetectorPlugin.AD_THREAD_POOL_NAME,
                                coldStartCallBack,
                                false
                            )
                        )
                    );
                earlyExit = false;
            } finally {
                if (earlyExit) {
                    listener.onResponse(null);
                }
            }

        }, exception -> {
            logger.error(new ParameterizedMessage("fail to get detector [{}]", detectorId), exception);
            listener.onFailure(exception);
        });
    }

    /**
     * Training model for an entity
     * @param modelId model Id corresponding to the entity
     * @param entity the entity's information
     * @param detectorId the detector Id corresponding to the entity
     * @param modelState model state associated with the entity
     * @param listener call back to call after cold start
     */
    private void coldStart(
        String modelId,
        Entity entity,
        String detectorId,
        ModelState<EntityModel> modelState,
        ActionListener<Void> listener
    ) {
        logger.debug("Trigger cold start for {}", modelId);

        if (lastThrottledColdStartTime.plus(Duration.ofMinutes(coolDownMinutes)).isAfter(clock.instant())) {
            listener.onResponse(null);
            return;
        }

        nodeStateManager.getAnomalyDetector(detectorId, onGetDetector(modelId, entity, detectorId, modelState, listener));
    }

    /**
     * Train model using given data points.
     *
     * @param dataPoints List of continuous data points, in ascending order of timestamps
     * @param entity Entity instance
     * @param entityState Entity state associated with the model Id
     */
    private void trainModelFromDataSegments(List<double[][]> dataPoints, Entity entity, ModelState<EntityModel> entityState) {
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
            double[] scores = trainRCFModel(continuousDataPoints, rcf);
            allScores.add(scores);
            totalLength += scores.length;
        }

        EntityModel model = entityState.getModel();
        if (model == null) {
            model = new EntityModel(entity, new ArrayDeque<>(), null, null);
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
        checkpointWriteQueue.write(entityState, true, RequestPriority.MEDIUM);
    }

    /**
     * Train the RCF model using given data points
     * @param dataPoints Data points
     * @param rcf RCF model to be trained
     * @return scores returned by RCF models
     */
    private double[] trainRCFModel(double[][] dataPoints, RandomCutForest rcf) {
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
     * @param entity the entity's information
     * @param entityShingleSize model's shingle size
     * @param listener listener to return training data
     */
    private void getEntityColdStartData(
        String detectorId,
        Entity entity,
        int entityShingleSize,
        ActionListener<Optional<List<double[][]>>> listener
    ) {
        ActionListener<Optional<AnomalyDetector>> getDetectorListener = ActionListener.wrap(detectorOp -> {
            if (!detectorOp.isPresent()) {
                nodeStateManager.setException(detectorId, new EndRunException(detectorId, "AnomalyDetector is not available.", true));
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
                            entity,
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
                    entity,
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
     * @param entity The entity info
     * @param detectorId Detector Id
     * @param modelState Model state associated with the entity
     * @param listener callback before the method returns whenever EntityColdStarter
     * finishes training or encounters exceptions.  The listener helps notify the
     * cold start queue to pull another request (if any) to execute.
     */
    public void trainModel(Entity entity, String detectorId, ModelState<EntityModel> modelState, ActionListener<Void> listener) {
        Queue<double[]> samples = modelState.getModel().getSamples();
        String modelId = modelState.getModelId();

        if (samples.size() < this.numMinSamples) {
            // we cannot get last RCF score since cold start happens asynchronously
            coldStart(modelId, entity, detectorId, modelState, listener);
        } else {
            try {
                double[][] trainData = featureManager.batchShingle(samples.toArray(new double[0][0]), this.shingleSize);
                trainModelFromDataSegments(Collections.singletonList(trainData), entity, modelState);
            } finally {
                listener.onResponse(null);
            }
        }
    }

    public void trainModelFromExistingSamples(ModelState<EntityModel> modelState) {
        if (modelState == null || modelState.getModel() == null || modelState.getModel().getSamples() == null) {
            return;
        }

        EntityModel model = modelState.getModel();
        Queue<double[]> samples = model.getSamples();
        if (samples.size() >= this.numMinSamples) {
            double[][] trainData = featureManager.batchShingle(samples.toArray(new double[0][0]), this.shingleSize);
            trainModelFromDataSegments(Collections.singletonList(trainData), model.getEntity().orElse(null), modelState);
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
            model = new EntityModel(null, new ArrayDeque<>(), null, null);
        }
        for (double[][] consecutivePoints : coldstartDatapoints) {
            for (int i = 0; i < consecutivePoints.length; i++) {
                model.addSample(consecutivePoints[i]);
            }
        }
        // save to checkpoint
        checkpointWriteQueue.write(entityState, true, RequestPriority.MEDIUM);
    }

    @Override
    public void maintenance() {
        doorKeepers.entrySet().stream().forEach(doorKeeperEntry -> {
            String detectorId = doorKeeperEntry.getKey();
            DoorKeeper doorKeeper = doorKeeperEntry.getValue();
            if (doorKeeper.expired(modelTtl)) {
                doorKeepers.remove(detectorId);
            } else {
                doorKeeper.maintenance();
            }
        });
    }
}
