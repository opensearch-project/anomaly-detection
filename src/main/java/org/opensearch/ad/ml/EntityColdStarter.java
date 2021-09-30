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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
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

import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

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
    private final int maxSampleStride;
    private final int maxTrainSamples;
    private final Interpolator interpolator;
    private final SearchFeatureDao searchFeatureDao;
    private Instant lastThrottledColdStartTime;
    private final FeatureManager featureManager;
    private int coolDownMinutes;
    // A bloom filter checked before cold start to ensure we don't repeatedly
    // retry cold start of the same model.
    // keys are detector ids.
    private Map<String, DoorKeeper> doorKeepers;
    private final Duration modelTtl;
    private final CheckpointWriteWorker checkpointWriteQueue;
    // make sure rcf use a specific random seed. Otherwise, we will use a random random (not a typo) seed.
    // this is mainly used for testing to make sure the model we trained and the reference rcf produce
    // the same results
    private final long rcfSeed;

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
     * @param thresholdMinPvalue min P-value for thresholding
     * @param featureManager Used to create features for models.
     * @param settings ES settings accessor
     * @param modelTtl time-to-live before last access time of the cold start cache.
     *   We have a cache to record entities that have run cold starts to avoid
     *   repeated unsuccessful cold start.
     * @param checkpointWriteQueue queue to insert model checkpoints
     * @param rcfSeed rcf random seed
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
        double thresholdMinPvalue,
        FeatureManager featureManager,
        Settings settings,
        Duration modelTtl,
        CheckpointWriteWorker checkpointWriteQueue,
        long rcfSeed
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
        this.thresholdMinPvalue = thresholdMinPvalue;
        this.featureManager = featureManager;
        this.coolDownMinutes = (int) (COOLDOWN_MINUTES.get(settings).getMinutes());
        this.doorKeepers = new ConcurrentHashMap<>();
        this.modelTtl = modelTtl;
        this.checkpointWriteQueue = checkpointWriteQueue;
        this.rcfSeed = rcfSeed;
    }

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
        double thresholdMinPvalue,
        FeatureManager featureManager,
        Settings settings,
        Duration modelTtl,
        CheckpointWriteWorker checkpointWriteQueue
    ) {
        this(
            clock,
            threadPool,
            nodeStateManager,
            rcfSampleSize,
            numberOfTrees,
            rcfTimeDecay,
            numMinSamples,
            maxSampleStride,
            maxTrainSamples,
            interpolator,
            searchFeatureDao,
            thresholdMinPvalue,
            featureManager,
            settings,
            modelTtl,
            checkpointWriteQueue,
            -1
        );
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
        AnomalyDetector detector,
        ActionListener<Void> listener
    ) {
        logger.debug("Trigger cold start for {}", modelId);

        if (lastThrottledColdStartTime.plus(Duration.ofMinutes(coolDownMinutes)).isAfter(clock.instant())) {
            listener.onResponse(null);
            return;
        }

        boolean earlyExit = true;
        try {
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
                            trainModelFromDataSegments(dataPoints, entity, modelState, detector.getShingleSize());
                        }
                        logger.info("Succeeded in training entity: {}", modelId);
                    } else {
                        logger.info("Cannot get training data for {}", modelId);
                    }
                    listener.onResponse(null);
                } catch (Exception e) {
                    listener.onFailure(e);
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
                    listener.onFailure(exception);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });

            threadPool
                .executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)
                .execute(
                    () -> getEntityColdStartData(
                        detectorId,
                        entity,
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
    }

    /**
     * Train model using given data points.
     *
     * @param dataPoints List of continuous data points, in ascending order of timestamps
     * @param entity Entity instance
     * @param entityState Entity state associated with the model Id
     */
    private void trainModelFromDataSegments(
        List<double[][]> dataPoints,
        Entity entity,
        ModelState<EntityModel> entityState,
        int shingleSize
    ) {
        if (dataPoints == null || dataPoints.size() == 0 || dataPoints.get(0) == null || dataPoints.get(0).length == 0) {
            throw new IllegalArgumentException("Data points must not be empty.");
        }

        int dimensions = dataPoints.get(0)[0].length * shingleSize;
        ThresholdedRandomCutForest.Builder<?> rcfBuilder = ThresholdedRandomCutForest
            .builder()
            .dimensions(dimensions)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            // same with dimension for opportunistic memory saving
            // Usually, we use it as shingleSize(dimension). When a new point comes in, we will
            // look at the point store if there is any overlapping. Say the previously-stored
            // vector is x1, x2, x3, x4, now we add x3, x4, x5, x6. RCF will recognize
            // overlapping x3, x4, and only store x5, x6.
            .shingleSize(shingleSize)
            .internalShinglingEnabled(true)
            .anomalyRate(1 - this.thresholdMinPvalue);

        if (rcfSeed > 0) {
            rcfBuilder.randomSeed(rcfSeed);
        }
        ThresholdedRandomCutForest trcf = new ThresholdedRandomCutForest(rcfBuilder);

        dataPoints.stream().flatMap(d -> Arrays.stream(d)).forEach(s -> trcf.process(s, 0));

        EntityModel model = entityState.getModel();
        if (model == null) {
            model = new EntityModel(entity, new ArrayDeque<>(), null);
        }
        model.setTrcf(trcf);

        entityState.setLastUsedTime(clock.instant());

        // save to checkpoint
        checkpointWriteQueue.write(entityState, true, RequestPriority.MEDIUM);
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
     * @param listener listener to return training data
     */
    private void getEntityColdStartData(String detectorId, Entity entity, ActionListener<Optional<List<double[][]>>> listener) {
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
                                coldStartData.add(points);
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
                            coldStartData.add(points);
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
        nodeStateManager.getAnomalyDetector(detectorId, ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                logger.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                listener.onFailure(new AnomalyDetectionException(detectorId, "fail to find detector"));
                return;
            }

            AnomalyDetector detector = detectorOptional.get();

            Queue<double[]> samples = modelState.getModel().getSamples();
            String modelId = modelState.getModelId();

            if (samples.size() < this.numMinSamples) {
                // we cannot get last RCF score since cold start happens asynchronously
                coldStart(modelId, entity, detectorId, modelState, detector, listener);
            } else {
                try {
                    trainModelFromDataSegments(
                        Collections.singletonList(samples.toArray(new double[0][0])),
                        entity,
                        modelState,
                        detector.getShingleSize()
                    );
                    listener.onResponse(null);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

        }, listener::onFailure));
    }

    public void trainModelFromExistingSamples(ModelState<EntityModel> modelState, int shingleSize) {
        if (modelState == null || modelState.getModel() == null || modelState.getModel().getSamples() == null) {
            return;
        }

        EntityModel model = modelState.getModel();
        Queue<double[]> samples = model.getSamples();
        if (samples.size() >= this.numMinSamples) {
            try {
                trainModelFromDataSegments(
                    Collections.singletonList(samples.toArray(new double[0][0])),
                    model.getEntity().orElse(null),
                    modelState,
                    shingleSize
                );
                // clear samples after using
                samples.clear();
            } catch (Exception e) {
                // e.g., exception from rcf. We can do nothing except logging the error
                // We won't retry training for the same entity in the cooldown period
                // (60 detector intervals).
                logger.error("Unexpected training error", e);
            }

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
            model = new EntityModel(null, new ArrayDeque<>(), null);
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
