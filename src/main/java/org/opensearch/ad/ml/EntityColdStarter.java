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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.CleanState;
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
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Training models for HCAD detectors
 *
 */
public class EntityColdStarter implements MaintenanceState, CleanState {
    private static final Logger logger = LogManager.getLogger(EntityColdStarter.class);
    private final Clock clock;
    private final ThreadPool threadPool;
    private final NodeStateManager nodeStateManager;
    private final int rcfSampleSize;
    private final int numberOfTrees;
    private final double rcfTimeDecay;
    private final int numMinSamples;
    private final double thresholdMinPvalue;
    private final int defaulStrideLength;
    private final int defaultNumberOfSamples;
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
    private final int maxRoundofColdStart;
    private final double initialAcceptFraction;

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
     * @param defaultSampleStride default sample distances measured in detector intervals.
     * @param defaultTrainSamples Default train samples to collect.
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
     * @param maxRoundofColdStart max number of rounds of cold start
     */
    public EntityColdStarter(
        Clock clock,
        ThreadPool threadPool,
        NodeStateManager nodeStateManager,
        int rcfSampleSize,
        int numberOfTrees,
        double rcfTimeDecay,
        int numMinSamples,
        int defaultSampleStride,
        int defaultTrainSamples,
        Interpolator interpolator,
        SearchFeatureDao searchFeatureDao,
        double thresholdMinPvalue,
        FeatureManager featureManager,
        Settings settings,
        Duration modelTtl,
        CheckpointWriteWorker checkpointWriteQueue,
        long rcfSeed,
        int maxRoundofColdStart
    ) {
        this.clock = clock;
        this.lastThrottledColdStartTime = Instant.MIN;
        this.threadPool = threadPool;
        this.nodeStateManager = nodeStateManager;
        this.rcfSampleSize = rcfSampleSize;
        this.numberOfTrees = numberOfTrees;
        this.rcfTimeDecay = rcfTimeDecay;
        this.numMinSamples = numMinSamples;
        this.defaulStrideLength = defaultSampleStride;
        this.defaultNumberOfSamples = defaultTrainSamples;
        this.interpolator = interpolator;
        this.searchFeatureDao = searchFeatureDao;
        this.thresholdMinPvalue = thresholdMinPvalue;
        this.featureManager = featureManager;
        this.coolDownMinutes = (int) (COOLDOWN_MINUTES.get(settings).getMinutes());
        this.doorKeepers = new ConcurrentHashMap<>();
        this.modelTtl = modelTtl;
        this.checkpointWriteQueue = checkpointWriteQueue;
        this.rcfSeed = rcfSeed;
        this.maxRoundofColdStart = maxRoundofColdStart;
        this.initialAcceptFraction = numMinSamples * 1.0d / rcfSampleSize;
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
        CheckpointWriteWorker checkpointWriteQueue,
        int maxRoundofColdStart
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
            -1,
            maxRoundofColdStart
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

        if (modelState == null || entity == null) {
            listener
                .onFailure(
                    new IllegalArgumentException(
                        String
                            .format(
                                Locale.ROOT,
                                "Cannot have empty model state or entity: model state [%b], entity [%b]",
                                modelState == null,
                                entity == null
                            )
                    )
                );
            return;
        }

        if (lastThrottledColdStartTime.plus(Duration.ofMinutes(coolDownMinutes)).isAfter(clock.instant())) {
            listener.onResponse(null);
            return;
        }

        boolean earlyExit = true;
        try {
            DoorKeeper doorKeeper = doorKeepers.computeIfAbsent(detectorId, id -> {
                // reset every 60 intervals
                return new DoorKeeper(
                    AnomalyDetectorSettings.DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION,
                    AnomalyDetectorSettings.DOOR_KEEPER_FAULSE_POSITIVE_RATE,
                    detector.getDetectionIntervalDuration().multipliedBy(AnomalyDetectorSettings.DOOR_KEEPER_MAINTENANCE_FREQ),
                    clock
                );
            });

            // Won't retry cold start within 60 intervals for an entity
            if (doorKeeper.mightContain(modelId)) {
                return;
            }

            doorKeeper.put(modelId);

            ActionListener<Optional<List<double[][]>>> coldStartCallBack = ActionListener.wrap(trainingData -> {
                try {
                    if (trainingData.isPresent()) {
                        List<double[][]> dataPoints = trainingData.get();
                        extractTrainSamples(dataPoints, modelId, modelState);
                        Queue<double[]> samples = modelState.getModel().getSamples();
                        // only train models if we have enough samples
                        if (samples.size() >= numMinSamples) {
                            // The function trainModelFromDataSegments will save a trained a model. trainModelFromDataSegments is called by
                            // multiple places so I want to make the saving model implicit just in case I forgot.
                            trainModelFromDataSegments(samples, entity, modelState, detector.getShingleSize());
                            logger.info("Succeeded in training entity: {}", modelId);
                        } else {
                            // save to checkpoint
                            checkpointWriteQueue.write(modelState, true, RequestPriority.MEDIUM);
                            logger.info("Not enough data to train entity: {}, currently we have {}", modelId, samples.size());
                        }
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
     * Train model using given data points and save the trained model.
     *
     * @param dataPoints Queue of continuous data points, in ascending order of timestamps
     * @param entity Entity instance
     * @param entityState Entity state associated with the model Id
     */
    private void trainModelFromDataSegments(
        Queue<double[]> dataPoints,
        Entity entity,
        ModelState<EntityModel> entityState,
        int shingleSize
    ) {
        if (dataPoints == null || dataPoints.size() == 0) {
            throw new IllegalArgumentException("Data points must not be empty.");
        }

        double[] firstPoint = dataPoints.peek();
        if (firstPoint == null || firstPoint.length == 0) {
            throw new IllegalArgumentException("Data points must not be empty.");
        }
        int dimensions = firstPoint.length * shingleSize;
        ThresholdedRandomCutForest.Builder<?> rcfBuilder = ThresholdedRandomCutForest
            .builder()
            .dimensions(dimensions)
            .sampleSize(rcfSampleSize)
            .numberOfTrees(numberOfTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(numMinSamples)
            .initialAcceptFraction(initialAcceptFraction)
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

        while (!dataPoints.isEmpty()) {
            trcf.process(dataPoints.poll(), 0);
        }

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
                listener.onFailure(new EndRunException(detectorId, "AnomalyDetector is not available.", false));
                return;
            }
            List<double[][]> coldStartData = new ArrayList<>();
            AnomalyDetector detector = detectorOp.get();

            ActionListener<Optional<Long>> minTimeListener = ActionListener.wrap(earliest -> {
                if (earliest.isPresent()) {
                    long startTimeMs = earliest.get().longValue();

                    // End time uses milliseconds as start time is assumed to be in milliseconds.
                    // Opensearch uses a set of preconfigured formats to recognize and parse these
                    // strings into a long value
                    // representing milliseconds-since-the-epoch in UTC.
                    // More on https://tinyurl.com/wub4fk92

                    long endTimeMs = clock.millis();
                    Pair<Integer, Integer> params = selectRangeParam(detector);
                    int stride = params.getLeft();
                    int numberOfSamples = params.getRight();

                    // we start with round 0
                    getFeatures(listener, 0, coldStartData, detector, entity, stride, numberOfSamples, startTimeMs, endTimeMs);
                } else {
                    listener.onResponse(Optional.empty());
                }
            }, listener::onFailure);

            searchFeatureDao
                .getEntityMinDataTime(
                    detector,
                    entity,
                    new ThreadedActionListener<>(logger, threadPool, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME, minTimeListener, false)
                );

        }, listener::onFailure);

        nodeStateManager
            .getAnomalyDetector(
                detectorId,
                new ThreadedActionListener<>(logger, threadPool, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME, getDetectorListener, false)
            );
    }

    private void getFeatures(
        ActionListener<Optional<List<double[][]>>> listener,
        int round,
        List<double[][]> lastRoundColdStartData,
        AnomalyDetector detector,
        Entity entity,
        int stride,
        int numberOfSamples,
        long startTimeMs,
        long endTimeMs
    ) {
        if (startTimeMs >= endTimeMs || endTimeMs - startTimeMs < detector.getDetectorIntervalInMilliseconds()) {
            listener.onResponse(Optional.of(lastRoundColdStartData));
            return;
        }

        // create ranges in desending order, we will reorder it in ascending order
        // in Opensearch's response
        List<Entry<Long, Long>> sampleRanges = getTrainSampleRanges(detector, startTimeMs, endTimeMs, stride, numberOfSamples);

        if (sampleRanges.isEmpty()) {
            listener.onResponse(Optional.of(lastRoundColdStartData));
            return;
        }

        ActionListener<List<Optional<double[]>>> getFeaturelistener = ActionListener.wrap(featureSamples -> {
            // storing <index, feature vector.
            Pair<Integer, double[]> lastSample = null;
            List<double[][]> currentRoundColdStartData = new ArrayList<>();

            // featuresSamples are in ascending order of time.
            for (int i = 0; i < featureSamples.size(); i++) {
                Optional<double[]> featuresOptional = featureSamples.get(i);
                if (featuresOptional.isPresent()) {
                    // we only need the most recent two samples
                    // For the missing samples we use linear interpolation as well.
                    // Denote the Samples S0, S1, ... as samples in reverse order of time.
                    // Each [Si​,Si−1​]corresponds to strideLength * detector interval.
                    // If we got samples for S0, S1, S4 (both S2 and S3 are missing), then
                    // we interpolate the [S4,S1] into 3*strideLength pieces.
                    if (lastSample != null) {
                        // right sample has index i and feature featuresOptional.get()
                        int numInterpolants = (i - lastSample.getLeft()) * stride + 1;
                        double[][] points = featureManager
                            .transpose(
                                interpolator
                                    .interpolate(
                                        featureManager.transpose(new double[][] { lastSample.getRight(), featuresOptional.get() }),
                                        numInterpolants
                                    )
                            );
                        // the last point will be included in the next iteration or we process
                        // it in the end. We don't want to repeatedly include the samples twice.
                        currentRoundColdStartData.add(Arrays.copyOfRange(points, 0, points.length - 1));
                    }
                    lastSample = Pair.of(i, featuresOptional.get());
                }
            }

            if (lastSample != null) {
                currentRoundColdStartData.add(new double[][] { lastSample.getRight() });
            }
            if (lastRoundColdStartData.size() > 0) {
                currentRoundColdStartData.addAll(lastRoundColdStartData);
            }

            // If the first round of probe provides (32+shingleSize) points (note that if S0 is
            // missing or all Si​ for some i > N is missing then we would miss a lot of points.
            // Otherwise we can issue another round of query — if there is any sample in the
            // second round then we would have 32 + shingleSize points. If there is no sample
            // in the second round then we should wait for real data.
            if (calculateColdStartDataSize(currentRoundColdStartData) >= detector.getShingleSize() + numMinSamples
                || round + 1 >= maxRoundofColdStart) {
                listener.onResponse(Optional.of(currentRoundColdStartData));
            } else {
                // the last sample's start time is the endTimeMs of next round of probe.
                long lastSampleStartTime = sampleRanges.get(sampleRanges.size() - 1).getKey();
                getFeatures(
                    listener,
                    round + 1,
                    currentRoundColdStartData,
                    detector,
                    entity,
                    stride,
                    numberOfSamples,
                    startTimeMs,
                    lastSampleStartTime
                );
            }
        }, listener::onFailure);

        try {
            searchFeatureDao
                .getColdStartSamplesForPeriods(
                    detector,
                    sampleRanges,
                    entity,
                    // Accept empty bucket.
                    // 0, as returned by the engine should constitute a valid answer, “null” is a missing answer — it may be that 0
                    // is meaningless in some case, but 0 is also meaningful in some cases. It may be that the query defining the
                    // metric is ill-formed, but that cannot be solved by cold-start strategy of the AD plugin — if we attempt to do
                    // that, we will have issues with legitimate interpretations of 0.
                    true,
                    new ThreadedActionListener<>(logger, threadPool, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME, getFeaturelistener, false)
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private int calculateColdStartDataSize(List<double[][]> coldStartData) {
        int size = 0;
        for (int i = 0; i < coldStartData.size(); i++) {
            size += coldStartData.get(i).length;
        }
        return size;
    }

    /**
     * Select strideLength and numberOfSamples, where stride is the number of intervals
     * between two samples and trainSamples is training samples to fetch. If we disable
     * interpolation, strideLength is 1 and numberOfSamples is shingleSize + numMinSamples;
     *
     * Algorithm:
     *
     * delta is the length of the detector interval in minutes.
     *
     * 1. Suppose delta ≤ 30 and divides 60. Then set numberOfSamples = ceil ( (shingleSize + 32)/ 24 )*24
     * and strideLength = 60/delta. Note that if there is enough data — we may have lot more than shingleSize+32
     * points — which is only good. This step tries to match data with hourly pattern.
     * 2. otherwise, set numberOfSamples = (shingleSize + 32) and strideLength = 1.
     * This should be an uncommon case as we are assuming most users think in terms of multiple of 5 minutes
     *(say 10 or 30 minutes). But if someone wants a 23 minutes interval —- and the system permits --
     * we give it to them. In this case, we disable interpolation as we want to interpolate based on the hourly pattern.
     * That's why we use 60 as a dividend in case 1. The 23 minute case does not fit that pattern.
     * Note the smallest delta that does not divide 60 is 7 which is quite large to wait for one data point.
     * @return the chosen strideLength and numberOfSamples
     */
    private Pair<Integer, Integer> selectRangeParam(AnomalyDetector detector) {
        int shingleSize = detector.getShingleSize();
        if (EnabledSetting.isInterpolationInColdStartEnabled()) {
            long delta = detector.getDetectorIntervalInMinutes();

            int strideLength = defaulStrideLength;
            int numberOfSamples = defaultNumberOfSamples;
            if (delta <= 30 && 60 % delta == 0) {
                strideLength = (int) (60 / delta);
                numberOfSamples = (int) Math.ceil((shingleSize + numMinSamples) / 24.0d) * 24;
            } else {
                strideLength = 1;
                numberOfSamples = shingleSize + numMinSamples;
            }
            return Pair.of(strideLength, numberOfSamples);
        } else {
            return Pair.of(1, shingleSize + numMinSamples);
        }

    }

    /**
     * Get train samples within a time range.
     *
     * @param detector accessor to detector config
     * @param startMilli range start
     * @param endMilli range end
     * @param stride the number of intervals between two samples
     * @param numberOfSamples maximum training samples to fetch
     * @return list of sample time ranges
     */
    private List<Entry<Long, Long>> getTrainSampleRanges(
        AnomalyDetector detector,
        long startMilli,
        long endMilli,
        int stride,
        int numberOfSamples
    ) {
        long bucketSize = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMillis();
        int numBuckets = (int) Math.floor((endMilli - startMilli) / (double) bucketSize);
        // adjust if numStrides is more than the max samples
        int numStrides = Math.min((int) Math.floor(numBuckets / (double) stride), numberOfSamples);
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
                    trainModelFromDataSegments(samples, entity, modelState, detector.getShingleSize());
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
                trainModelFromDataSegments(samples, model.getEntity().orElse(null), modelState, shingleSize);
            } catch (Exception e) {
                // e.g., exception from rcf. We can do nothing except logging the error
                // We won't retry training for the same entity in the cooldown period
                // (60 detector intervals).
                logger.error("Unexpected training error", e);
            }

        }
    }

    /**
     * Extract training data and put them into ModelState
     *
     * @param coldstartDatapoints training data generated from cold start
     * @param modelId model Id
     * @param modelState entity State
     */
    private void extractTrainSamples(List<double[][]> coldstartDatapoints, String modelId, ModelState<EntityModel> modelState) {
        if (coldstartDatapoints == null || coldstartDatapoints.size() == 0 || modelState == null) {
            return;
        }

        EntityModel model = modelState.getModel();
        if (model == null) {
            model = new EntityModel(null, new ArrayDeque<>(), null);
            modelState.setModel(model);
        }

        Queue<double[]> newSamples = new ArrayDeque<>();
        for (double[][] consecutivePoints : coldstartDatapoints) {
            for (int i = 0; i < consecutivePoints.length; i++) {
                newSamples.add(consecutivePoints[i]);
            }
        }

        model.setSamples(newSamples);
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

    @Override
    public void clear(String detectorId) {
        doorKeepers.remove(detectorId);
    }
}
