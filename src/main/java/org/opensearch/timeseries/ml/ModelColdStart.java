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

package org.opensearch.timeseries.ml;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.CleanState;
import org.opensearch.timeseries.MaintenanceState;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.caching.DoorKeeper;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ExceptionUtil;

import com.amazon.randomcutforest.config.ImputationMethod;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * The class bootstraps a model by performing a cold start
 */
public abstract class ModelColdStart<RCFModelType extends ThresholdedRandomCutForest, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, IndexableResultType extends IndexableResult>
    implements
        MaintenanceState,
        CleanState {
    private static final Logger logger = LogManager.getLogger(ModelColdStart.class);

    private final Duration modelTtl;

    // A bloom filter checked before cold start to ensure we don't repeatedly
    // retry cold start of the same model.
    // keys are detector ids.
    protected Map<String, DoorKeeper> doorKeepers;
    protected Instant lastThrottledColdStartTime;
    protected int coolDownMinutes;
    protected final Clock clock;
    protected final ThreadPool threadPool;
    protected final int numMinSamples;
    protected CheckpointWriteWorkerType checkpointWriteWorker;
    // make sure rcf use a specific random seed. Otherwise, we will use a random random (not a typo) seed.
    // this is mainly used for testing to make sure the model we trained and the reference rcf produce
    // the same results
    protected final long rcfSeed;
    protected final int numberOfTrees;
    protected final int rcfSampleSize;
    protected final double thresholdMinPvalue;
    protected final double initialAcceptFraction;
    protected final NodeStateManager nodeStateManager;
    protected final int defaulStrideLength;
    protected final int defaultNumberOfSamples;
    protected final SearchFeatureDao searchFeatureDao;
    protected final FeatureManager featureManager;
    protected final int maxRoundofColdStart;
    protected final String threadPoolName;
    protected final AnalysisType context;
    protected final int resultMappingVersion;

    public ModelColdStart(
        Duration modelTtl,
        int coolDownMinutes,
        Clock clock,
        ThreadPool threadPool,
        int numMinSamples,
        CheckpointWriteWorkerType checkpointWriteWorker,
        long rcfSeed,
        int numberOfTrees,
        int rcfSampleSize,
        double thresholdMinPvalue,
        NodeStateManager nodeStateManager,
        int defaultSampleStride,
        int defaultTrainSamples,
        SearchFeatureDao searchFeatureDao,
        FeatureManager featureManager,
        int maxRoundofColdStart,
        String threadPoolName,
        AnalysisType context,
        int resultMappingVersion
    ) {
        this.modelTtl = modelTtl;
        this.coolDownMinutes = coolDownMinutes;
        this.clock = clock;
        this.threadPool = threadPool;
        this.numMinSamples = numMinSamples;
        this.checkpointWriteWorker = checkpointWriteWorker;
        this.rcfSeed = rcfSeed;
        this.numberOfTrees = numberOfTrees;
        this.rcfSampleSize = rcfSampleSize;
        this.thresholdMinPvalue = thresholdMinPvalue;

        this.doorKeepers = new ConcurrentHashMap<>();
        this.lastThrottledColdStartTime = Instant.MIN;
        this.initialAcceptFraction = numMinSamples * 1.0d / rcfSampleSize;

        this.nodeStateManager = nodeStateManager;
        this.defaulStrideLength = defaultSampleStride;
        this.defaultNumberOfSamples = defaultTrainSamples;
        this.searchFeatureDao = searchFeatureDao;
        this.featureManager = featureManager;
        this.maxRoundofColdStart = maxRoundofColdStart;
        this.threadPoolName = threadPoolName;
        this.context = context;
        this.resultMappingVersion = resultMappingVersion;
    }

    @Override
    public void maintenance() {
        doorKeepers.entrySet().stream().forEach(doorKeeperEntry -> {
            String id = doorKeeperEntry.getKey();
            DoorKeeper doorKeeper = doorKeeperEntry.getValue();
            if (doorKeeper.expired(modelTtl)) {
                doorKeepers.remove(id);
            } else {
                doorKeeper.maintenance();
            }
        });
    }

    @Override
    public void clear(String id) {
        doorKeepers.remove(id);
    }

    /**
     * Train models
     * @param coldStartRequest cold start request
     * @param configId Config Id
     * @param modelState Model state
     * @param listener callback before the method returns whenever ColdStarter
     * finishes training or encounters exceptions.  The listener helps notify the
     * cold start queue to pull another request (if any) to execute. We save the
     * training data in result index so that the frontend can plot it.
     */
    public void trainModel(
        FeatureRequest coldStartRequest,
        String configId,
        ModelState<RCFModelType> modelState,
        ActionListener<List<IndexableResultType>> listener
    ) {
        // run once does not need to cache
        nodeStateManager.getConfig(configId, context, !coldStartRequest.isRunOnce(), ActionListener.wrap(configOptional -> {
            if (false == configOptional.isPresent()) {
                logger.warn(new ParameterizedMessage("Config [{}] is not available.", configId));
                listener.onFailure(new TimeSeriesException(configId, "fail to find config"));
                return;
            }

            Config config = configOptional.get();

            String modelId = modelState.getModelId();

            if (modelState.getSamples().size() < this.numMinSamples) {
                coldStart(modelId, coldStartRequest, modelState, config, listener);
            } else {
                try {
                    trainModelFromExistingSamples(modelState, config, coldStartRequest.getTaskId());
                    listener.onResponse(null);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }, listener::onFailure));
    }

    public void trainModelFromExistingSamples(ModelState<RCFModelType> modelState, Config config, String taskId) {
        if (modelState.getSamples().size() >= this.numMinSamples) {
            Deque<Sample> samples = modelState.getSamples();
            trainModelFromDataSegments(new ArrayList<>(samples), modelState, config, taskId);
            // clear after use
            modelState.clearSamples();
        }
    }

    /**
     * Training model
     * @param modelId model Id corresponding to the entity
     * @param coldStartRequest cold start request
     * @param modelState model state
     * @param config config accessor
     * @param listener call back to send processed training data and last sample in the training data
     */
    private void coldStart(
        String modelId,
        FeatureRequest coldStartRequest,
        ModelState<RCFModelType> modelState,
        Config config,
        ActionListener<List<IndexableResultType>> listener
    ) {
        logger.debug("Trigger cold start for {}", modelId);

        if (modelState == null) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT, "Cannot have empty model state")));
            return;
        }

        if (lastThrottledColdStartTime.plus(Duration.ofMinutes(coolDownMinutes)).isAfter(clock.instant())) {
            logger.info("Still in cool down.");
            listener.onResponse(null);
            return;
        }

        String configId = config.getId();
        boolean earlyExit = true;
        try {
            if (null == coldStartRequest.getTaskId()) {
                // Won't retry real-time cold start within 60 intervals for an entity
                // coldStartRequest.getTaskId() == null in real-time cold start

                DoorKeeper doorKeeper = doorKeepers.computeIfAbsent(configId, id -> {
                    // reset every 60 intervals
                    return new DoorKeeper(
                        TimeSeriesSettings.DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION,
                        config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ),
                        clock,
                        TimeSeriesSettings.COLD_START_DOOR_KEEPER_COUNT_THRESHOLD
                    );
                });

                // only use door keeper when this is for real time
                if (doorKeeper.appearsMoreThanOrEqualToThreshold(modelId)) {
                    logger
                        .info(
                            "Won't retry real-time cold start within {} intervals for model {}",
                            TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ,
                            modelId
                        );
                    return;
                }

                doorKeeper.put(modelId);
            }

            ActionListener<List<Sample>> coldStartCallBack = ActionListener.wrap(trainingData -> {
                // existing samples might have different interval or duplicated data compared to training data we just grabbed.
                // clear it before adding historical data.
                modelState.clearSamples();
                if (trainingData != null && !trainingData.isEmpty()) {
                    int dataSize = trainingData.size();
                    // only train models if we have enough samples
                    if (dataSize >= numMinSamples) {
                        // The function trainModelFromDataSegments will save a trained a model. trainModelFromDataSegments is called by
                        // multiple places, so I want to make the saving model implicit just in case I forgot.
                        List<IndexableResultType> processedTrainingData = trainModelFromDataSegments(
                            trainingData,
                            modelState,
                            config,
                            coldStartRequest.getTaskId()
                        );
                        logger.info("Succeeded in training entity: {}", modelId);
                        listener.onResponse(processedTrainingData);
                    } else {
                        logger.info("Not enough data to train model: {}, currently we have {}", modelId, dataSize);

                        trainingData.forEach(modelState::addSample);
                        // save to checkpoint for real time only
                        if (null == coldStartRequest.getTaskId()) {
                            checkpointWriteWorker.write(modelState, true, RequestPriority.MEDIUM);
                        }

                        listener.onResponse(null);
                    }
                } else {
                    logger.info("Cannot get training data for {}", modelId);
                    listener.onResponse(null);
                }
            }, exception -> {
                try {
                    logger.error(new ParameterizedMessage("Error while cold start {}", modelId), exception);
                    Throwable cause = Throwables.getRootCause(exception);
                    if (ExceptionUtil.isOverloaded(cause)) {
                        logger.error("too many requests");
                        lastThrottledColdStartTime = Instant.now();
                    } else if (exception instanceof TimeSeriesException) {
                        // e.g., cannot find anomaly detector
                        nodeStateManager
                            .setException(
                                configId,
                                ((TimeSeriesException) exception).cloneWithMsgPrefix(CommonMessages.COLD_START_EXCEPTION)
                            );
                    } else if (cause instanceof TimeSeriesException) {
                        nodeStateManager
                            .setException(configId, ((TimeSeriesException) cause).cloneWithMsgPrefix(CommonMessages.COLD_START_EXCEPTION));
                    } else {
                        nodeStateManager
                            .setException(configId, new TimeSeriesException(configId, CommonMessages.COLD_START_EXCEPTION, cause));
                    }
                    listener.onFailure(exception);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });

            threadPool
                .executor(threadPoolName)
                .execute(
                    () -> getColdStartData(
                        configId,
                        coldStartRequest,
                        new ThreadedActionListener<>(logger, threadPool, threadPoolName, coldStartCallBack, false)
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
     * Get training data for an entity.
     *
     * We first note the maximum and minimum timestamp, and sample at most 24 points
     * (with 60 points apart between two neighboring samples) between those minimum
     * and maximum timestamps.  Samples can be missing.  We only interpolate points
     * between present neighboring samples. We then transform samples and interpolate
     * points to shingles. Finally, full shingles will be used for cold start.
     *
     * @param configId config Id
     * @param coldStartRequest cold start request
     * @param listener A callback listener for receiving training data.
     */
    private void getColdStartData(String configId, FeatureRequest coldStartRequest, ActionListener<List<Sample>> listener) {
        ActionListener<Optional<? extends Config>> getDetectorListener = ActionListener.wrap(configOp -> {
            if (!configOp.isPresent()) {
                listener.onFailure(new EndRunException(configId, "Config is not available.", false));
                return;
            }
            Config config = configOp.get();

            ActionListener<Optional<Long>> minTimeListener = ActionListener.wrap(earliest -> {
                if (earliest.isPresent()) {
                    long startTimeMs = earliest.get().longValue();

                    // End time uses milliseconds as start time is assumed to be in milliseconds.
                    // Opensearch uses a set of preconfigured formats to recognize and parse these
                    // strings into a long value
                    // representing milliseconds-since-the-epoch in UTC.
                    // More on https://tinyurl.com/wub4fk92
                    // also, since we want to use current feature to score, we don't use current interval
                    // [current start, current end] for training. So we fetch training data ending at current start
                    long endTimeMs = coldStartRequest.getDataStartTimeMillis();
                    int numberOfSamples = selectNumberOfSamples(config);
                    // we start with round 0
                    getFeatures(
                        listener,
                        0,
                        new ArrayList<>(),
                        config,
                        coldStartRequest.getEntity(),
                        numberOfSamples,
                        startTimeMs,
                        endTimeMs
                    );
                } else {
                    listener.onResponse(new ArrayList<>());
                }
            }, listener::onFailure);

            searchFeatureDao
                .getMinDataTime(
                    config,
                    coldStartRequest.getEntity(),
                    context,
                    new ThreadedActionListener<>(logger, threadPool, threadPoolName, minTimeListener, false)
                );

        }, listener::onFailure);

        nodeStateManager
            .getConfig(
                configId,
                context,
                // not run once means it is real time and we want to cache
                !coldStartRequest.isRunOnce(),
                new ThreadedActionListener<>(logger, threadPool, threadPoolName, getDetectorListener, false)
            );
    }

    /**
     * Get the number of training samples to fetch from history.
     * We require at least numMinSamples to let rcf output non-zero rcf scores.
     *
     * @return number of training samples
     */
    private int selectNumberOfSamples(Config config) {
        return Math.max(numMinSamples, config.getHistoryIntervals());
    }

    private void getFeatures(
        ActionListener<List<Sample>> listener,
        int round,
        List<Sample> lastRounddataSample,
        Config config,
        Optional<Entity> entity,
        int numberOfSamples,
        long startTimeMs,
        long endTimeMs
    ) {
        if (startTimeMs >= endTimeMs || endTimeMs - startTimeMs < config.getIntervalInMilliseconds()) {
            listener.onResponse(lastRounddataSample);
            return;
        }

        // Create ranges in ascending where the last sample's end time is the given endTimeMs.
        // Sample ranges are also in ascending order in Opensearch's response.
        List<Entry<Long, Long>> sampleRanges = searchFeatureDao
            .getTrainSampleRanges((IntervalTimeConfiguration) config.getInterval(), startTimeMs, endTimeMs, numberOfSamples);

        if (sampleRanges.isEmpty()) {
            listener.onResponse(lastRounddataSample);
            return;
        }
        ActionListener<List<Optional<double[]>>> getFeaturelistener = ActionListener.wrap(featureSamples -> {

            int totalNumSamples = featureSamples.size();

            if (totalNumSamples != sampleRanges.size()) {
                String err = String
                    .format(
                        Locale.ROOT,
                        "length mismatch: totalNumSamples %d != time range length %d",
                        totalNumSamples,
                        sampleRanges.size()
                    );
                listener.onFailure(new IllegalArgumentException(err));
                return;
            }

            // featuresSamples are in ascending order of time.

            List<Sample> samples = new ArrayList<>();
            for (int index = 0; index < featureSamples.size(); index++) {
                Optional<double[]> featuresOptional = featureSamples.get(index);
                if (featuresOptional.isPresent()) {
                    Entry<Long, Long> curRange = sampleRanges.get(index);
                    samples
                        .add(
                            new Sample(
                                featuresOptional.get(),
                                Instant.ofEpochMilli(curRange.getKey()),
                                Instant.ofEpochMilli(curRange.getValue())
                            )
                        );
                }
            }

            List<Sample> concatenatedDataSample = null;
            // make sure the following logic making sense via checking lastRoundFirstStartTime > 0
            if (lastRounddataSample != null && lastRounddataSample.size() > 0) {
                concatenatedDataSample = new ArrayList<>();
                // since we move farther in history in current one, last round data should come
                // after current round data to keep time in sequence.
                concatenatedDataSample.addAll(samples);
                concatenatedDataSample.addAll(lastRounddataSample);
            } else {
                concatenatedDataSample = samples;
            }

            // If the first round of probe provides numMinSamples points (note that if S0 is
            // missing or all Si​ for some i > N is missing then we would miss a lot of points.
            // Otherwise we can issue another round of query — if there is any sample in the
            // second round then we would have numMinSamples points. If there is no sample
            // in the second round then we should wait for real data.
            // Note that even though we have imputation built in rcf, it is beneficial to require
            // more samples at least during cold start. Garbage in, garbage out.
            if (concatenatedDataSample.size() >= numMinSamples || round + 1 >= maxRoundofColdStart) {
                listener.onResponse(concatenatedDataSample);
            } else {
                // the earliest sample's start time is the endTimeMs of next round of probe.
                long earliestSampleStartTime = sampleRanges.get(0).getKey();
                getFeatures(
                    listener,
                    round + 1,
                    concatenatedDataSample,
                    config,
                    entity,
                    numberOfSamples,
                    startTimeMs,
                    earliestSampleStartTime
                );
            }
        }, listener::onFailure);

        try {
            searchFeatureDao
                .getColdStartSamplesForPeriods(
                    config,
                    sampleRanges,
                    entity,
                    // Accept empty bucket.
                    // 0, as returned by the engine should constitute a valid answer, “null” is a missing answer — it may be that 0
                    // is meaningless in some case, but 0 is also meaningful in some cases. It may be that the query defining the
                    // metric is ill-formed, but that cannot be solved by cold-start strategy of the AD plugin — if we attempt to do
                    // that, we will have issues with legitimate interpretations of 0.
                    true,
                    context,
                    new ThreadedActionListener<>(logger, threadPool, threadPoolName, getFeaturelistener, false)
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    // Method to apply imputation method based on the imputation option
    public static <T extends ThresholdedRandomCutForest.Builder<T>> T applyImputationMethod(Config config, T builder) {
        ImputationOption imputationOption = config.getImputationOption();
        if (imputationOption == null) {
            // by default using last known value
            return builder.imputationMethod(ImputationMethod.PREVIOUS);
        } else {
            switch (imputationOption.getMethod()) {
                case ZERO:
                    return builder.imputationMethod(ImputationMethod.ZERO);
                case FIXED_VALUES:
                    // we did validate default fill is not empty, size matches enabled feature number in Config's constructor,
                    // and feature names matches existing features
                    List<String> enabledFeatureName = config.getEnabledFeatureNames();
                    double[] fillValues = new double[enabledFeatureName.size()];
                    Map<String, Double> defaultFillMap = imputationOption.getDefaultFill();
                    for (int i = 0; i < enabledFeatureName.size(); i++) {
                        fillValues[i] = defaultFillMap.get(enabledFeatureName.get(i));
                    }
                    return builder.imputationMethod(ImputationMethod.FIXED_VALUES).fillValues(fillValues);
                case PREVIOUS:
                    return builder.imputationMethod(ImputationMethod.PREVIOUS);
                default:
                    // by default using last known value
                    return builder.imputationMethod(ImputationMethod.PREVIOUS);
            }
        }
    }

    protected abstract List<IndexableResultType> trainModelFromDataSegments(
        List<Sample> dataPoints,
        ModelState<RCFModelType> state,
        Config config,
        String taskId
    );
}
