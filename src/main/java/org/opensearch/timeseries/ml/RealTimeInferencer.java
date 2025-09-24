/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ml;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.MaintenanceState;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.SaveResultStrategy;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ExpiringValue;
import org.opensearch.timeseries.util.ModelUtil;

import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Since we assume model state's last access time is current time and compare it with incoming data's execution time,
 * this class is only meant to be used by real time analysis.
 */
public abstract class RealTimeInferencer<RCFModelType extends ThresholdedRandomCutForest, ResultType extends IndexableResult, RCFResultType extends IntermediateResult<ResultType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriterType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, ResultType>, ModelManagerType extends ModelManager<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, ColdStarterType>, SaveResultStrategyType extends SaveResultStrategy<ResultType, RCFResultType>, CacheType extends TimeSeriesCache<RCFModelType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType, CacheType, ResultType, RCFResultType, ModelManagerType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType>>
    implements
        MaintenanceState {

    private static final Logger LOG = LogManager.getLogger(RealTimeInferencer.class);
    protected ModelManagerType modelManager;
    protected Stats stats;
    private String modelCorruptionStat;
    protected CheckpointDaoType checkpointDao;
    protected ColdStartWorkerType coldStartWorker;
    protected SaveResultStrategyType resultWriteWorker;
    private CacheProvider<RCFModelType, CacheType> cache;
    // ensure no two threads can score samples at the same time which can happen in tests
    // where we send a lot of requests in a fast pace and the run API returns immediately
    // without waiting for the requests get finished processing. It can also happen in
    // production as the impute request and actual data scoring in the next interval
    // can happen at the same time.
    private Map<String, ExpiringValue<Lock>> modelLocks;
    private ThreadPool threadPool;
    private String threadPoolName;
    // ensure we process samples in the ascending order of time in case race conditions.
    private Map<String, ExpiringValue<TreeSet<Sample>>> sampleQueues;
    private Comparator<Sample> sampleComparator;
    private Clock clock;
    private SearchFeatureDao searchFeatureDao;
    private AnalysisType analysisContext;

    public RealTimeInferencer(
        ModelManagerType modelManager,
        Stats stats,
        String modelCorruptionStat,
        CheckpointDaoType checkpointDao,
        ColdStartWorkerType coldStartWorker,
        SaveResultStrategyType resultWriteWorker,
        CacheProvider<RCFModelType, CacheType> cache,
        ThreadPool threadPool,
        String threadPoolName,
        Clock clock,
        SearchFeatureDao searchFeatureDao,
        AnalysisType analysisContext
    ) {
        this.modelManager = modelManager;
        this.stats = stats;
        this.modelCorruptionStat = modelCorruptionStat;
        this.checkpointDao = checkpointDao;
        this.coldStartWorker = coldStartWorker;
        this.resultWriteWorker = resultWriteWorker;
        this.cache = cache;
        this.threadPool = threadPool;
        this.threadPoolName = threadPoolName;
        this.modelLocks = new ConcurrentHashMap<>();
        this.sampleQueues = new ConcurrentHashMap<>();
        this.sampleComparator = Comparator.comparing(Sample::getDataEndTime);
        this.clock = clock;
        this.searchFeatureDao = searchFeatureDao;
        this.analysisContext = analysisContext;
    }

    /**
     *
     * @param sample Sample to process
     * @param modelState model state
     * @param config Config accessor
     * @param taskId task Id for batch analysis
     */
    public void process(
        Sample sample,
        ModelState<RCFModelType> modelState,
        Config config,
        String taskId,
        ActionListener<Boolean> listener
    ) {
        String modelId = modelState.getModelId();
        ExpiringValue<TreeSet<Sample>> expiringSampleQueue = sampleQueues
            .computeIfAbsent(
                modelId,
                k -> new ExpiringValue<>(
                    new TreeSet<>(sampleComparator),
                    config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
                    clock
                )
            );
        TreeSet<Sample> queue = expiringSampleQueue.getValue();
        // model state might have samples that are not processed yet
        addSamples(queue, modelState.getSamples(), config);
        // record the last unprocessed historical sample's data end time
        // this is used to calculate the time gap between last input timestamp and current sample's data end time
        Instant lastSampleDataEndTime = queue.isEmpty() ? Instant.MIN : queue.last().getDataEndTime();
        // add current sample to queue
        addSample(queue, sample, config);
        Optional<RCFModelType> modelOptional = modelState.getModel();
        if (modelOptional.isPresent()) {
            // we need to use the latest sample in the queue to calculate the time gap because last scored RCF sample might not be the
            // latest sample in the queue
            long lastInputTimestampSecs = Math
                .max(ModelUtil.getLastInputTimestampSeconds(modelOptional.get()), lastSampleDataEndTime.getEpochSecond());
            // Current sample is already retrieved. We need to figure out how many data points before current sample.
            // We send data end time in seconds to rcf, so we need to find the gap between last input timestamp and current sample's data.
            long currentTimeSecs = sample.getDataEndTime().getEpochSecond();
            long diffSecs = currentTimeSecs - lastInputTimestampSecs;
            LOG
                .debug(
                    "diffSecs:{} interval:{} maxFrequencyMultiple:{} lastInputTimestampSecs:{} currentTimeSecs:{}",
                    diffSecs,
                    config.getIntervalInSeconds(),
                    TimeSeriesSettings.MAX_FREQUENCY_MULTIPLE,
                    lastInputTimestampSecs,
                    currentTimeSecs
                );
            // it is expected that the time gap is at least 1 interval. So 2 intervals is the minimum gap to fetch data.
            long minGapSecs = 2 * config.getIntervalInSeconds();
            if (diffSecs >= minGapSecs && diffSecs / config.getIntervalInSeconds() <= TimeSeriesSettings.MAX_FREQUENCY_MULTIPLE) {
                LOG.info("fetching features between {} and {}", lastInputTimestampSecs, currentTimeSecs);
                // get features for the interval since last input timestamp and current sample's data start time
                // getFeatures uses milliseconds as unit, so we need to convert seconds to milliseconds
                // We avoid querying the same features twice. Each query begins at the
                // latest existing sample, ensuring we always process it at least once.
                // Future queries start from that sample onward, never looking back
                // before it.
                getFeatures(
                    config,
                    modelState.getEntity(),
                    lastInputTimestampSecs * 1000,
                    sample.getDataStartTime().getEpochSecond() * 1000,
                    ActionListener.wrap(samples -> {
                        LOG.info("samples size: {}", samples.size());
                        for (Sample s : samples) {
                            addSample(queue, s, config);
                        }
                        processWithTimeout(modelState, config, taskId, sample, listener);
                    }, listener::onFailure)
                );
            } else if (diffSecs < minGapSecs) {
                processWithTimeout(modelState, config, taskId, sample, listener);
            } else {
                LOG
                    .warn(
                        "Time gap {} is too large for config [{}], model [{}]. " + "Triggering cold start.",
                        diffSecs,
                        config.getId(),
                        modelId
                    );
                reColdStart(config, modelId, null, sample, taskId);
                listener.onResponse(false);
            }
        } else {
            // model not present, cannot process.
            LOG.warn("Model not present for config [{}], model [{}]. Skipping.", config.getId(), modelId);
            listener.onResponse(false);
        }
    }

    public void processWithTimeout(
        ModelState<RCFModelType> modelState,
        Config config,
        String taskId,
        Sample sample,
        ActionListener<Boolean> listener
    ) {
        String modelId = modelState.getModelId();
        ReentrantLock lock = (ReentrantLock) modelLocks
            .computeIfAbsent(
                modelId,
                k -> new ExpiringValue<>(
                    new ReentrantLock(),
                    config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
                    clock
                )
            )
            .getValue();

        boolean success = false;
        LOG.debug("try lock");
        if (lock.tryLock()) {
            LOG.debug("lock acquired");
            try {
                TreeSet<Sample> queue = sampleQueues.get(modelId).getValue();
                LOG.debug("queue size:{}", queue.size());
                if (!queue.isEmpty()) {
                    List<Sample> samples = new ArrayList<>(queue);
                    queue.clear();

                    double[][] points = new double[samples.size()][];
                    long[] timestamps = new long[samples.size()];
                    List<Instant> dataStarts = new ArrayList<>();
                    List<Instant> dataEnds = new ArrayList<>();
                    for (int i = 0; i < samples.size(); i++) {
                        points[i] = samples.get(i).getValueList();
                        Instant dataStart = samples.get(i).getDataStartTime();
                        dataStarts.add(dataStart);
                        Instant dataEnd = samples.get(i).getDataEndTime();
                        dataEnds.add(dataEnd);
                        timestamps[i] = dataEnd.getEpochSecond();
                    }

                    RCFModelType model = modelState.getModel().get();
                    LOG
                        .debug(
                            "Processing sequential points - timestamps: {}, entity: {}",
                            Arrays.toString(timestamps),
                            modelState.getEntity().map(Object::toString).orElse("null")
                        );
                    List<AnomalyDescriptor> results = model.processSequentially(points, timestamps, x -> true);
                    List<RCFResultType> intermediateResults = new ArrayList<>();
                    for (int i = 0; i < results.size(); i++) {
                        Sample sampleI = samples.get(i);
                        AnomalyDescriptor result = results.get(i);
                        RCFResultType rcfResult = modelManager
                            .toResult(model.getForest(), result, sampleI.getValueList(), result.getMissingValues() != null, config);
                        intermediateResults.add(rcfResult);
                    }
                    resultWriteWorker
                        .saveAllResults(
                            intermediateResults,
                            config,
                            dataStarts,
                            dataEnds,
                            modelId,
                            Arrays.asList(points),
                            modelState.getEntity(),
                            taskId
                        );
                    success = true;
                }
                listener.onResponse(success);
            } catch (Exception e) {
                LOG.error("Error processing samples", e);
                if (e.getMessage() != null && e.getMessage().contains("incorrect ordering of time")) {
                    // ignore current timestamp.
                    LOG
                        .warn(
                            String
                                .format(
                                    Locale.ROOT,
                                    "incorrect ordering of time for config %s model %s at data end time %d",
                                    config.getId(),
                                    modelState.getModelId(),
                                    sample.getDataEndTime().toEpochMilli()
                                )
                        );
                } else {
                    reColdStart(config, modelId, e, sample, taskId);
                }
                listener.onFailure(e);
            } finally {
                LOG.debug("unlock");
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        } else {
            long windowDelayMillis = config.getWindowDelay() == null
                ? 0
                : ((IntervalTimeConfiguration) config.getWindowDelay()).toDuration().toMillis();
            long curExecutionEnd = sample.getDataEndTime().toEpochMilli() + windowDelayMillis;
            long nextExecutionEnd = curExecutionEnd + config.getIntervalInMilliseconds();
            // schedule a retry if not already time out
            if (clock.millis() >= nextExecutionEnd) {
                LOG.warn("Timeout reached, not retrying.");
                listener.onResponse(false);
            } else {
                try {
                    LOG
                        .debug(
                            "Scheduling a retry in one second for model [{}], config [{}], taskId [{}], sample data end time [{}], next execution end time [{}], current time [{}], window delay [{}], interval in milliseconds [{}]",
                            modelId,
                            config.getId(),
                            taskId,
                            sample.getDataEndTime().toEpochMilli(),
                            nextExecutionEnd,
                            clock.millis(),
                            windowDelayMillis,
                            config.getIntervalInMilliseconds()
                        );
                    // Schedule a retry in one second
                    threadPool
                        .schedule(
                            () -> processWithTimeout(modelState, config, taskId, sample, listener),
                            new TimeValue(1, TimeUnit.SECONDS),
                            threadPoolName
                        );
                    // no call to listener as we scheduled a retry and listener will be called when the retry is successful
                } catch (Exception e) {
                    LOG.error("Failed to schedule retry", e);
                    listener.onFailure(e);
                }
            }
        }
    }

    public void reColdStart(Config config, String modelId, Exception e, Sample sample, String taskId) {
        // fail to score likely due to model corruption. Re-cold start to recover.
        if (e != null) {
            LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", modelId), e);
        } else {
            LOG.warn(new ParameterizedMessage("Likely model corruption for [{}]", modelId));
        }
        stats.getStat(modelCorruptionStat).increment();
        cache.get().removeModel(config.getId(), modelId);
        if (null != modelId) {
            checkpointDao
                .deleteModelCheckpoint(
                    modelId,
                    ActionListener
                        .wrap(
                            r -> LOG.debug(new ParameterizedMessage("Succeeded in deleting checkpoint [{}].", modelId)),
                            ex -> LOG.error(new ParameterizedMessage("Failed to delete checkpoint [{}].", modelId), ex)
                        )
                );
        }

        coldStartWorker
            .put(
                new FeatureRequest(
                    clock.millis() + config.getFrequencyInMilliseconds(),
                    config.getId(),
                    RequestPriority.MEDIUM,
                    modelId,
                    sample.getValueList(),
                    sample.getDataStartTime().toEpochMilli(),
                    taskId
                )
            );
    }

    /**
     * Simplified version of getFeatures without round and lastRounddataSample parameters.
     * This method fetches features for a single time period without recursive calling logic.
     *
     * @param config Config accessor
     * @param entity Optional entity for which to fetch samples
     * @param startTimeMs Start time in milliseconds
     * @param endTimeMs End time in milliseconds
     * @param listener ActionListener to return available samples
     */
    private void getFeatures(
        Config config,
        Optional<Entity> entity,
        long startTimeMs,
        long endTimeMs,
        ActionListener<List<Sample>> listener
    ) {
        if (startTimeMs == 0 || startTimeMs >= endTimeMs || endTimeMs - startTimeMs < config.getIntervalInMilliseconds()) {
            listener.onResponse(new ArrayList<>());
            return;
        }

        int numberOfSamples = (int) Math.floor((endTimeMs - startTimeMs) / (double) config.getIntervalInMilliseconds());

        if (numberOfSamples > TimeSeriesSettings.MAX_FREQUENCY_MULTIPLE) {
            listener.onResponse(new ArrayList<>());
            return;
        }

        // Create ranges in ascending order where the last sample's end time is the given endTimeMs.
        // Sample ranges are also in ascending order in OpenSearch's response.
        List<Entry<Long, Long>> sampleRanges = searchFeatureDao
            .getTrainSampleRanges((IntervalTimeConfiguration) config.getInterval(), startTimeMs, endTimeMs, numberOfSamples);

        if (sampleRanges.isEmpty()) {
            listener.onResponse(new ArrayList<>());
            return;
        }

        ActionListener<List<Optional<double[]>>> getFeatureListener = ActionListener.wrap(featureSamples -> {
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
                Entry<Long, Long> curRange = sampleRanges.get(index);
                if (featuresOptional.isPresent()) {
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

            listener.onResponse(samples);
        }, listener::onFailure);

        try {
            searchFeatureDao
                .getColdStartSamplesForPeriods(
                    config,
                    sampleRanges,
                    entity,
                    // Accept empty bucket.
                    // 0, as returned by the engine should constitute a valid answer, "null" is a missing answer — it may be that 0
                    // is meaningless in some case, but 0 is also meaningful in other cases. It may be that the query defining the
                    // metric is ill-formed, but that cannot be solved by cold-start strategy of the AD plugin — if we attempt to do
                    // that, we will have issues with legitimate interpretations of 0.
                    true,
                    analysisContext,
                    new ThreadedActionListener<>(LOG, threadPool, threadPoolName, getFeatureListener, false)
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Adds a sample to the queue only if it's at least one detector interval apart from its potential neighbors (previous and next).
     * This is to avoid an IllegalArgumentException from the RCF model due to out-of-order processing of timestamps.
     * @param queue The queue to add the sample to.
     * @param sample The sample to be added.
     * @param config The detector configuration.
     */
    private void addSample(TreeSet<Sample> queue, Sample sample, Config config) {
        long intervalSeconds = config.getIntervalInSeconds();
        long sampleTime = sample.getDataStartTime().getEpochSecond();

        Sample previousSample = queue.floor(sample);
        Sample nextSample = queue.ceiling(sample);

        LOG
            .debug(
                "lastSample: {} sample: {}",
                previousSample == null ? "null" : previousSample.getDataStartTime().getEpochSecond(),
                sample.getDataStartTime().getEpochSecond()
            );

        boolean previousGapOk = (previousSample == null)
            || (sampleTime - previousSample.getDataStartTime().getEpochSecond() >= intervalSeconds);

        boolean nextGapOk = (nextSample == null) || (nextSample.getDataStartTime().getEpochSecond() - sampleTime >= intervalSeconds);

        if (previousGapOk && nextGapOk) {
            queue.add(sample);
        }
    }

    private void addSamples(TreeSet<Sample> queue, Deque<Sample> samples, Config config) {
        if (samples != null) {
            for (Sample sample : samples) {
                addSample(queue, sample, config);
            }
        }
    }

    @Override
    public void maintenance() {
        try {
            // clean up expired items
            modelLocks.entrySet().removeIf(entry -> entry.getValue().isExpired());
            sampleQueues.entrySet().removeIf(entry -> entry.getValue().isExpired());
        } catch (Exception e) {
            // will be thrown to transport broadcast handler
            throw new TimeSeriesException("Fail to maintain RealTimeInferencer", e);
        }
    }

    public Map<String, ExpiringValue<Lock>> getModelLocks() {
        return modelLocks;
    }

    public Map<String, ExpiringValue<TreeSet<Sample>>> getSampleQueues() {
        return sampleQueues;
    }
}
