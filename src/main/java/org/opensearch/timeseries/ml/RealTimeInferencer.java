/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ml;

import java.time.Clock;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.MaintenanceState;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.SaveResultStrategy;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.util.ExpiringValue;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Since we assume model state's last access time is current time and compare it with incoming data's execution time,
 * this class is only meant to be used by real time analysis.
 */
public abstract class RealTimeInferencer<RCFModelType extends ThresholdedRandomCutForest, ResultType extends IndexableResult, RCFResultType extends IntermediateResult<ResultType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriterType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType>, ModelManagerType extends ModelManager<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType>, SaveResultStrategyType extends SaveResultStrategy<ResultType, RCFResultType>, CacheType extends TimeSeriesCache<RCFModelType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType, CacheType, ResultType, RCFResultType, ModelManagerType, SaveResultStrategyType>>
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
    private Map<String, ExpiringValue<PriorityQueue<Sample>>> sampleQueues;
    private Comparator<Sample> sampleComparator;
    private Clock clock;

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
        Clock clock
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
    }

    /**
     *
     * @param sample Sample to process
     * @param modelState model state
     * @param config Config accessor
     * @param taskId task Id for batch analysis
     * @return whether process succeeds or not
     */
    public boolean process(Sample sample, ModelState<RCFModelType> modelState, Config config, String taskId) {
        String modelId = modelState.getModelId();
        ExpiringValue<PriorityQueue<Sample>> expiringSampleQueue = sampleQueues
            .computeIfAbsent(
                modelId,
                k -> new ExpiringValue<>(
                    new PriorityQueue<>(sampleComparator),
                    config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
                    clock
                )
            );
        expiringSampleQueue.getValue().add(sample);
        return processWithTimeout(modelState, config, taskId, sample);
    }

    private boolean processWithTimeout(ModelState<RCFModelType> modelState, Config config, String taskId, Sample sample) {
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
        if (lock.tryLock()) {
            try {
                PriorityQueue<Sample> queue = sampleQueues.get(modelId).getValue();
                while (!queue.isEmpty()) {
                    Sample curSample = queue.poll();
                    long windowDelayMillis = config.getWindowDelay() == null
                        ? 0
                        : ((IntervalTimeConfiguration) config.getWindowDelay()).toDuration().toMillis();
                    long curExecutionEnd = curSample.getDataEndTime().toEpochMilli() + windowDelayMillis;

                    success = tryProcess(curSample, modelState, config, taskId, curExecutionEnd);
                }
            } finally {
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
            } else {
                // Schedule a retry in one second
                threadPool
                    .schedule(
                        () -> processWithTimeout(modelState, config, taskId, sample),
                        new TimeValue(1, TimeUnit.SECONDS),
                        threadPoolName
                    );
            }

            return false;
        }
        return success;
    }

    private boolean tryProcess(Sample sample, ModelState<RCFModelType> modelState, Config config, String taskId, long curExecutionEnd) {
        String modelId = modelState.getModelId();
        try {
            RCFResultType result = modelManager.getResult(sample, modelState, modelId, config, taskId);
            resultWriteWorker
                .saveResult(
                    result,
                    config,
                    sample.getDataStartTime(),
                    sample.getDataEndTime(),
                    modelId,
                    sample.getValueList(),
                    modelState.getEntity(),
                    taskId
                );
        } catch (IllegalArgumentException e) {
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
            return false;
        } catch (Exception e) {
            // e.g., null pointer exception when there is a bug in RCF
            reColdStart(config, modelId, e, sample, taskId);
        }
        return true;
    }

    private void reColdStart(Config config, String modelId, Exception e, Sample sample, String taskId) {
        // fail to score likely due to model corruption. Re-cold start to recover.
        LOG.error(new ParameterizedMessage("Likely model corruption for [{}]", modelId), e);
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
                    clock.millis() + config.getIntervalInMilliseconds(),
                    config.getId(),
                    RequestPriority.MEDIUM,
                    modelId,
                    sample.getValueList(),
                    sample.getDataStartTime().toEpochMilli(),
                    taskId
                )
            );
    }

    @Override
    public void maintenance() {
        try {
            sampleQueues.entrySet().removeIf(entry -> entry.getValue().isExpired());
            modelLocks.entrySet().removeIf(entry -> entry.getValue().isExpired());
        } catch (Exception e) {
            // will be thrown to transport broadcast handler
            throw new TimeSeriesException("Fail to maintain RealTimeInferencer", e);
        }

    }
}
