/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ml;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.TimeSeriesCache;
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
import org.opensearch.timeseries.stats.Stats;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Since we assume model state's last access time is current time and compare it with incoming data's execution time,
 * this class is only meant to be used by real time analysis.
 */
public abstract class RealTimeInferencer<RCFModelType extends ThresholdedRandomCutForest, ResultType extends IndexableResult, RCFResultType extends IntermediateResult<ResultType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriterType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ColdStarterType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType>, ModelManagerType extends ModelManager<RCFModelType, ResultType, RCFResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType>, SaveResultStrategyType extends SaveResultStrategy<ResultType, RCFResultType>, CacheType extends TimeSeriesCache<RCFModelType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, ColdStarterType, CacheType, ResultType, RCFResultType, ModelManagerType, SaveResultStrategyType>> {
    private static final Logger LOG = LogManager.getLogger(RealTimeInferencer.class);
    protected ModelManagerType modelManager;
    protected Stats stats;
    private String modelCorruptionStat;
    protected CheckpointDaoType checkpointDao;
    protected ColdStartWorkerType coldStartWorker;
    protected SaveResultStrategyType resultWriteWorker;
    private CacheProvider<RCFModelType, CacheType> cache;
    private Map<String, Lock> modelLocks = Collections.synchronizedMap(new WeakHashMap<>());
    private ThreadPool threadPool;
    private String threadPoolName;

    public RealTimeInferencer(
        ModelManagerType modelManager,
        Stats stats,
        String modelCorruptionStat,
        CheckpointDaoType checkpointDao,
        ColdStartWorkerType coldStartWorker,
        SaveResultStrategyType resultWriteWorker,
        CacheProvider<RCFModelType, CacheType> cache,
        ThreadPool threadPool,
        String threadPoolName
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
        // WeakHashMap allows for automatic removal of entries when the key is no longer referenced elsewhere.
        // This helps prevent memory leaks as the garbage collector can reclaim memory when modelId is no
        // longer in use.
        this.modelLocks = Collections.synchronizedMap(new WeakHashMap<>());
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
        long windowDelayMillis = config.getWindowDelay() == null
            ? 0
            : ((IntervalTimeConfiguration) config.getWindowDelay()).toDuration().toMillis();
        long curExecutionEnd = sample.getDataEndTime().toEpochMilli() + windowDelayMillis;
        long nextExecutionEnd = curExecutionEnd + config.getIntervalInMilliseconds();

        return processWithTimeout(sample, modelState, config, taskId, curExecutionEnd, nextExecutionEnd);
    }

    private boolean processWithTimeout(
        Sample sample,
        ModelState<RCFModelType> modelState,
        Config config,
        String taskId,
        long curExecutionEnd,
        long nextExecutionEnd
    ) {
        String modelId = modelState.getModelId();
        ReentrantLock lock = (ReentrantLock) modelLocks.computeIfAbsent(modelId, k -> new ReentrantLock());

        if (lock.tryLock()) {
            try {
                tryProcess(sample, modelState, config, taskId, curExecutionEnd);
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
            return true;
        } else {
            if (System.currentTimeMillis() >= nextExecutionEnd) {
                LOG.warn("Timeout reached, not retrying.");
            } else {
                // Schedule a retry in one second
                threadPool
                    .schedule(
                        () -> processWithTimeout(sample, modelState, config, taskId, curExecutionEnd, nextExecutionEnd),
                        new TimeValue(1, TimeUnit.SECONDS),
                        threadPoolName
                    );
            }

            return false;
        }
    }

    private boolean tryProcess(Sample sample, ModelState<RCFModelType> modelState, Config config, String taskId, long curExecutionEnd) {
        // execution end time (when job starts execution in this interval) >= last used time => the model state is updated in
        // previous intervals
        // This can happen while scheduled to waiting some other threads have already scored the same interval (e.g., during tests
        // when everything happens fast)
        if (curExecutionEnd < modelState.getLastUsedTime().toEpochMilli()) {
            return false;
        }
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
                    System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                    config.getId(),
                    RequestPriority.MEDIUM,
                    modelId,
                    sample.getValueList(),
                    sample.getDataStartTime().toEpochMilli(),
                    taskId
                )
            );
    }
}
