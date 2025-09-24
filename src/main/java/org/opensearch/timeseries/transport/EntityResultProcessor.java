/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.RealTimeInferencer;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.ratelimit.CheckpointReadWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.ratelimit.ColdEntityWorker;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.SaveResultStrategy;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ActionListenerExecutor;
import org.opensearch.timeseries.util.ExceptionUtil;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Shared code to implement an entity result transportation
 * (e.g., EntityForecastResultTransportAction)
 *
 */
public class EntityResultProcessor<RCFModelType extends ThresholdedRandomCutForest, IndexableResultType extends IndexableResult, IntermediateResultType extends IntermediateResult<IndexableResultType>, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, ModelColdStartType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, IndexableResultType>, ModelManagerType extends ModelManager<RCFModelType, IndexableResultType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, ModelColdStartType>, CacheType extends TimeSeriesCache<RCFModelType>, SaveResultStrategyType extends SaveResultStrategy<IndexableResultType, IntermediateResultType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, ColdStartWorkerType extends ColdStartWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType, CacheType, IndexableResultType, IntermediateResultType, ModelManagerType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType>, InferencerType extends RealTimeInferencer<RCFModelType, IndexableResultType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType, ModelManagerType, SaveResultStrategyType, CacheType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType>, HCCheckpointReadWorkerType extends CheckpointReadWorker<RCFModelType, IndexableResultType, IntermediateResultType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriteWorkerType, ModelColdStartType, ModelManagerType, CacheType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType, InferencerType>, ColdEntityWorkerType extends ColdEntityWorker<RCFModelType, IndexableResultType, IndexType, IndexManagementType, CheckpointDaoType, IntermediateResultType, ModelManagerType, CheckpointWriteWorkerType, ModelColdStartType, CacheType, SaveResultStrategyType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, ColdStartWorkerType, InferencerType, HCCheckpointReadWorkerType>> {

    private static final Logger LOG = LogManager.getLogger(EntityResultProcessor.class);

    private CacheProvider<RCFModelType, CacheType> cache;
    private HCCheckpointReadWorkerType checkpointReadQueue;
    private ColdEntityWorkerType coldEntityQueue;
    private InferencerType inferencer;
    private ThreadPool threadPool;
    private String threadPoolName;
    private Clock clock;

    public EntityResultProcessor(
        CacheProvider<RCFModelType, CacheType> cache,
        HCCheckpointReadWorkerType checkpointReadQueue,
        ColdEntityWorkerType coldEntityQueue,
        InferencerType inferencer,
        ThreadPool threadPool,
        String threadPoolName,
        Clock clock
    ) {
        this.cache = cache;
        this.checkpointReadQueue = checkpointReadQueue;
        this.coldEntityQueue = coldEntityQueue;
        this.inferencer = inferencer;
        this.threadPool = threadPool;
        this.threadPoolName = threadPoolName;
        this.clock = clock;
    }

    public ActionListener<Optional<? extends Config>> onGetConfig(
        ActionListener<AcknowledgedResponse> listener,
        String configId,
        EntityResultRequest request,
        Optional<Exception> prevException,
        AnalysisType analysisType
    ) {
        return ActionListenerExecutor.wrap(configOptional -> {
            if (!configOptional.isPresent()) {
                listener.onFailure(new EndRunException(configId, "Config " + configId + " is not available.", false));
                return;
            }

            Config config = configOptional.get();

            if (request.getEntities() == null) {
                listener.onFailure(new EndRunException(configId, "Fail to get any entities from request.", false));
                return;
            }

            Map<Entity, double[]> cacheMissEntities = new ConcurrentHashMap<>();

            // Convert the entrySet to a list for parallel processing
            List<Entry<Entity, double[]>> entityEntries = new ArrayList<>(request.getEntities().entrySet());
            long deadline = clock.millis() + config.getFrequencyInMilliseconds();

            AtomicReference<Exception> processingException = new AtomicReference<>(prevException.orElse(null));
            // Process entities in parallel instead of sequentially
            processEntitiesInParallel(entityEntries, cacheMissEntities, config, configId, request, listener, processingException, deadline);
        }, exception -> {
            LOG
                .error(
                    new ParameterizedMessage(
                        "fail to get entity's analysis result for config [{}]: start: [{}], end: [{}]",
                        configId,
                        request.getStart(),
                        request.getEnd()
                    ),
                    exception
                );
            listener.onFailure(exception);
        }, threadPool.executor(threadPoolName));
    }

    private void processEntitiesInParallel(
        List<Entry<Entity, double[]>> entityEntries,
        Map<Entity, double[]> cacheMissEntities,
        Config config,
        String configId,
        EntityResultRequest request,
        ActionListener<AcknowledgedResponse> listener,
        AtomicReference<Exception> processingException,
        long deadline
    ) {
        if (entityEntries.isEmpty()) {
            // No entities to process; proceed directly with cache misses handling.
            processCacheMissEntities(cacheMissEntities, config, configId, request, listener, processingException);
            return;
        }

        // GroupedActionListener coordinates the multiple parallel operations.
        GroupedActionListener<Void> groupedListener = new GroupedActionListener<>(ActionListener.wrap(r -> {
            // This is called when ALL entities have been processed (successfully or with cache misses).
            processCacheMissEntities(cacheMissEntities, config, configId, request, listener, processingException);
        }, e -> {
            // This is called if ANY of the entity processing calls result in an exception.
            LOG.error("Error occurred during parallel entity processing", e);
            // We still proceed with cache misses even if some hot entities failed.
            processingException.updateAndGet(existing -> {
                if (existing == null) {
                    return e;
                }
                return ExceptionUtil.selectHigherPriorityException(e, existing);
            });
            processCacheMissEntities(cacheMissEntities, config, configId, request, listener, processingException);
        }), entityEntries.size());

        for (Entry<Entity, double[]> entityEntry : entityEntries) {
            // Once the maximum number of threads is reached and all of them are busy, any new tasks will be queued
            // until one of the threads is freed up. The queue is unbounded, so it's possible to starve the queue if
            // the processing takes too long.
            threadPool.executor(threadPoolName).execute(() -> {
                processSingleEntity(entityEntry, cacheMissEntities, config, configId, request, groupedListener, deadline);
            });
        }
    }

    private void processSingleEntity(
        Entry<Entity, double[]> entityEntry,
        Map<Entity, double[]> cacheMissEntities,
        Config config,
        String configId,
        EntityResultRequest request,
        ActionListener<Void> listener, // This is the sub-listener from GroupedActionListener
        long deadline
    ) {
        if (clock.millis() >= deadline) {
            LOG.warn("Timeout reached for config [{}], skipping entity processing.", config.getId());
            listener.onResponse(null);
            return;
        }
        Entity entity = entityEntry.getKey();

        if (isEntityFromOldNodeMsg(entity) && config.getCategoryFields() != null && config.getCategoryFields().size() == 1) {
            Map<String, String> attrValues = entity.getAttributes();
            // handle a request from a version before OpenSearch 1.1.
            entity = Entity.createSingleAttributeEntity(config.getCategoryFields().get(0), attrValues.get(CommonName.EMPTY_FIELD));
        }

        // Create final reference for lambda - without this, compiler error occurs because 'entity' is reassigned above and lambdas can only
        // capture effectively final variables
        final Entity finalEntity = entity;

        Optional<String> modelIdOptional = entity.getModelId(configId);
        if (modelIdOptional.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        try {
            String modelId = modelIdOptional.get();
            double[] datapoint = entityEntry.getValue();
            ModelState<RCFModelType> entityModel = cache.get().get(modelId, config);
            if (entityModel == null) {
                // cache miss
                cacheMissEntities.put(finalEntity, datapoint);
                listener.onResponse(null);
                return;
            }

            inferencer
                .process(
                    new Sample(datapoint, Instant.ofEpochMilli(request.getStart()), Instant.ofEpochMilli(request.getEnd())),
                    entityModel,
                    config,
                    request.getTaskId(),
                    ActionListener.wrap(r -> listener.onResponse(null), e -> {
                        LOG.error("Failed to process entity " + finalEntity, e);
                        listener.onFailure(e);
                    })
                );
        } catch (Exception e) {
            LOG.error("Failed to process entity " + finalEntity, e);
            listener.onFailure(e);
        }
    }

    private void processCacheMissEntities(
        Map<Entity, double[]> cacheMissEntities,
        Config config,
        String configId,
        EntityResultRequest request,
        ActionListener<AcknowledgedResponse> listener,
        AtomicReference<Exception> processingException
    ) {
        try {
            // split hot and cold entities
            Pair<List<Entity>, List<Entity>> hotColdEntities = cache
                .get()
                .selectUpdateCandidate(cacheMissEntities.keySet(), configId, config);

            List<FeatureRequest> hotEntityRequests = new ArrayList<>();
            List<FeatureRequest> coldEntityRequests = new ArrayList<>();

            for (Entity hotEntity : hotColdEntities.getLeft()) {
                double[] hotEntityValue = cacheMissEntities.get(hotEntity);
                if (hotEntityValue == null) {
                    LOG.error(new ParameterizedMessage("feature value should not be null: [{}]", hotEntity));
                    continue;
                }
                hotEntityRequests
                    .add(
                        new FeatureRequest(
                            System.currentTimeMillis() + config.getFrequencyInMilliseconds(),
                            configId,
                            // hot entities has MEDIUM priority
                            RequestPriority.MEDIUM,
                            hotEntityValue,
                            request.getStart(),
                            hotEntity,
                            request.getTaskId()
                        )
                    );
            }

            // cold entities has LOW priority, but long interval entities have normal priorities
            RequestPriority coldEntityPriority = config.isLongFrequency() ? RequestPriority.MEDIUM : RequestPriority.LOW;
            for (Entity coldEntity : hotColdEntities.getRight()) {
                double[] coldEntityValue = cacheMissEntities.get(coldEntity);
                if (coldEntityValue == null) {
                    LOG.error(new ParameterizedMessage("feature value should not be null: [{}]", coldEntity));
                    continue;
                }
                coldEntityRequests
                    .add(
                        new FeatureRequest(
                            System.currentTimeMillis() + config.getFrequencyInMilliseconds(),
                            configId,
                            coldEntityPriority,
                            coldEntityValue,
                            request.getStart(),
                            coldEntity,
                            request.getTaskId()
                        )
                    );
            }

            checkpointReadQueue.putAll(hotEntityRequests);
            coldEntityQueue.putAll(coldEntityRequests);
            // respond back
            Exception aggregated = processingException.get();
            if (aggregated != null) {
                listener.onFailure(aggregated);
            } else {
                listener.onResponse(new AcknowledgedResponse(true));
            }
        } catch (Exception e) {
            LOG.error("Error processing cache miss entities for " + configId, e);
            listener.onFailure(e);
        }
    }

    /**
     * Whether the received entity comes from an node that doesn't support multi-category fields.
     * This can happen during rolling-upgrade or blue/green deployment.
     *
     * Specifically, when receiving an EntityResultRequest from an incompatible node,
     * EntityResultRequest(StreamInput in) gets an String that represents an entity.
     * But Entity class requires both an category field name and value. Since we
     * don't have access to detector config in EntityResultRequest(StreamInput in),
     * we put CommonName.EMPTY_FIELD as the placeholder.  In this method,
     * we use the same CommonName.EMPTY_FIELD to check if the deserialized entity
     * comes from an incompatible node.  If it is, we will add the field name back
     * as EntityResultTranportAction has access to the detector config object.
     *
     * @param categoricalValues deserialized Entity from inbound message.
     * @return Whether the received entity comes from an node that doesn't support multi-category fields.
     */
    private boolean isEntityFromOldNodeMsg(Entity categoricalValues) {
        Map<String, String> attrValues = categoricalValues.getAttributes();
        return (attrValues != null && attrValues.containsKey(CommonName.EMPTY_FIELD));
    }
}
