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

package org.opensearch.forecast.ml;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.util.ClientUtil;

import com.amazon.randomcutforest.parkservices.RCFCaster;
import com.amazon.randomcutforest.parkservices.state.RCFCasterMapper;
import com.amazon.randomcutforest.parkservices.state.RCFCasterState;
import com.google.gson.Gson;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;

/**
 * The ForecastCheckpointDao class implements all the functionality required for fetching, updating and
 * removing forecast checkpoints.
 *
 */
public class ForecastCheckpointDao extends CheckpointDao<RCFCaster, ForecastIndex, ForecastIndexManagement> {
    public static final Logger logger = LogManager.getLogger(ForecastCheckpointDao.class);

    static final String NOT_ABLE_TO_DELETE_CHECKPOINT_MSG = "Cannot delete all checkpoints of forecaster";

    RCFCasterMapper mapper;
    private Schema<RCFCasterState> rcfCasterSchema;

    public ForecastCheckpointDao(
        Client client,
        ClientUtil clientUtil,
        Gson gson,
        int maxCheckpointBytes,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        ForecastIndexManagement indexUtil,
        RCFCasterMapper mapper,
        Schema<RCFCasterState> rcfCasterSchema,
        Clock clock
    ) {
        super(
            client,
            clientUtil,
            ForecastIndex.CHECKPOINT.getIndexName(),
            gson,
            maxCheckpointBytes,
            serializeRCFBufferPool,
            serializeRCFBufferSize,
            indexUtil,
            clock
        );
        this.mapper = mapper;
        this.rcfCasterSchema = rcfCasterSchema;
    }

    /**
     * Puts a RCFCaster model checkpoint in the storage. Used in single-stream forecasting.
     *
     * @param modelId id of the model
     * @param caster the RCFCaster model
     * @param listener onResponse is called with null when the operation is completed
     */
    public void putCasterCheckpoint(String modelId, RCFCaster caster, ActionListener<Void> listener) {
        Map<String, Object> source = new HashMap<>();
        Optional<String> modelCheckpoint = toCheckpoint(Optional.of(caster));
        if (modelCheckpoint.isPresent()) {
            source.put(CommonName.FIELD_MODEL, modelCheckpoint.get());
            source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
            source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
            source.put(CommonName.SCHEMA_VERSION_FIELD, indexUtil.getSchemaVersion(ForecastIndex.CHECKPOINT));
            putModelCheckpoint(modelId, source, listener);
        } else {
            listener.onFailure(new RuntimeException("Fail to create checkpoint to save"));
        }
    }

    private Optional<String> toCheckpoint(Optional<RCFCaster> caster) {
        if (caster.isEmpty()) {
            return Optional.empty();
        }
        Optional<String> checkpoint = Optional.empty();
        Map.Entry<LinkedBuffer, Boolean> result = checkoutOrNewBuffer();
        LinkedBuffer buffer = result.getKey();
        boolean needCheckin = result.getValue();
        try {
            checkpoint = toCheckpoint(caster, buffer);
        } catch (Exception e) {
            logger.error("Failed to serialize model", e);
            if (needCheckin) {
                try {
                    serializeRCFBufferPool.invalidateObject(buffer);
                    needCheckin = false;
                } catch (Exception x) {
                    logger.warn("Failed to invalidate buffer", x);
                }
                try {
                    checkpoint = toCheckpoint(caster, LinkedBuffer.allocate(serializeRCFBufferSize));
                } catch (Exception ex) {
                    logger.warn("Failed to generate checkpoint", ex);
                }
            }
        } finally {
            if (needCheckin) {
                try {
                    serializeRCFBufferPool.returnObject(buffer);
                } catch (Exception e) {
                    logger.warn("Failed to return buffer to pool", e);
                }
            }
        }
        return checkpoint;
    }

    private Optional<String> toCheckpoint(Optional<RCFCaster> caster, LinkedBuffer buffer) {
        if (caster.isEmpty()) {
            return Optional.empty();
        }
        try {
            byte[] bytes = AccessController.doPrivileged((PrivilegedAction<byte[]>) () -> {
                RCFCasterState casterState = mapper.toState(caster.get());
                return ProtostuffIOUtil.toByteArray(casterState, rcfCasterSchema, buffer);
            });
            return Optional.ofNullable(Base64.getEncoder().encodeToString(bytes));
        } finally {
            buffer.clear();
        }
    }

    /**
     * Prepare for index request using the contents of the given model state. Used in HC forecasting.
     * @param modelState an entity model state
     * @return serialized JSON map or empty map if the state is too bloated
     * @throws IOException  when serialization fails
     */
    @Override
    public Map<String, Object> toIndexSource(ModelState<RCFCaster> modelState) throws IOException {
        Map<String, Object> source = new HashMap<>();
        Optional<RCFCaster> model = modelState.getModel();

        Optional<String> serializedModel = toCheckpoint(model);
        if (serializedModel.isPresent() && serializedModel.get().length() <= maxCheckpointBytes) {
            // we cannot pass Optional as OpenSearch does not know how to serialize an Optional value
            source.put(CommonName.FIELD_MODEL, serializedModel.get());
        } else {
            logger
                .warn(
                    new ParameterizedMessage(
                        "[{}]'s model is empty or too large: [{}] bytes",
                        modelState.getModelId(),
                        serializedModel.isPresent() ? serializedModel.get().length() : 0
                    )
                );
        }
        Optional<Sample[]> samples = toCheckpoint(modelState.getSamples());
        if (samples.isPresent()) {
            source.put(CommonName.SAMPLE_QUEUE, samples.get());
        }
        // if there are no samples and no model, no need to index as other information are meta data
        if (!source.containsKey(CommonName.SAMPLE_QUEUE) && !source.containsKey(CommonName.FIELD_MODEL)) {
            logger.info("nothing to save for [{}]", modelState.getModelId());
            return source;
        }

        source.put(ForecastCommonName.FORECASTER_ID_KEY, modelState.getConfigId());
        source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
        source.put(CommonName.SCHEMA_VERSION_FIELD, indexUtil.getSchemaVersion(ForecastIndex.CHECKPOINT));

        Optional<Entity> entity = modelState.getEntity();
        if (entity.isPresent()) {
            source.put(CommonName.ENTITY_KEY, entity.get());
        }
        return source;
    }

    private void deserializeRCFCasterModel(GetResponse response, String rcfModelId, ActionListener<Optional<RCFCaster>> listener) {
        Object model = null;
        if (response.isExists()) {
            try {
                model = response.getSource().get(CommonName.FIELD_MODEL);
                listener.onResponse(Optional.ofNullable(toRCFCaster((String) model)));

            } catch (Exception e) {
                logger.error(new ParameterizedMessage("Unexpected error when deserializing [{}]", rcfModelId), e);
                listener.onResponse(Optional.empty());
            }
        } else {
            listener.onResponse(Optional.empty());
        }
    }

    RCFCaster toRCFCaster(String checkpoint) {
        RCFCaster rcfCaster = null;
        if (checkpoint != null && checkpoint.length() > 0) {
            try {
                byte[] bytes = Base64.getDecoder().decode(checkpoint);
                RCFCasterState state = rcfCasterSchema.newMessage();
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    ProtostuffIOUtil.mergeFrom(bytes, state, rcfCasterSchema);
                    return null;
                });
                rcfCaster = mapper.toModel(state);
            } catch (RuntimeException e) {
                logger.error("Failed to deserialize RCFCaster model", e);
            }
        }
        return rcfCaster;
    }

    /**
     * Returns to listener the checkpoint for the RCFCaster model. Used in single-stream forecasting.
     *
     * @param modelId id of the model
     * @param listener onResponse is called with the model checkpoint, or empty for no such model
     */
    public void getCasterModel(String modelId, ActionListener<Optional<RCFCaster>> listener) {
        clientUtil
            .<GetRequest, GetResponse>asyncRequest(
                new GetRequest(indexName, modelId),
                client::get,
                ActionListener.wrap(response -> deserializeRCFCasterModel(response, modelId, listener), exception -> {
                    // expected exception, don't print stack trace
                    if (exception instanceof IndexNotFoundException) {
                        listener.onResponse(Optional.empty());
                    } else {
                        listener.onFailure(exception);
                    }
                })
            );
    }

    /**
     * Load json checkpoint into models. Used in HC forecasting.
     *
     * @param checkpoint json checkpoint contents
     * @param modelId Model Id
     * @return a pair of entity model and its last checkpoint time; or empty if
     *  the raw checkpoint is too large
     */
    @Override
    protected ModelState<RCFCaster> fromEntityModelCheckpoint(Map<String, Object> checkpoint, String modelId, String configId) {
        try {
            return AccessController.doPrivileged((PrivilegedAction<ModelState<RCFCaster>>) () -> {

                RCFCaster rcfCaster = loadRCFCaster(checkpoint, modelId);

                Entity entity = null;
                Object serializedEntity = checkpoint.get(CommonName.ENTITY_KEY);
                if (serializedEntity != null) {
                    try {
                        entity = Entity.fromJsonArray(serializedEntity);
                    } catch (Exception e) {
                        logger.error(new ParameterizedMessage("fail to parse entity", serializedEntity), e);
                    }
                }

                ModelState<RCFCaster> modelState = new ModelState<RCFCaster>(
                    rcfCaster,
                    modelId,
                    configId,
                    ModelManager.ModelType.RCFCASTER.getName(),
                    clock,
                    0,
                    Optional.ofNullable(entity),
                    loadSampleQueue(checkpoint, modelId)
                );

                modelState.setLastCheckpointTime(loadTimestamp(checkpoint, modelId));

                return modelState;
            });
        } catch (Exception e) {
            logger.warn("Exception while deserializing checkpoint " + modelId, e);
            // checkpoint corrupted (e.g., a checkpoint not recognized by current code
            // due to bugs). Better redo training.
            return null;
        }
    }

    /**
     * Delete checkpoints associated with a forecaster.  Used in HC forecaster.
     * @param forecasterId Forecaster Id
     */
    public void deleteModelCheckpointByForecasterId(String forecasterId) {
        // A bulk delete request is performed for each batch of matching documents. If a
        // search or bulk request is rejected, the requests are retried up to 10 times,
        // with exponential back off. If the maximum retry limit is reached, processing
        // halts and all failed requests are returned in the response. Any delete
        // requests that completed successfully still stick, they are not rolled back.
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(indexName)
            .setQuery(new MatchQueryBuilder(ForecastCommonName.FORECASTER_ID_KEY, forecasterId))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setAbortOnVersionConflict(false) // when current delete happens, previous might not finish.
            // Retry in this case
            .setRequestsPerSecond(500); // throttle delete requests
        logger.info("Delete checkpoints of forecaster {}", forecasterId);
        client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, ActionListener.wrap(response -> {
            if (response.isTimedOut() || !response.getBulkFailures().isEmpty() || !response.getSearchFailures().isEmpty()) {
                logFailure(response, forecasterId);
            }
            // can return 0 docs get deleted because:
            // 1) we cannot find matching docs
            // 2) bad stats from OpenSearch. In this case, docs are deleted, but
            // OpenSearch says deleted is 0.
            logger.info("{} " + CheckpointDao.DOC_GOT_DELETED_LOG_MSG, response.getDeleted());
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(CheckpointDao.INDEX_DELETED_LOG_MSG + " {}", forecasterId);
            } else {
                // Gonna eventually delete in daily cron.
                logger.error(NOT_ABLE_TO_DELETE_CHECKPOINT_MSG, exception);
            }
        }));
    }

    @Override
    protected DeleteByQueryRequest createDeleteCheckpointRequest(String configId) {
        return new DeleteByQueryRequest(indexName)
            .setQuery(new MatchQueryBuilder(ForecastCommonName.FORECASTER_ID_KEY, configId))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setAbortOnVersionConflict(false) // when current delete happens, previous might not finish.
            // Retry in this case
            .setRequestsPerSecond(500); // throttle delete requests
    }

    @Override
    protected ModelState<RCFCaster> fromSingleStreamModelCheckpoint(Map<String, Object> checkpoint, String modelId, String configId) {

        return AccessController.doPrivileged((PrivilegedAction<ModelState<RCFCaster>>) () -> {

            RCFCaster rcfCaster = loadRCFCaster(checkpoint, modelId);

            ModelState<RCFCaster> modelState = new ModelState<RCFCaster>(
                rcfCaster,
                modelId,
                configId,
                ModelManager.ModelType.RCFCASTER.getName(),
                clock,
                0,
                Optional.empty(),
                loadSampleQueue(checkpoint, modelId)
            );

            modelState.setLastCheckpointTime(loadTimestamp(checkpoint, modelId));

            return modelState;
        });
    }

    private RCFCaster loadRCFCaster(Map<String, Object> checkpoint, String modelId) {
        String model = (String) checkpoint.get(CommonName.FIELD_MODEL);
        if (model == null || model.length() > maxCheckpointBytes) {
            logger
                .warn(new ParameterizedMessage("[{}]'s model empty or too large: [{}] bytes", modelId, model == null ? 0 : model.length()));
            return null;
        }
        return toRCFCaster(model);
    }

    private Instant loadTimestamp(Map<String, Object> checkpoint, String modelId) {
        String lastCheckpointTimeString = (String) (checkpoint.get(CommonName.TIMESTAMP));
        return Instant.parse(lastCheckpointTimeString);
    }
}
