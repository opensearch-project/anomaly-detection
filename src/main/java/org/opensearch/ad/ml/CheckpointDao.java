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

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ScrollableHitSource;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.amazon.randomcutforest.state.RandomCutForestState;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;

/**
 * DAO for model checkpoints.
 */
public class CheckpointDao {

    private static final Logger logger = LogManager.getLogger(CheckpointDao.class);
    static final String TIMEOUT_LOG_MSG = "Timeout while deleting checkpoints of";
    static final String BULK_FAILURE_LOG_MSG = "Bulk failure while deleting checkpoints of";
    static final String SEARCH_FAILURE_LOG_MSG = "Search failure while deleting checkpoints of";
    static final String DOC_GOT_DELETED_LOG_MSG = "checkpoints docs get deleted";
    static final String INDEX_DELETED_LOG_MSG = "Checkpoint index has been deleted.  Has nothing to do:";
    static final String NOT_ABLE_TO_DELETE_LOG_MSG = "Cannot delete all checkpoints of detector";

    public static final String ENTITY_RCF = "rcf";
    public static final String ENTITY_THRESHOLD = "th";
    public static final String ENTITY_TRCF = "trcf";
    public static final String FIELD_MODELV2 = "modelV2";
    public static final String DETECTOR_ID = "detectorId";

    // dependencies
    private final Client client;
    private final ClientUtil clientUtil;

    // configuration
    private final String indexName;

    private Gson gson;
    private RandomCutForestMapper mapper;

    // For further reference v1, v2 and v3 refer to the different variations of RCF models
    // used by AD. v1 was originally used with the launch of OS 1.0. We later converted to v2
    // which included changes requiring a specific converter from v1 to v2 for BWC.
    // v2 models are created by RCF-3.0-rc1 which can be found on maven central.
    // v3 is the latest model version form RCF introduced by RCF-3.0-rc2.
    // Although this version has a converter method for v2 to v3, after BWC testing it was decided that
    // an explicit use of the converter won't be needed as the changes between the models are indeed BWC.
    private V1JsonToV3StateConverter converter;
    private ThresholdedRandomCutForestMapper trcfMapper;
    private Schema<ThresholdedRandomCutForestState> trcfSchema;

    private final Class<? extends ThresholdingModel> thresholdingModelClass;

    private final AnomalyDetectionIndices indexUtil;
    private final JsonParser parser = new JsonParser();
    // we won't read/write a checkpoint larger than a threshold
    private final int maxCheckpointBytes;

    private final GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    private final int serializeRCFBufferSize;
    // anomaly rate
    private double anomalyRate;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param client ES search client
     * @param clientUtil utility with ES client
     * @param indexName name of the index for model checkpoints
     * @param gson accessor to Gson functionality
     * @param mapper RCF model serialization utility
     * @param converter converter from rcf v1 serde to protostuff based format
     * @param trcfMapper TRCF serialization mapper
     * @param trcfSchema TRCF serialization schema
     * @param thresholdingModelClass thresholding model's class
     * @param indexUtil Index utility methods
     * @param maxCheckpointBytes max checkpoint size in bytes
     * @param serializeRCFBufferPool object pool for serializing rcf models
     * @param serializeRCFBufferSize the size of the buffer for RCF serialization
     * @param anomalyRate anomaly rate
     */
    public CheckpointDao(
        Client client,
        ClientUtil clientUtil,
        String indexName,
        Gson gson,
        RandomCutForestMapper mapper,
        V1JsonToV3StateConverter converter,
        ThresholdedRandomCutForestMapper trcfMapper,
        Schema<ThresholdedRandomCutForestState> trcfSchema,
        Class<? extends ThresholdingModel> thresholdingModelClass,
        AnomalyDetectionIndices indexUtil,
        int maxCheckpointBytes,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        double anomalyRate
    ) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.indexName = indexName;
        this.gson = gson;
        this.mapper = mapper;
        this.converter = converter;
        this.trcfMapper = trcfMapper;
        this.trcfSchema = trcfSchema;
        this.thresholdingModelClass = thresholdingModelClass;
        this.indexUtil = indexUtil;
        this.maxCheckpointBytes = maxCheckpointBytes;
        this.serializeRCFBufferPool = serializeRCFBufferPool;
        this.serializeRCFBufferSize = serializeRCFBufferSize;
        this.anomalyRate = anomalyRate;
    }

    private void saveModelCheckpointSync(Map<String, Object> source, String modelId) {
        clientUtil.<IndexRequest, IndexResponse>timedRequest(new IndexRequest(indexName).id(modelId).source(source), logger, client::index);
    }

    private void putModelCheckpoint(String modelId, Map<String, Object> source, ActionListener<Void> listener) {
        if (indexUtil.doesCheckpointIndexExist()) {
            saveModelCheckpointAsync(source, modelId, listener);
        } else {
            onCheckpointNotExist(source, modelId, true, listener);
        }
    }

    /**
     * Puts a rcf model checkpoint in the storage.
     *
     * @param modelId id of the model
     * @param forest the rcf model
     * @param listener onResponse is called with null when the operation is completed
     */
    public void putTRCFCheckpoint(String modelId, ThresholdedRandomCutForest forest, ActionListener<Void> listener) {
        Map<String, Object> source = new HashMap<>();
        String modelCheckpoint = toCheckpoint(forest);
        if (modelCheckpoint != null) {
            source.put(FIELD_MODELV2, modelCheckpoint);
            source.put(CommonName.TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));
            putModelCheckpoint(modelId, source, listener);
        } else {
            listener.onFailure(new RuntimeException("Fail to create checkpoint to save"));
        }
    }

    /**
     * Puts a thresholding model checkpoint in the storage.
     *
     * @param modelId id of the model
     * @param threshold the thresholding model
     * @param listener onResponse is called with null when the operation is completed
     */
    public void putThresholdCheckpoint(String modelId, ThresholdingModel threshold, ActionListener<Void> listener) {
        String modelCheckpoint = AccessController.doPrivileged((PrivilegedAction<String>) () -> gson.toJson(threshold));
        Map<String, Object> source = new HashMap<>();
        source.put(CommonName.FIELD_MODEL, modelCheckpoint);
        source.put(CommonName.TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));
        putModelCheckpoint(modelId, source, listener);
    }

    private void onCheckpointNotExist(Map<String, Object> source, String modelId, boolean isAsync, ActionListener<Void> listener) {
        indexUtil.initCheckpointIndex(ActionListener.wrap(initResponse -> {
            if (initResponse.isAcknowledged()) {
                if (isAsync) {
                    saveModelCheckpointAsync(source, modelId, listener);
                } else {
                    saveModelCheckpointSync(source, modelId);
                }
            } else {
                throw new RuntimeException("Creating checkpoint with mappings call not acknowledged.");
            }
        }, exception -> {
            if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                // It is possible the index has been created while we sending the create request
                if (isAsync) {
                    saveModelCheckpointAsync(source, modelId, listener);
                } else {
                    saveModelCheckpointSync(source, modelId);
                }
            } else {
                logger.error(String.format(Locale.ROOT, "Unexpected error creating index %s", indexName), exception);
            }
        }));
    }

    /**
     * Update the model doc using fields in source.  This ensures we won't touch
     * the old checkpoint and nodes with old/new logic can coexist in a cluster.
     * This is useful for introducing compact rcf new model format.
     *
     * @param source fields to update
     * @param modelId model Id, used as doc id in the checkpoint index
     * @param listener Listener to return response
     */
    private void saveModelCheckpointAsync(Map<String, Object> source, String modelId, ActionListener<Void> listener) {

        UpdateRequest updateRequest = new UpdateRequest(indexName, modelId);
        updateRequest.doc(source);
        // If the document does not already exist, the contents of the upsert element are inserted as a new document.
        // If the document exists, update fields in the map
        updateRequest.docAsUpsert(true);
        clientUtil
            .<UpdateRequest, UpdateResponse>asyncRequest(
                updateRequest,
                client::update,
                ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
            );
    }

    /**
     * Prepare for index request using the contents of the given model state
     * @param modelState an entity model state
     * @return serialized JSON map or empty map if the state is too bloated
     * @throws IOException  when serialization fails
     */
    public Map<String, Object> toIndexSource(ModelState<EntityModel> modelState) throws IOException {
        String modelId = modelState.getModelId();
        Map<String, Object> source = new HashMap<>();
        EntityModel model = modelState.getModel();
        Optional<String> serializedModel = toCheckpoint(model, modelId);
        if (!serializedModel.isPresent() || serializedModel.get().length() > maxCheckpointBytes) {
            logger
                .warn(
                    new ParameterizedMessage(
                        "[{}]'s model is empty or too large: [{}] bytes",
                        modelState.getModelId(),
                        serializedModel.isPresent() ? serializedModel.get().length() : 0
                    )
                );
            return source;
        }
        String detectorId = modelState.getId();
        source.put(DETECTOR_ID, detectorId);
        // we cannot pass Optional as OpenSearch does not know how to serialize an Optional value
        source.put(FIELD_MODELV2, serializedModel.get());
        source.put(CommonName.TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));
        source.put(CommonName.SCHEMA_VERSION_FIELD, indexUtil.getSchemaVersion(ADIndex.CHECKPOINT));
        Optional<Entity> entity = model.getEntity();
        if (entity.isPresent()) {
            source.put(CommonName.ENTITY_KEY, entity.get());
        }

        return source;
    }

    /**
     * Serialized an EntityModel
     * @param model input model
     * @param modelId model id
     * @return serialized string
     */
    public Optional<String> toCheckpoint(EntityModel model, String modelId) {
        return AccessController.doPrivileged((PrivilegedAction<Optional<String>>) () -> {
            if (model == null) {
                logger.warn("Empty model");
                return Optional.empty();
            }
            try {
                JsonObject json = new JsonObject();
                if (model.getSamples() != null && !(model.getSamples().isEmpty())) {
                    json.add(CommonName.ENTITY_SAMPLE, gson.toJsonTree(model.getSamples()));
                }
                if (model.getTrcf().isPresent()) {
                    json.addProperty(ENTITY_TRCF, toCheckpoint(model.getTrcf().get()));
                }
                // if json is empty, it will be an empty Json string {}. No need to save it on disk.
                return json.entrySet().isEmpty() ? Optional.empty() : Optional.ofNullable(gson.toJson(json));
            } catch (Exception ex) {
                logger.warn(new ParameterizedMessage("fail to generate checkpoint for [{}]", modelId), ex);
            }
            return Optional.empty();
        });
    }

    private String toCheckpoint(ThresholdedRandomCutForest trcf) {
        String checkpoint = null;
        Map.Entry<LinkedBuffer, Boolean> result = checkoutOrNewBuffer();
        LinkedBuffer buffer = result.getKey();
        boolean needCheckin = result.getValue();
        try {
            checkpoint = toCheckpoint(trcf, buffer);
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
                    checkpoint = toCheckpoint(trcf, LinkedBuffer.allocate(serializeRCFBufferSize));
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

    private Map.Entry<LinkedBuffer, Boolean> checkoutOrNewBuffer() {
        LinkedBuffer buffer = null;
        boolean isCheckout = true;
        try {
            buffer = serializeRCFBufferPool.borrowObject();
        } catch (Exception e) {
            logger.warn("Failed to borrow a buffer from pool", e);
        }
        if (buffer == null) {
            buffer = LinkedBuffer.allocate(serializeRCFBufferSize);
            isCheckout = false;
        }
        return new SimpleImmutableEntry<LinkedBuffer, Boolean>(buffer, isCheckout);
    }

    private String toCheckpoint(ThresholdedRandomCutForest trcf, LinkedBuffer buffer) {
        try {
            byte[] bytes = AccessController.doPrivileged((PrivilegedAction<byte[]>) () -> {
                ThresholdedRandomCutForestState trcfState = trcfMapper.toState(trcf);
                return ProtostuffIOUtil.toByteArray(trcfState, trcfSchema, buffer);
            });
            return Base64.getEncoder().encodeToString(bytes);
        } finally {
            buffer.clear();
        }
    }

    /**
     * Deletes the model checkpoint for the model.
     *
     * @param modelId id of the model
     * @param listener onReponse is called with null when the operation is completed
     */
    public void deleteModelCheckpoint(String modelId, ActionListener<Void> listener) {
        clientUtil
            .<DeleteRequest, DeleteResponse>asyncRequest(
                new DeleteRequest(indexName, modelId),
                client::delete,
                ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
            );
    }

    /**
     * Delete checkpoints associated with a detector.  Used in multi-entity detector.
     * @param detectorID Detector Id
     */
    public void deleteModelCheckpointByDetectorId(String detectorID) {
        // A bulk delete request is performed for each batch of matching documents. If a
        // search or bulk request is rejected, the requests are retried up to 10 times,
        // with exponential back off. If the maximum retry limit is reached, processing
        // halts and all failed requests are returned in the response. Any delete
        // requests that completed successfully still stick, they are not rolled back.
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(ADCommonName.CHECKPOINT_INDEX_NAME)
            .setQuery(new MatchQueryBuilder(DETECTOR_ID, detectorID))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setAbortOnVersionConflict(false) // when current delete happens, previous might not finish.
                                              // Retry in this case
            .setRequestsPerSecond(500); // throttle delete requests
        logger.info("Delete checkpoints of detector {}", detectorID);
        client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, ActionListener.wrap(response -> {
            if (response.isTimedOut() || !response.getBulkFailures().isEmpty() || !response.getSearchFailures().isEmpty()) {
                logFailure(response, detectorID);
            }
            // can return 0 docs get deleted because:
            // 1) we cannot find matching docs
            // 2) bad stats from OpenSearch. In this case, docs are deleted, but
            // OpenSearch says deleted is 0.
            logger.info("{} " + DOC_GOT_DELETED_LOG_MSG, response.getDeleted());
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(INDEX_DELETED_LOG_MSG + " {}", detectorID);
            } else {
                // Gonna eventually delete in daily cron.
                logger.error(NOT_ABLE_TO_DELETE_LOG_MSG, exception);
            }
        }));
    }

    private void logFailure(BulkByScrollResponse response, String detectorID) {
        if (response.isTimedOut()) {
            logger.warn(TIMEOUT_LOG_MSG + " {}", detectorID);
        } else if (!response.getBulkFailures().isEmpty()) {
            logger.warn(BULK_FAILURE_LOG_MSG + " {}", detectorID);
            for (BulkItemResponse.Failure bulkFailure : response.getBulkFailures()) {
                logger.warn(bulkFailure);
            }
        } else {
            logger.warn(SEARCH_FAILURE_LOG_MSG + " {}", detectorID);
            for (ScrollableHitSource.SearchFailure searchFailure : response.getSearchFailures()) {
                logger.warn(searchFailure);
            }
        }
    }

    /**
     * Load json checkpoint into models
     *
     * @param checkpoint json checkpoint contents
     * @param modelId Model Id
     * @return a pair of entity model and its last checkpoint time; or empty if
     *  the raw checkpoint is too large
     */
    public Optional<Entry<EntityModel, Instant>> fromEntityModelCheckpoint(Map<String, Object> checkpoint, String modelId) {
        try {
            return AccessController.doPrivileged((PrivilegedAction<Optional<Entry<EntityModel, Instant>>>) () -> {
                Object modelObj = checkpoint.get(FIELD_MODELV2);
                if (modelObj == null) {
                    // in case there is old -format checkpoint
                    modelObj = checkpoint.get(CommonName.FIELD_MODEL);
                }
                if (modelObj == null) {
                    logger.warn(new ParameterizedMessage("Empty model for [{}]", modelId));
                    return Optional.empty();
                }
                String model = (String) modelObj;
                if (model.length() > maxCheckpointBytes) {
                    logger.warn(new ParameterizedMessage("[{}]'s model too large: [{}] bytes", modelId, model.length()));
                    return Optional.empty();
                }
                JsonObject json = parser.parse(model).getAsJsonObject();
                ArrayDeque<double[]> samples = null;
                if (json.has(CommonName.ENTITY_SAMPLE)) {
                    // verified, don't need privileged call to get permission
                    samples = new ArrayDeque<>(
                        Arrays.asList(this.gson.fromJson(json.getAsJsonArray(CommonName.ENTITY_SAMPLE), new double[0][0].getClass()))
                    );
                } else {
                    // avoid possible null pointer exception
                    samples = new ArrayDeque<>();
                }
                ThresholdedRandomCutForest trcf = null;

                if (json.has(ENTITY_TRCF)) {
                    trcf = toTrcf(json.getAsJsonPrimitive(ENTITY_TRCF).getAsString());
                } else {
                    Optional<RandomCutForest> rcf = Optional.empty();
                    Optional<ThresholdingModel> threshold = Optional.empty();
                    if (json.has(ENTITY_RCF)) {
                        String serializedRCF = json.getAsJsonPrimitive(ENTITY_RCF).getAsString();
                        rcf = deserializeRCFModel(serializedRCF, modelId);
                    }
                    if (json.has(ENTITY_THRESHOLD)) {
                        // verified, don't need privileged call to get permission
                        threshold = Optional
                            .ofNullable(
                                this.gson.fromJson(json.getAsJsonPrimitive(ENTITY_THRESHOLD).getAsString(), thresholdingModelClass)
                            );
                    }

                    Optional<ThresholdedRandomCutForest> convertedTRCF = convertToTRCF(rcf, threshold);
                    // if checkpoint is corrupted (e.g., some unexpected checkpoint when we missed
                    // the mark in backward compatibility), we are not gonna load the model part
                    // the model will have to use live data to initialize
                    if (convertedTRCF.isPresent()) {
                        trcf = convertedTRCF.get();
                    }
                }

                String lastCheckpointTimeString = (String) (checkpoint.get(CommonName.TIMESTAMP));
                Instant timestamp = Instant.parse(lastCheckpointTimeString);
                Entity entity = null;
                Object serializedEntity = checkpoint.get(CommonName.ENTITY_KEY);
                if (serializedEntity != null) {
                    try {
                        entity = Entity.fromJsonArray(serializedEntity);
                    } catch (Exception e) {
                        logger.error(new ParameterizedMessage("fail to parse entity", serializedEntity), e);
                    }
                }
                EntityModel entityModel = new EntityModel(entity, samples, trcf);
                return Optional.of(new SimpleImmutableEntry<>(entityModel, timestamp));
            });
        } catch (Exception e) {
            logger.warn("Exception while deserializing checkpoint " + modelId, e);
            // checkpoint corrupted (e.g., a checkpoint not recognized by current code
            // due to bugs). Better redo training.
            return Optional.empty();
        }
    }

    ThresholdedRandomCutForest toTrcf(String checkpoint) {
        ThresholdedRandomCutForest trcf = null;
        if (checkpoint != null && !checkpoint.isEmpty()) {
            try {
                byte[] bytes = Base64.getDecoder().decode(checkpoint);
                ThresholdedRandomCutForestState state = trcfSchema.newMessage();
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    ProtostuffIOUtil.mergeFrom(bytes, state, trcfSchema);
                    return null;
                });
                trcf = trcfMapper.toModel(state);
            } catch (RuntimeException e) {
                logger.error("Failed to deserialize TRCF model", e);
            }
        }
        return trcf;
    }

    private Optional<RandomCutForest> deserializeRCFModel(String checkpoint, String modelId) {
        if (checkpoint == null || checkpoint.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(AccessController.doPrivileged((PrivilegedAction<RandomCutForest>) () -> {
            try {
                RandomCutForestState state = converter.convert(checkpoint, Precision.FLOAT_32);
                return mapper.toModel(state);
            } catch (Exception e) {
                logger.error("Unexpected error when deserializing " + modelId, e);
                return null;
            }
        }));
    }

    private void deserializeTRCFModel(
        GetResponse response,
        String rcfModelId,
        ActionListener<Optional<ThresholdedRandomCutForest>> listener
    ) {
        Object model = null;
        if (response.isExists()) {
            try {
                model = response.getSource().get(FIELD_MODELV2);
                if (model != null) {
                    listener.onResponse(Optional.ofNullable(toTrcf((String) model)));
                } else {
                    Object modelV1 = response.getSource().get(CommonName.FIELD_MODEL);
                    Optional<RandomCutForest> forest = deserializeRCFModel((String) modelV1, rcfModelId);
                    if (!forest.isPresent()) {
                        logger.error("Unexpected error when deserializing [{}]", rcfModelId);
                        listener.onResponse(Optional.empty());
                        return;
                    }
                    String thresholdingModelId = SingleStreamModelIdMapper.getThresholdModelIdFromRCFModelId(rcfModelId);
                    // query for threshold model and combinne rcf and threshold model into a ThresholdedRandomCutForest
                    getThresholdModel(
                        thresholdingModelId,
                        ActionListener
                            .wrap(
                                thresholdingModel -> { listener.onResponse(convertToTRCF(forest, thresholdingModel)); },
                                listener::onFailure
                            )
                    );
                }
            } catch (Exception e) {
                logger.error(new ParameterizedMessage("Unexpected error when deserializing [{}]", rcfModelId), e);
                listener.onResponse(Optional.empty());
            }
        } else {
            listener.onResponse(Optional.empty());
        }
    }

    /**
     * Read a checkpoint from the index and return the EntityModel object
     * @param modelId Model Id
     * @param listener Listener to return a pair of entity model and its last checkpoint time
     */
    public void deserializeModelCheckpoint(String modelId, ActionListener<Optional<Entry<EntityModel, Instant>>> listener) {
        clientUtil
            .<GetRequest, GetResponse>asyncRequest(
                new GetRequest(indexName, modelId),
                client::get,
                ActionListener.wrap(response -> { listener.onResponse(processGetResponse(response, modelId)); }, listener::onFailure)
            );
    }

    /**
     * Process a checkpoint GetResponse and return the EntityModel object
     * @param response Checkpoint Index GetResponse
     * @param modelId  Model Id
     * @return a pair of entity model and its last checkpoint time
     */
    public Optional<Entry<EntityModel, Instant>> processGetResponse(GetResponse response, String modelId) {
        Optional<Map<String, Object>> checkpointString = processRawCheckpoint(response);
        if (checkpointString.isPresent()) {
            return fromEntityModelCheckpoint(checkpointString.get(), modelId);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns to listener the checkpoint for the rcf model.
     *
     * @param modelId id of the model
     * @param listener onResponse is called with the model checkpoint, or empty for no such model
     */
    public void getTRCFModel(String modelId, ActionListener<Optional<ThresholdedRandomCutForest>> listener) {
        clientUtil
            .<GetRequest, GetResponse>asyncRequest(
                new GetRequest(indexName, modelId),
                client::get,
                ActionListener
                    .wrap(
                        response -> deserializeTRCFModel(response, modelId, listener),
                        exception -> {
                            // expected exception, don't print stack trace
                            if (exception instanceof IndexNotFoundException) {
                                listener.onResponse(Optional.empty());
                            } else {
                                listener.onFailure(exception);
                            }
                        }
                    )
            );
    }

    /**
     * Returns to listener the checkpoint for the threshold model.
     *
     * @param modelId id of the model
     * @param listener onResponse is called with the model checkpoint, or empty for no such model
     */
    public void getThresholdModel(String modelId, ActionListener<Optional<ThresholdingModel>> listener) {
        clientUtil.<GetRequest, GetResponse>asyncRequest(new GetRequest(indexName, modelId), client::get, ActionListener.wrap(response -> {
            Optional<Object> thresholdCheckpoint = processThresholdModelCheckpoint(response);
            if (!thresholdCheckpoint.isPresent()) {
                listener.onFailure(new ResourceNotFoundException("", "Fail to find model " + modelId));
                return;
            }
            Optional<ThresholdingModel> model = thresholdCheckpoint
                .map(
                    checkpoint -> AccessController
                        .doPrivileged(
                            (PrivilegedAction<ThresholdingModel>) () -> gson.fromJson((String) checkpoint, thresholdingModelClass)
                        )
                );
            listener.onResponse(model);
        },
            exception -> {
                // expected exception, don't print stack trace
                if (exception instanceof IndexNotFoundException) {
                    listener.onResponse(Optional.empty());
                } else {
                    listener.onFailure(exception);
                }
            }
        ));
    }

    private Optional<Object> processThresholdModelCheckpoint(GetResponse response) {
        return Optional
            .ofNullable(response)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> source.get(CommonName.FIELD_MODEL));
    }

    private Optional<Map<String, Object>> processRawCheckpoint(GetResponse response) {
        return Optional.ofNullable(response).filter(GetResponse::isExists).map(GetResponse::getSource);
    }

    public void batchRead(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        clientUtil.<MultiGetRequest, MultiGetResponse>execute(MultiGetAction.INSTANCE, request, listener);
    }

    public void batchWrite(BulkRequest request, ActionListener<BulkResponse> listener) {
        if (indexUtil.doesCheckpointIndexExist()) {
            clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
        } else {
            indexUtil.initCheckpointIndex(ActionListener.wrap(initResponse -> {
                if (initResponse.isAcknowledged()) {
                    clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
                } else {
                    // create index failure. Notify callers using listener.
                    listener.onFailure(new TimeSeriesException("Creating checkpoint with mappings call not acknowledged."));
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    clientUtil.<BulkRequest, BulkResponse>execute(BulkAction.INSTANCE, request, listener);
                } else {
                    logger.error(String.format(Locale.ROOT, "Unexpected error creating checkpoint index"), exception);
                    listener.onFailure(exception);
                }
            }));
        }
    }

    private Optional<ThresholdedRandomCutForest> convertToTRCF(Optional<RandomCutForest> rcf, Optional<ThresholdingModel> kllThreshold) {
        if (!rcf.isPresent()) {
            return Optional.empty();
        }
        // if there is no threshold model (e.g., threshold model is deleted by HourlyCron), we are gonna
        // start with empty list of rcf scores
        List<Double> scores = new ArrayList<>();
        if (kllThreshold.isPresent()) {
            scores = kllThreshold.get().extractScores();
        }
        return Optional.of(new ThresholdedRandomCutForest(rcf.get(), anomalyRate, scores));
    }

    /**
     * Should we save the checkpoint or not
     * @param lastCheckpointTIme Last checkpoint time
     * @param forceWrite Save no matter what
     * @param checkpointInterval Checkpoint interval
     * @param clock UTC clock
     *
     * @return true when forceWrite is true or we haven't saved checkpoint in the
     *  last checkpoint interval; false otherwise
     */
    public boolean shouldSave(Instant lastCheckpointTIme, boolean forceWrite, Duration checkpointInterval, Clock clock) {
        return (lastCheckpointTIme != Instant.MIN && lastCheckpointTIme.plus(checkpointInterval).isBefore(clock.instant())) || forceWrite;
    }
}
