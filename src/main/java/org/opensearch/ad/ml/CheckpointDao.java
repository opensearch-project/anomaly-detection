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

import java.io.IOException;
import java.io.StringReader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
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
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ScrollableHitSource;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV2StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.amazon.randomcutforest.state.RandomCutForestState;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;

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

    // ======================================
    // Model serialization/deserialization
    // ======================================
    public static final String ENTITY_SAMPLE = "sp";
    public static final String ENTITY_RCF = "rcf";
    public static final String ENTITY_THRESHOLD = "th";
    public static final String FIELD_MODEL = "model";
    public static final String FIELD_MODELV2 = "modelV2";
    public static final String TIMESTAMP = "timestamp";
    public static final String DETECTOR_ID = "detectorId";

    // dependencies
    private final Client client;
    private final ClientUtil clientUtil;

    // configuration
    private final String indexName;

    private Gson gson;
    private RandomCutForestMapper mapper;
    private Schema<RandomCutForestState> schema;
    private V1JsonToV2StateConverter converter;

    private final Class<? extends ThresholdingModel> thresholdingModelClass;

    private final AnomalyDetectionIndices indexUtil;
    private final JsonParser parser = new JsonParser();
    // we won't read/write a checkpoint larger than a threshold
    private final int maxCheckpointBytes;

    private final TypeAdapter<JsonObject> strictGsonObjectAdapter;
    private final GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    private final int serializeRCFBufferSize;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param client ES search client
     * @param clientUtil utility with ES client
     * @param indexName name of the index for model checkpoints
     * @param gson accessor to Gson functionality
     * @param mapper RCF model serialization utility
     * @param schema RandomCutForestState schema used by ProtoStuff
     * @param converter converter from rcf v1 serde to protostuff based format
     * @param thresholdingModelClass thresholding model's class
     * @param indexUtil Index utility methods
     * @param maxCheckpointBytes max checkpoint size in bytes
     * @param serializeRCFBufferPool object pool for serializing rcf models
     * @param serializeRCFBufferSize the size of the buffer for RCF serialization
     */
    public CheckpointDao(
        Client client,
        ClientUtil clientUtil,
        String indexName,
        Gson gson,
        RandomCutForestMapper mapper,
        Schema<RandomCutForestState> schema,
        V1JsonToV2StateConverter converter,
        Class<? extends ThresholdingModel> thresholdingModelClass,
        AnomalyDetectionIndices indexUtil,
        int maxCheckpointBytes,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize
    ) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.indexName = indexName;
        this.gson = gson;
        this.mapper = mapper;
        this.schema = schema;
        this.converter = converter;
        this.thresholdingModelClass = thresholdingModelClass;
        this.indexUtil = indexUtil;
        this.maxCheckpointBytes = maxCheckpointBytes;
        this.strictGsonObjectAdapter = new Gson().getAdapter(JsonObject.class);
        this.serializeRCFBufferPool = serializeRCFBufferPool;
        this.serializeRCFBufferSize = serializeRCFBufferSize;
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
    public void putRCFCheckpoint(String modelId, RandomCutForest forest, ActionListener<Void> listener) {
        Map<String, Object> source = new HashMap<>();
        String modelCheckpoint = rcfModelToCheckpoint(forest);
        if (modelCheckpoint != null) {
            source.put(FIELD_MODELV2, modelCheckpoint);
            source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));
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
        source.put(FIELD_MODEL, modelCheckpoint);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));
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
        Map<String, Object> source = new HashMap<>();
        EntityModel model = modelState.getModel();
        String serializedModel = toCheckpoint(model);
        if (serializedModel == null || serializedModel.length() > maxCheckpointBytes) {
            logger
                .warn(
                    new ParameterizedMessage(
                        "[{}]'s model empty or too large: [{}] bytes",
                        modelState.getModelId(),
                        serializedModel == null ? 0 : serializedModel.length()
                    )
                );
            return source;
        }
        String detectorId = modelState.getDetectorId();
        source.put(DETECTOR_ID, detectorId);
        source.put(FIELD_MODELV2, serializedModel);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));
        source.put(CommonName.SCHEMA_VERSION_FIELD, indexUtil.getSchemaVersion(ADIndex.CHECKPOINT));
        Optional<Entity> entity = model.getEntity();
        if (entity.isPresent()) {
            source.put(CommonName.ENTITY_KEY, entity.get());
        }

        return source;
    }

    public String toCheckpoint(EntityModel model) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> {
            if (model == null) {
                logger.warn("Empty model");
                return null;
            }
            JsonObject json = new JsonObject();
            if (model.getSamples() != null && !(model.getSamples().isEmpty())) {
                json.add(ENTITY_SAMPLE, gson.toJsonTree(model.getSamples()));
            }
            if (model.getRcf() != null) {
                String serializedRCF = rcfModelToCheckpoint(model.getRcf());
                if (!Strings.isEmpty(serializedRCF)) {
                    json.addProperty(ENTITY_RCF, serializedRCF);
                }
            }
            if (model.getThreshold() != null) {
                json.addProperty(ENTITY_THRESHOLD, gson.toJson(model.getThreshold()));
            }
            return gson.toJson(json);
        });
    }

    private String rcfModelToCheckpoint(RandomCutForest model) {
        LinkedBuffer borrowedBuffer = null;
        try {
            borrowedBuffer = serializeRCFBufferPool.borrowObject();
            try {
                return rcfModelToCheckpoint(model, borrowedBuffer);
            } catch (Exception e) {
                if (borrowedBuffer != null) {
                    serializeRCFBufferPool.invalidateObject(borrowedBuffer);
                    borrowedBuffer = null;
                }
                // return null when we successfully borrow an object but fail to
                // create a serialized model
                return null;
            } finally {
                if (borrowedBuffer != null) {
                    serializeRCFBufferPool.returnObject(borrowedBuffer);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to borrow an buffer from object pool", e);
            // allocate a new LinkedBuffer and create a serialized model
            return rcfModelToCheckpoint(model, null);
        }
    }

    String rcfModelToCheckpoint(RandomCutForest model, LinkedBuffer buffer) {
        final LinkedBuffer serializationBuffer = buffer == null ? LinkedBuffer.allocate(serializeRCFBufferSize) : buffer;
        try {
            RandomCutForestState state = mapper.toState(model);
            byte[] bytes = AccessController
                .doPrivileged((PrivilegedAction<byte[]>) () -> ProtostuffIOUtil.toByteArray(state, schema, serializationBuffer));
            return Base64.getEncoder().encodeToString(bytes);
        } finally {
            serializationBuffer.clear();
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
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(CommonName.CHECKPOINT_INDEX_NAME)
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
                    modelObj = checkpoint.get(FIELD_MODEL);
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
                // verified, don't need privileged call to get permission
                ArrayDeque<double[]> samples = new ArrayDeque<>(
                    Arrays.asList(this.gson.fromJson(json.getAsJsonArray(ENTITY_SAMPLE), new double[0][0].getClass()))
                );
                RandomCutForest rcf = null;
                if (json.has(ENTITY_RCF)) {
                    String serializedRCF = json.getAsJsonPrimitive(ENTITY_RCF).getAsString();
                    rcf = deserializeRCFModel(serializedRCF);
                }
                ThresholdingModel threshold = null;
                if (json.has(ENTITY_THRESHOLD)) {
                    // verified, don't need privileged call to get permission
                    threshold = this.gson.fromJson(json.getAsJsonPrimitive(ENTITY_THRESHOLD).getAsString(), thresholdingModelClass);
                }

                String lastCheckpointTimeString = (String) (checkpoint.get(TIMESTAMP));
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
                return Optional.of(new SimpleImmutableEntry<>(new EntityModel(entity, samples, rcf, threshold), timestamp));
            });
        } catch (Exception e) {
            logger.warn("Exception while deserializing checkpoint", e);
            throw e;
        }
    }

    private RandomCutForest deserializeRCFModel(String rcfCheckpoint) {
        if (Strings.isEmpty(rcfCheckpoint)) {
            return null;
        }

        String checkpoint = rcfCheckpoint.trim();
        RandomCutForest forest = null;
        try {
            if (isVersionEqualsOne(checkpoint)) {
                RandomCutForestState state = converter.convert(checkpoint, Precision.FLOAT_32);
                forest = mapper.toModel(state);
            } else {
                byte[] bytes = Base64.getDecoder().decode(checkpoint);
                RandomCutForestState state = schema.newMessage();
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    ProtostuffIOUtil.mergeFrom(bytes, state, schema);
                    return null;
                });
                forest = mapper.toModel(state);
            }
        } catch (Exception e) {
            logger.error("Unexpected deserialization error", e);
        }
        return forest;
    }

    /**
     * Whether the input checkpoint has version 1.0
     *
     * @param checkpoint the checkpoint to check
     * @return whether the input checkpoint has version 1.0, where we serialize
     *  rcf model to Json, and when needed deserializing a forest from JSON.
     */
    private boolean isVersionEqualsOne(String checkpoint) {
        // Pre-checking that the first character is '{' and the last is '}'. If it's not the case,
        // it is the new serialization format
        // To make sure, a more expensive way that parses the JSON and check exception is done after
        // the pre-checks.
        return checkpoint.charAt(0) == '{' && checkpoint.charAt(checkpoint.length() - 1) == '}' && isJson(checkpoint);
    }

    /**
     * @param testString test string
     * @return if the given string is json or not
     */
    private boolean isJson(String testString) {
        try (JsonReader reader = new JsonReader(new StringReader(testString))) {
            strictGsonObjectAdapter.read(reader);
            reader.hasNext(); // throws on multiple top level values
            return true;
        } catch (IOException e) {
            return false;
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
    public void getRCFModel(String modelId, ActionListener<Optional<RandomCutForest>> listener) {
        clientUtil.<GetRequest, GetResponse>asyncRequest(new GetRequest(indexName, modelId), client::get, ActionListener.wrap(response -> {
            Optional<Object> rcfCheckpoint = processRCFModelCheckpoint(response);
            if (!rcfCheckpoint.isPresent()) {
                listener.onFailure(new ResourceNotFoundException("", "Fail to find model " + modelId));
                return;
            }
            listener
                .onResponse(
                    rcfCheckpoint
                        .map(
                            checkpoint -> AccessController
                                .doPrivileged((PrivilegedAction<RandomCutForest>) () -> deserializeRCFModel((String) checkpoint))
                        )
                );
        }, listener::onFailure));
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
        }, listener::onFailure));
    }

    private Optional<Object> processRCFModelCheckpoint(GetResponse response) {
        Object model = null;
        if (response.isExists()) {
            model = response.getSource().get(FIELD_MODELV2);
            if (model == null) {
                model = response.getSource().get(FIELD_MODEL);
            }
        }
        return Optional.ofNullable(model);
    }

    private Optional<Object> processThresholdModelCheckpoint(GetResponse response) {
        return Optional
            .ofNullable(response)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> source.get(FIELD_MODEL));
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
                    listener.onFailure(new AnomalyDetectionException("Creating checkpoint with mappings call not acknowledged."));
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
}
