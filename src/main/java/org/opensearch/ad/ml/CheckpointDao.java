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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.constant.CommonName;
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

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
    public static final String TIMESTAMP = "timestamp";
    public static final String DETECTOR_ID = "detectorId";

    // dependencies
    private final Client client;
    private final ClientUtil clientUtil;

    // configuration
    private final String indexName;

    private Gson gson;
    private RandomCutForestSerDe rcfSerde;

    private final Class<? extends ThresholdingModel> thresholdingModelClass;

    private final AnomalyDetectionIndices indexUtil;
    private final JsonParser parser = new JsonParser();
    // we won't read/write a checkpoint larger than a threshold
    private final int maxCheckpointBytes;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param client ES search client
     * @param clientUtil utility with ES client
     * @param indexName name of the index for model checkpoints
     * @param gson accessor to Gson functionality
     * @param rcfSerde accessor to rcf serialization/deserialization
     * @param thresholdingModelClass thresholding model's class
     * @param indexUtil Index utility methods
     * @param maxCheckpointBytes max checkpoint size in bytes
     */
    public CheckpointDao(
        Client client,
        ClientUtil clientUtil,
        String indexName,
        Gson gson,
        RandomCutForestSerDe rcfSerde,
        Class<? extends ThresholdingModel> thresholdingModelClass,
        AnomalyDetectionIndices indexUtil,
        int maxCheckpointBytes
    ) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.indexName = indexName;
        this.gson = gson;
        this.rcfSerde = rcfSerde;
        this.thresholdingModelClass = thresholdingModelClass;
        this.indexUtil = indexUtil;
        this.maxCheckpointBytes = maxCheckpointBytes;
    }

    /**
     * Puts a model checkpoint in the storage.
     *
     * @deprecated use putModelCheckpoint with listener instead
     *
     * @param modelId Id of the model
     * @param modelCheckpoint Checkpoint data of the model
     */
    @Deprecated
    public void putModelCheckpoint(String modelId, String modelCheckpoint) {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD_MODEL, modelCheckpoint);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));

        if (indexUtil.doesCheckpointIndexExist()) {
            saveModelCheckpointSync(source, modelId);
        } else {
            onCheckpointNotExist(source, modelId, false, null);
        }
    }

    private void saveModelCheckpointSync(Map<String, Object> source, String modelId) {
        clientUtil.<IndexRequest, IndexResponse>timedRequest(new IndexRequest(indexName).id(modelId).source(source), logger, client::index);
    }

    /**
     * Puts a model checkpoint in the storage.
     *
     * @param modelId id of the model
     * @param modelCheckpoint checkpoint of the model
     * @param listener onResponse is called with null when the operation is completed
     */
    public void putModelCheckpoint(String modelId, String modelCheckpoint, ActionListener<Void> listener) {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD_MODEL, modelCheckpoint);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));
        if (indexUtil.doesCheckpointIndexExist()) {
            saveModelCheckpointAsync(source, modelId, listener);
        } else {
            onCheckpointNotExist(source, modelId, true, listener);
        }
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

    private void saveModelCheckpointAsync(Map<String, Object> source, String modelId, ActionListener<Void> listener) {
        clientUtil
            .<IndexRequest, IndexResponse>asyncRequest(
                new IndexRequest(indexName).id(modelId).source(source),
                client::index,
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
        source.put(FIELD_MODEL, serializedModel);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));
        source.put(CommonName.SCHEMA_VERSION_FIELD, indexUtil.getSchemaVersion(ADIndex.CHECKPOINT));
        Optional<Entity> entity = model.getEntity();
        if (entity.isPresent()) {
            source.put(CommonName.ENTITY_KEY, entity.get());
        }

        return source;
    }

    /**
     * Returns the checkpoint for the model.
     *
     * @deprecated use getModelCheckpoint with listener instead
     *
     * @param modelId ID of the model
     * @return model checkpoint, or empty if not found
     */
    @Deprecated
    public Optional<String> getModelCheckpoint(String modelId) {
        return clientUtil
            .<GetRequest, GetResponse>timedRequest(new GetRequest(indexName, modelId), logger, client::get)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> (String) source.get(FIELD_MODEL));
    }

    public String toCheckpoint(EntityModel model) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> {
            if (model == null) {
                logger.warn("Empty model");
                return null;
            }
            JsonObject json = new JsonObject();
            json.add(ENTITY_SAMPLE, gson.toJsonTree(model.getSamples()));
            if (model.getRcf() != null) {
                json.addProperty(ENTITY_RCF, rcfSerde.toJson(model.getRcf()));
            }
            if (model.getThreshold() != null) {
                json.addProperty(ENTITY_THRESHOLD, gson.toJson(model.getThreshold()));
            }
            return gson.toJson(json);
        });
    }

    /**
     * Deletes the model checkpoint for the id.
     *
     * @deprecated use deleteModelCheckpoint with listener instead
     *
     * @param modelId ID of the model checkpoint
     */
    @Deprecated
    public void deleteModelCheckpoint(String modelId) {
        clientUtil.<DeleteRequest, DeleteResponse>timedRequest(new DeleteRequest(indexName, modelId), logger, client::delete);
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
            // if 0 docs get deleted, it means we cannot find matching docs
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
                String model = (String) (checkpoint.get(FIELD_MODEL));
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
                    // verified, don't need privileged call to get permission
                    rcf = rcfSerde.fromJson(json.getAsJsonPrimitive(ENTITY_RCF).getAsString());
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

    /**
     * Read a checkpoint from the index and return the EntityModel object
     * @param modelId Model Id
     * @param listener Listener to return the EntityModel object
     */
    public void restoreModelCheckpoint(String modelId, ActionListener<Optional<Entry<EntityModel, Instant>>> listener) {
        clientUtil.<GetRequest, GetResponse>asyncRequest(new GetRequest(indexName, modelId), client::get, ActionListener.wrap(response -> {
            Optional<Map<String, Object>> checkpointString = processRawCheckpoint(response);
            if (checkpointString.isPresent()) {
                listener.onResponse(fromEntityModelCheckpoint(checkpointString.get(), modelId));
            } else {
                listener.onResponse(Optional.empty());
            }
        }, listener::onFailure));
    }

    /**
     * Returns to listener the checkpoint for the model.
     *
     * @param modelId id of the model
     * @param listener onResponse is called with the model checkpoint, or empty for no such model
     */
    public void getModelCheckpoint(String modelId, ActionListener<Optional<String>> listener) {
        clientUtil
            .<GetRequest, GetResponse>asyncRequest(
                new GetRequest(indexName, modelId),
                client::get,
                ActionListener.wrap(response -> listener.onResponse(processModelCheckpoint(response)), listener::onFailure)
            );
    }

    private Optional<String> processModelCheckpoint(GetResponse response) {
        return Optional
            .ofNullable(response)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> (String) source.get(FIELD_MODEL));
    }

    public Optional<Map<String, Object>> processRawCheckpoint(GetResponse response) {
        return Optional.ofNullable(response).filter(GetResponse::isExists).map(GetResponse::getSource);
    }
}
