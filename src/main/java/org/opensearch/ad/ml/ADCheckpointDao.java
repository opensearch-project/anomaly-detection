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
import java.lang.reflect.Type;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.util.ClientUtil;

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
import com.google.gson.reflect.TypeToken;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;

/**
 * DAO for model checkpoints.
 */
public class ADCheckpointDao extends CheckpointDao<ThresholdedRandomCutForest, ADIndex, ADIndexManagement> {

    private static final Logger logger = LogManager.getLogger(ADCheckpointDao.class);
    // ======================================
    // Model serialization/deserialization
    // ======================================
    public static final String ENTITY_RCF = "rcf";
    public static final String ENTITY_THRESHOLD = "th";
    public static final String ENTITY_TRCF = "trcf";
    public static final String FIELD_MODELV2 = "modelV2";
    public static final String DETECTOR_ID = "detectorId";

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

    private final ADIndexManagement indexUtil;
    private final JsonParser parser = new JsonParser();

    // anomaly rate
    private double anomalyRate;
    // Use TypeToken to properly deserialize the double array
    private final Type doubleArrayType;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param client ES search client
     * @param clientUtil utility with ES client
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
    public ADCheckpointDao(
        Client client,
        ClientUtil clientUtil,
        Gson gson,
        RandomCutForestMapper mapper,
        V1JsonToV3StateConverter converter,
        ThresholdedRandomCutForestMapper trcfMapper,
        Schema<ThresholdedRandomCutForestState> trcfSchema,
        Class<? extends ThresholdingModel> thresholdingModelClass,
        ADIndexManagement indexUtil,
        int maxCheckpointBytes,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        double anomalyRate,
        Clock clock
    ) {
        super(
            client,
            clientUtil,
            ADCommonName.CHECKPOINT_INDEX_NAME,
            gson,
            maxCheckpointBytes,
            serializeRCFBufferPool,
            serializeRCFBufferSize,
            indexUtil,
            clock
        );
        this.mapper = mapper;
        this.converter = converter;
        this.trcfMapper = trcfMapper;
        this.trcfSchema = trcfSchema;
        this.thresholdingModelClass = thresholdingModelClass;
        this.indexUtil = indexUtil;
        this.anomalyRate = anomalyRate;
        this.doubleArrayType = new TypeToken<double[][]>() {
        }.getType();
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
            source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
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
        source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
        putModelCheckpoint(modelId, source, listener);
    }

    /**
     * Prepare for index request using the contents of the given model state
     * @param modelState an entity model state
     * @return serialized JSON map or empty map if the state is too bloated
     * @throws IOException  when serialization fails
     */
    @Override
    public Map<String, Object> toIndexSource(ModelState<ThresholdedRandomCutForest> modelState) throws IOException {
        String modelId = modelState.getModelId();
        Map<String, Object> source = new HashMap<>();

        Optional<ThresholdedRandomCutForest> model = modelState.getModel();
        if (model.isPresent()) {
            ThresholdedRandomCutForest entityModel = model.get();

            Optional<String> serializedModel = toCheckpoint(entityModel, modelId);
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
            source.put(FIELD_MODELV2, serializedModel.get());
        }

        Optional<Sample[]> samples = toCheckpoint(modelState.getSamples());
        if (samples.isPresent()) {
            source.put(CommonName.SAMPLE_QUEUE, samples.get());
        }

        // if there are no samples and no model, no need to index as other information are meta data
        if (!source.containsKey(CommonName.SAMPLE_QUEUE) && !source.containsKey(FIELD_MODELV2)) {
            return source;
        }

        String detectorId = modelState.getConfigId();
        source.put(DETECTOR_ID, detectorId);
        // we cannot pass Optional as OpenSearch does not know how to serialize an Optional value

        source.put(CommonName.TIMESTAMP, clock.instant().atZone(ZoneOffset.UTC));
        source.put(org.opensearch.timeseries.constant.CommonName.SCHEMA_VERSION_FIELD, indexUtil.getSchemaVersion(ADIndex.CHECKPOINT));

        Optional<Entity> entity = modelState.getEntity();
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
    public Optional<String> toCheckpoint(ThresholdedRandomCutForest model, String modelId) {
        return AccessController.doPrivileged((PrivilegedAction<Optional<String>>) () -> {
            if (model == null) {
                logger.warn("Empty model");
                return Optional.empty();
            }
            try {
                JsonObject json = new JsonObject();
                if (model != null) {
                    json.addProperty(ENTITY_TRCF, toCheckpoint(model));
                }
                // if json is empty, it will be an empty Json string {}. No need to save it on disk.
                return json.entrySet().isEmpty() ? Optional.empty() : Optional.ofNullable(gson.toJson(json));
            } catch (Exception ex) {
                logger.warn(new ParameterizedMessage("fail to generate checkpoint for [{}]", modelId), ex);
            }
            return Optional.empty();
        });
    }

    String toCheckpoint(ThresholdedRandomCutForest trcf) {
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
     * Load json checkpoint into models
     *
     * @param checkpoint json checkpoint contents
     * @param modelId Model Id
     * @return a pair of entity model and its last checkpoint time; or empty if
     *  the raw checkpoint is too large
     */
    @Override
    protected ModelState<ThresholdedRandomCutForest> fromEntityModelCheckpoint(
        Map<String, Object> checkpoint,
        String modelId,
        String configId
    ) {
        try {
            return AccessController.doPrivileged((PrivilegedAction<ModelState<ThresholdedRandomCutForest>>) () -> {
                Object modelObj = checkpoint.get(FIELD_MODELV2);
                if (modelObj == null) {
                    // in case there is old -format checkpoint
                    modelObj = checkpoint.get(CommonName.FIELD_MODEL);
                }
                if (modelObj == null) {
                    logger.warn(new ParameterizedMessage("Empty model for [{}]", modelId));
                    return null;
                }
                String model = (String) modelObj;
                if (model.length() > maxCheckpointBytes) {
                    logger.warn(new ParameterizedMessage("[{}]'s model too large: [{}] bytes", modelId, model.length()));
                    return null;
                }
                JsonObject json = parser.parse(model).getAsJsonObject();
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

                    if (rcf.isPresent()) {
                        Optional<ThresholdedRandomCutForest> convertedTRCF = convertToTRCF(rcf.get(), threshold);
                        // if checkpoint is corrupted (e.g., some unexpected checkpoint when we missed
                        // the mark in backward compatibility), we are not gonna load the model part
                        // the model will have to use live data to initialize
                        if (convertedTRCF.isPresent()) {
                            trcf = convertedTRCF.get();
                        }
                    }
                }

                Deque<Sample> sampleQueue = processSampleQueue(json, checkpoint, modelId);

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

                ModelState<ThresholdedRandomCutForest> modelState = new ModelState<ThresholdedRandomCutForest>(
                    trcf,
                    modelId,
                    configId,
                    ModelManager.ModelType.TRCF.getName(),
                    clock,
                    0,
                    Optional.ofNullable(entity),
                    sampleQueue
                );
                modelState.setLastCheckpointTime(timestamp);
                return modelState;
            });
        } catch (Exception e) {
            logger.warn("Exception while deserializing checkpoint " + modelId, e);
            // checkpoint corrupted (e.g., a checkpoint not recognized by current code
            // due to bugs). Better redo training.
            return null;
        }
    }

    private Deque<Sample> processSampleQueue(JsonObject json, Map<String, Object> checkpoint, String modelId) {
        Deque<Sample> sampleQueue = new ArrayDeque<>();
        if (json.has(CommonName.ENTITY_SAMPLE)) {
            double[][] samplesArray = this.gson.fromJson(json.getAsJsonArray(CommonName.ENTITY_SAMPLE), doubleArrayType);
            // this branch exists for bwc. Since we didn't record start and end time, we have to give a default 0.
            Arrays
                .stream(samplesArray)
                .map(sampleArray -> new Sample(sampleArray, Instant.ofEpochMilli(0), Instant.ofEpochMilli(0)))
                .forEach(sampleQueue::add);
        } else {
            sampleQueue = loadSampleQueue(checkpoint, modelId);
        }
        return sampleQueue;
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
                    getThresholdModel(thresholdingModelId, ActionListener.wrap(thresholdingModel -> {
                        listener.onResponse(convertToTRCF(forest.get(), thresholdingModel));
                    }, listener::onFailure));
                }
            } catch (Exception e) {
                logger.error(new ParameterizedMessage("Unexpected error when deserializing [{}]", rcfModelId), e);
                listener.onResponse(Optional.empty());
            }
        } else {
            listener.onResponse(Optional.empty());
        }
    }

    @Override
    protected ModelState<ThresholdedRandomCutForest> fromSingleStreamModelCheckpoint(
        Map<String, Object> checkpoint,
        String modelId,
        String configId
    ) {
        // single stream AD code path is still using old way
        throw new UnsupportedOperationException("This method is not supported");
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
                ActionListener.wrap(response -> deserializeTRCFModel(response, modelId, listener), exception -> {
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
        }, exception -> {
            // expected exception, don't print stack trace
            if (exception instanceof IndexNotFoundException) {
                listener.onResponse(Optional.empty());
            } else {
                listener.onFailure(exception);
            }
        }));
    }

    private Optional<Object> processThresholdModelCheckpoint(GetResponse response) {
        return Optional
            .ofNullable(response)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> source.get(CommonName.FIELD_MODEL));
    }

    private Optional<ThresholdedRandomCutForest> convertToTRCF(RandomCutForest rcf, Optional<ThresholdingModel> kllThreshold) {
        if (rcf == null) {
            return Optional.empty();
        }
        // if there is no threshold model (e.g., threshold model is deleted by HourlyCron), we are gonna
        // start with empty list of rcf scores
        List<Double> scores = new ArrayList<>();
        if (kllThreshold.isPresent()) {
            scores = kllThreshold.get().extractScores();
        }
        // last parameter is lastShingledInput. Since we don't know it, use all 0 double array
        return Optional.of(new ThresholdedRandomCutForest(rcf, anomalyRate, scores, new double[rcf.getDimensions()]));
    }

    @Override
    protected DeleteByQueryRequest createDeleteCheckpointRequest(String detectorId) {
        return new DeleteByQueryRequest(indexName)
            .setQuery(new MatchQueryBuilder(DETECTOR_ID, detectorId))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setAbortOnVersionConflict(false) // when current delete happens, previous might not finish.
            // Retry in this case
            .setRequestsPerSecond(500); // throttle delete requests
    }
}
