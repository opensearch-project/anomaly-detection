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

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ScrollableHitSource;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.transport.client.Client;

import com.google.gson.Gson;

import io.protostuff.LinkedBuffer;

public abstract class CheckpointDao<RCFModelType, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>> {
    private static final Logger logger = LogManager.getLogger(CheckpointDao.class);
    public static final String TIMEOUT_LOG_MSG = "Timeout while deleting checkpoints of";
    public static final String BULK_FAILURE_LOG_MSG = "Bulk failure while deleting checkpoints of";
    public static final String SEARCH_FAILURE_LOG_MSG = "Search failure while deleting checkpoints of";
    public static final String DOC_GOT_DELETED_LOG_MSG = "checkpoints docs get deleted";
    public static final String INDEX_DELETED_LOG_MSG = "Checkpoint index has been deleted.  Has nothing to do:";

    // dependencies
    protected final Client client;
    protected final ClientUtil clientUtil;

    // configuration
    protected final String indexName;

    protected Gson gson;

    // we won't read/write a checkpoint larger than a threshold
    protected final int maxCheckpointBytes;

    protected final GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    protected final int serializeRCFBufferSize;

    protected final IndexManagement<IndexType> indexUtil;
    protected final Clock clock;
    public static final String NOT_ABLE_TO_DELETE_CHECKPOINT_MSG = "Cannot delete all checkpoints of detector";

    public CheckpointDao(
        Client client,
        ClientUtil clientUtil,
        String indexName,
        Gson gson,
        int maxCheckpointBytes,
        GenericObjectPool<LinkedBuffer> serializeRCFBufferPool,
        int serializeRCFBufferSize,
        IndexManagementType indexUtil,
        Clock clock
    ) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.indexName = indexName;
        this.gson = gson;
        this.maxCheckpointBytes = maxCheckpointBytes;
        this.serializeRCFBufferPool = serializeRCFBufferPool;
        this.serializeRCFBufferSize = serializeRCFBufferSize;
        this.indexUtil = indexUtil;
        this.clock = clock;
    }

    protected void putModelCheckpoint(String modelId, Map<String, Object> source, ActionListener<Void> listener) {
        if (indexUtil.doesCheckpointIndexExist()) {
            saveModelCheckpointAsync(source, modelId, listener);
        } else {
            onCheckpointNotExist(source, modelId, listener);
        }
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
    protected void saveModelCheckpointAsync(Map<String, Object> source, String modelId, ActionListener<Void> listener) {

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

    protected void onCheckpointNotExist(Map<String, Object> source, String modelId, ActionListener<Void> listener) {
        indexUtil.initCheckpointIndex(ActionListener.wrap(initResponse -> {
            if (initResponse.isAcknowledged()) {
                saveModelCheckpointAsync(source, modelId, listener);

            } else {
                throw new RuntimeException("Creating checkpoint with mappings call not acknowledged.");
            }
        }, exception -> {
            if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                // It is possible the index has been created while we sending the create request
                saveModelCheckpointAsync(source, modelId, listener);
            } else {
                logger.error(String.format(Locale.ROOT, "Unexpected error creating index %s", indexName), exception);
            }
        }));
    }

    protected Map.Entry<LinkedBuffer, Boolean> checkoutOrNewBuffer() {
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

    protected void logFailure(BulkByScrollResponse response, String id) {
        if (response.isTimedOut()) {
            logger.warn(CheckpointDao.TIMEOUT_LOG_MSG + " {}", id);
        } else if (!response.getBulkFailures().isEmpty()) {
            logger.warn(CheckpointDao.BULK_FAILURE_LOG_MSG + " {}", id);
            for (BulkItemResponse.Failure bulkFailure : response.getBulkFailures()) {
                logger.warn(bulkFailure);
            }
        } else {
            logger.warn(CheckpointDao.SEARCH_FAILURE_LOG_MSG + " {}", id);
            for (ScrollableHitSource.SearchFailure searchFailure : response.getSearchFailures()) {
                logger.warn(searchFailure);
            }
        }
    }

    /**
     * Determines whether to save the checkpoint based on various conditions.
     *
     * @param modelState The current state of the model, which includes the last checkpoint time.
     * @param forceWrite Indicates if the checkpoint should be saved regardless of other conditions.
     * @param checkpointInterval The interval at which checkpoints should be saved.
     * @param clock The clock used to determine the current time (usually in UTC).
     *
     * @return true if both of the following conditions are met:
     *         1. The model state is valid (the model is non-null or it has non-empty samples), and
     *         2. Either forceWrite is true, or the last checkpoint time is not the minimum instant and the current time exceeds the last checkpoint time by at least the checkpoint interval.
     *         Returns false otherwise.
     */
    public boolean shouldSave(ModelState<RCFModelType> modelState, boolean forceWrite, Duration checkpointInterval, Clock clock) {
        if (modelState == null) {
            return false;
        }

        Instant lastCheckpointTime = modelState.getLastCheckpointTime();
        boolean isTimeForCheckpoint = lastCheckpointTime != null
            && !lastCheckpointTime.equals(Instant.MIN)
            && lastCheckpointTime.plus(checkpointInterval).isBefore(clock.instant());
        boolean hasValidSamples = modelState.getSamples() != null && !modelState.getSamples().isEmpty();
        boolean isModelStateValid = modelState.getModel().isPresent() || hasValidSamples;
        return isModelStateValid && (isTimeForCheckpoint || forceWrite);
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

    /**
     * Serialized samples
     * @param samples input samples
     * @return serialized object
     */
    protected Optional<Sample[]> toCheckpoint(Queue<Sample> samples) {
        if (samples == null || samples.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(samples.toArray(new Sample[0]));
    }

    public void batchRead(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        clientUtil.<MultiGetRequest, MultiGetResponse>execute(MultiGetAction.INSTANCE, request, listener);
    }

    public void read(GetRequest request, ActionListener<GetResponse> listener) {
        clientUtil.<GetRequest, GetResponse>execute(GetAction.INSTANCE, request, listener);
    }

    /**
     * Delete checkpoints associated with a config.  Used in multi-entity detector.
     * @param configId Config Id
     */
    public void deleteModelCheckpointByConfigId(String configId) {
        // A bulk delete request is performed for each batch of matching documents. If a
        // search or bulk request is rejected, the requests are retried up to 10 times,
        // with exponential back off. If the maximum retry limit is reached, processing
        // halts and all failed requests are returned in the response. Any delete
        // requests that completed successfully still stick, they are not rolled back.
        DeleteByQueryRequest deleteRequest = createDeleteCheckpointRequest(configId);
        logger.info("Delete checkpoints of config {}", configId);
        client.execute(DeleteByQueryAction.INSTANCE, deleteRequest, ActionListener.wrap(response -> {
            if (response.isTimedOut() || !response.getBulkFailures().isEmpty() || !response.getSearchFailures().isEmpty()) {
                logFailure(response, configId);
            }
            // can return 0 docs get deleted because:
            // 1) we cannot find matching docs
            // 2) bad stats from OpenSearch. In this case, docs are deleted, but
            // OpenSearch says deleted is 0.
            logger.info("{} " + CheckpointDao.DOC_GOT_DELETED_LOG_MSG, response.getDeleted());
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(CheckpointDao.INDEX_DELETED_LOG_MSG + " {}", configId);
            } else {
                // Gonna eventually delete in daily cron.
                logger.error(NOT_ABLE_TO_DELETE_CHECKPOINT_MSG, exception);
            }
        }));
    }

    protected Optional<Map<String, Object>> processRawCheckpoint(GetResponse response) {
        try {
            return Optional.ofNullable(response).filter(GetResponse::isExists).map(GetResponse::getSource);
        } catch (Exception e) {
            // Assuming a logger is available
            logger.error("Error processing raw checkpoint", e);
            return Optional.empty();
        }
    }

    /**
     * Process a checkpoint GetResponse and return the EntityModel object
     * @param response Checkpoint Index GetResponse
     * @param modelId  Model Id
     * @return a pair of entity model and its last checkpoint time
     */
    public ModelState<RCFModelType> processHCGetResponse(GetResponse response, String modelId, String configId) {
        Optional<Map<String, Object>> checkpointString = processRawCheckpoint(response);
        if (checkpointString.isPresent()) {
            return fromEntityModelCheckpoint(checkpointString.get(), modelId, configId);
        } else {
            return null;
        }
    }

    /**
     * Process a checkpoint GetResponse and return the EntityModel object
     * @param response Checkpoint Index GetResponse
     * @param modelId  Model Id
     * @return a pair of entity model and its last checkpoint time
     */
    public ModelState<RCFModelType> processSingleStreamGetResponse(GetResponse response, String modelId, String configId) {
        Optional<Map<String, Object>> checkpointString = processRawCheckpoint(response);
        if (checkpointString.isPresent()) {
            return fromSingleStreamModelCheckpoint(checkpointString.get(), modelId, configId);
        } else {
            return null;
        }
    }

    protected abstract ModelState<RCFModelType> fromEntityModelCheckpoint(Map<String, Object> checkpoint, String modelId, String configId);

    protected abstract ModelState<RCFModelType> fromSingleStreamModelCheckpoint(
        Map<String, Object> checkpoint,
        String modelId,
        String configId
    );

    public abstract Map<String, Object> toIndexSource(ModelState<RCFModelType> modelState) throws IOException;

    protected abstract DeleteByQueryRequest createDeleteCheckpointRequest(String configId);

    protected Deque<Sample> loadSampleQueue(Map<String, Object> checkpoint, String modelId) {
        Deque<Sample> sampleQueue = new ArrayDeque<>();
        // Even though we we save sample_queue using array, after ser/der, we need to read it as List
        // we start using SAMPLE_QUEUE after forecasting refactoring. Previously in AD, we use CommonName.ENTITY_SAMPLE
        // to store samples. The refactoring moves samples out of EntityModel and makes it a first-level field.
        List<Map<String, Object>> samples = (List<Map<String, Object>>) checkpoint.get(CommonName.SAMPLE_QUEUE);
        if (samples != null) {
            samples.forEach(sampleMap -> {
                try {
                    Sample sample = Sample.extractSample(sampleMap);
                    if (sample != null) {
                        sampleQueue.add(sample);
                    }
                } catch (Exception e) {
                    logger.warn("Exception while deserializing samples for " + modelId, e);
                }
            });
        }
        // can be null when checkpoint corrupted (e.g., a checkpoint not recognized by current code
        // due to bugs). Better redo training.
        return sampleQueue;
    }
}
