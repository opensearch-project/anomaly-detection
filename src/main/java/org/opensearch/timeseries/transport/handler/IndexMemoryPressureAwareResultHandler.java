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

package org.opensearch.timeseries.transport.handler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.client.Client;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;
import org.opensearch.timeseries.transport.ResultBulkRequest;

/**
 * Different from ResultIndexingHandler and ResultBulkIndexingHandler, this class uses
 * customized transport action to bulk index results. These transport action will
 * reduce traffic when index memory pressure is high.
 *
 *
 * @param <BatchRequestType> Batch request type
 * @param <BatchResponseType> Batch response type
 * @param <IndexType> forecasting or AD result index
 * @param <IndexManagementType> Index management class
 */
public abstract class IndexMemoryPressureAwareResultHandler<
    ResultType extends IndexableResult,
    ResultWriteRequestType extends ResultWriteRequest<ResultType>,
    BatchRequestType extends ResultBulkRequest<ResultType, ResultWriteRequestType>,
    BatchResponseType,
    IndexType extends Enum<IndexType> & TimeSeriesIndex,
    IndexManagementType extends IndexManagement<IndexType>
    > {
    private static final Logger LOG = LogManager.getLogger(IndexMemoryPressureAwareResultHandler.class);

    protected final Client client;
    protected final IndexManagementType timeSeriesIndices;
    protected final ClusterService clusterService;

    public IndexMemoryPressureAwareResultHandler(Client client, IndexManagementType timeSeriesIndices, ClusterService clusterService) {
        this.client = client;
        this.timeSeriesIndices = timeSeriesIndices;
        this.clusterService = clusterService;
    }

    /**
     * Execute the bulk request
     * @param currentBulkRequest The bulk request
     * @param listener callback after flushing
     */
    public void flush(BatchRequestType currentBulkRequest, ActionListener<BatchResponseType> listener) {
        // we don't check index blocked as ResultIndexingHandler.index did as the batch request can contains different
        // custom index or default indices.
        ClusterBlockException blockException = clusterService.state().blocks().globalBlockedException(ClusterBlockLevel.WRITE);
        if (blockException != null) {
            listener.onFailure(new TimeSeriesException(CommonMessages.CANNOT_SAVE_RESULT_ERR_MSG));
            return;
        }
        Set<String> customResultIndexOrAlias = new HashSet<>();
        for (ResultWriteRequestType result : currentBulkRequest.getResults()) {
            if (result.getResultIndex() != null) {
                customResultIndexOrAlias.add(result.getResultIndex());
            }
        }
        List<String> customResultIndexOrAliasList = new ArrayList<>(customResultIndexOrAlias);

        // We create custom result index when creating a detector. Custom result index can be rolled over and thus we may need to create a
        // new one.
        if (!timeSeriesIndices.doesDefaultResultIndexExist()) {
            timeSeriesIndices.initDefaultResultIndexDirectly(ActionListener.wrap(initResponse -> {
                if (initResponse.isAcknowledged()) {
                    initCustomIndices(currentBulkRequest, customResultIndexOrAliasList, listener);
                } else {
                    LOG.warn("Creating result index with mappings call not acknowledged.");
                    listener.onFailure(new TimeSeriesException("", "Creating result index with mappings call not acknowledged."));
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    initCustomIndices(currentBulkRequest, customResultIndexOrAliasList, listener);
                } else {
                    LOG.warn("Unexpected error creating result index", exception);
                    listener.onFailure(exception);
                }
            }));
        } else {
            initCustomIndices(currentBulkRequest, customResultIndexOrAliasList, listener);
        }
    }

    private void initCustomIndices(
        BatchRequestType currentBulkRequest,
        List<String> customResultIndexOrAlias,
        ActionListener<BatchResponseType> listener
    ) {
        initCustomIndicesIteration(0, currentBulkRequest, customResultIndexOrAlias, listener);
    }

    private void initCustomIndicesIteration(
        int i,
        BatchRequestType currentBulkRequest,
        List<String> customResultIndexOrAlias,
        ActionListener<BatchResponseType> listener
    ) {
        if (i >= customResultIndexOrAlias.size()) {
            bulk(currentBulkRequest, listener);
            return;
        }
        String indexOrAliasName = customResultIndexOrAlias.get(i);
        if (!timeSeriesIndices.doesIndexExist(indexOrAliasName) && !timeSeriesIndices.doesAliasExist(indexOrAliasName)) {
            timeSeriesIndices.initCustomResultIndexDirectly(indexOrAliasName, ActionListener.wrap(initResponse -> {
                if (initResponse.isAcknowledged()) {
                    initCustomIndicesIteration(i + 1, currentBulkRequest, customResultIndexOrAlias, listener);
                } else {
                    LOG.warn("Creating result index {} with mappings call not acknowledged.", indexOrAliasName);
                    initCustomIndicesIteration(i + 1, currentBulkRequest, customResultIndexOrAlias, listener);
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    initCustomIndicesIteration(i + 1, currentBulkRequest, customResultIndexOrAlias, listener);
                } else {
                    LOG.warn("Unexpected error creating result index", exception);
                    initCustomIndicesIteration(i + 1, currentBulkRequest, customResultIndexOrAlias, listener);
                }
            }));
        } else {
            bulk(currentBulkRequest, listener);
        }
    }

    protected abstract void bulk(BatchRequestType currentBulkRequest, ActionListener<BatchResponseType> listener);
}
