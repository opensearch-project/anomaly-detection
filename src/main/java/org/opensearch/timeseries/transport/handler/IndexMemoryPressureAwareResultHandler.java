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
public abstract class IndexMemoryPressureAwareResultHandler<BatchRequestType, BatchResponseType, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>> {
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
        try {
            // Only create custom result index when creating detector, won’t recreate custom AD result index in realtime
            // job and historical analysis later if it’s deleted. If user delete the custom AD result index, and AD plugin
            // recreate it, that may bring confusion.
            if (!timeSeriesIndices.doesDefaultResultIndexExist()) {
                timeSeriesIndices.initDefaultResultIndexDirectly(ActionListener.wrap(initResponse -> {
                    if (initResponse.isAcknowledged()) {
                        bulk(currentBulkRequest, listener);
                    } else {
                        LOG.warn("Creating result index with mappings call not acknowledged.");
                        listener.onFailure(new TimeSeriesException("", "Creating result index with mappings call not acknowledged."));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        bulk(currentBulkRequest, listener);
                    } else {
                        LOG.warn("Unexpected error creating result index", exception);
                        listener.onFailure(exception);
                    }
                }));
            } else {
                bulk(currentBulkRequest, listener);
            }
        } catch (Exception e) {
            LOG.warn("Error in bulking results", e);
            listener.onFailure(e);
        }
    }

    protected abstract void bulk(BatchRequestType currentBulkRequest, ActionListener<BatchResponseType> listener);
}
