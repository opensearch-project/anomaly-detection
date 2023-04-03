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

package org.opensearch.ad.transport.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.transport.ADResultBulkAction;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.ad.transport.ADResultBulkResponse;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.threadpool.ThreadPool;

/**
 * EntityResultTransportAction depends on this class.  I cannot use
 * AnomalyIndexHandler &lt; AnomalyResult &gt; . All transport actions
 * needs dependency injection.  Guice has a hard time initializing generics class
 * AnomalyIndexHandler &lt; AnomalyResult &gt; due to type erasure.
 * To avoid that, I create a class with a built-in details so
 * that Guice would be able to work out the details.
 *
 */
public class MultiEntityResultHandler extends AnomalyIndexHandler<AnomalyResult> {
    private static final Logger LOG = LogManager.getLogger(MultiEntityResultHandler.class);
    // package private for testing
    static final String SUCCESS_SAVING_RESULT_MSG = "Result saved successfully.";
    static final String CANNOT_SAVE_RESULT_ERR_MSG = "Cannot save results due to write block.";

    @Inject
    public MultiEntityResultHandler(
        SDKRestClient client,
        Settings settings,
        ThreadPool threadPool,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        SDKClusterService clusterService
    ) {
        super(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            anomalyDetectionIndices,
            clientUtil,
            indexUtils,
            clusterService
        );
    }

    /**
     * Execute the bulk request
     * @param currentBulkRequest The bulk request
     * @param listener callback after flushing
     */
    public void flush(ADResultBulkRequest currentBulkRequest, ActionListener<ADResultBulkResponse> listener) {
        if (indexUtils.checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, this.indexName)) {
            listener.onFailure(new AnomalyDetectionException(CANNOT_SAVE_RESULT_ERR_MSG));
            return;
        }

        try {
            if (!anomalyDetectionIndices.doesDefaultAnomalyResultIndexExist()) {
                anomalyDetectionIndices.initDefaultAnomalyResultIndexDirectly(ActionListener.wrap(initResponse -> {
                    if (initResponse.isAcknowledged()) {
                        bulk(currentBulkRequest, listener);
                    } else {
                        LOG.warn("Creating result index with mappings call not acknowledged.");
                        listener.onFailure(new AnomalyDetectionException("", "Creating result index with mappings call not acknowledged."));
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

    private void bulk(ADResultBulkRequest currentBulkRequest, ActionListener<ADResultBulkResponse> listener) {
        if (currentBulkRequest.numberOfActions() <= 0) {
            listener.onFailure(new AnomalyDetectionException("no result to save"));
            return;
        }
        client.execute(ADResultBulkAction.INSTANCE, currentBulkRequest, ActionListener.<ADResultBulkResponse>wrap(response -> {
            LOG.debug(SUCCESS_SAVING_RESULT_MSG);
            listener.onResponse(response);
        }, exception -> {
            LOG.error("Error in bulking results", exception);
            listener.onFailure(exception);
        }));
    }
}
