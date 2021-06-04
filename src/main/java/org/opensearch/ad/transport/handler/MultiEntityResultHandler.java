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
import org.opensearch.ad.util.ThrowingConsumerWrapper;
import org.opensearch.client.Client;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
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
    private static final String SUCCESS_SAVING_RESULT_MSG = "Succeed in saving ";
    private static final String CANNOT_SAVE_RESULT_ERR_MSG = "Cannot save results due to write block.";

    @Inject
    public MultiEntityResultHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService
    ) {
        super(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
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
            if (!indexExists.getAsBoolean()) {
                createIndex.accept(ActionListener.wrap(initResponse -> {
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
