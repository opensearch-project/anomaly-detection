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
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.transport.ADResultBulkAction;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.transport.ResultBulkResponse;
import org.opensearch.timeseries.transport.handler.IndexMemoryPressureAwareResultHandler;

public class ADIndexMemoryPressureAwareResultHandler extends
    IndexMemoryPressureAwareResultHandler<ADResultBulkRequest, ResultBulkResponse, ADIndex, ADIndexManagement> {
    private static final Logger LOG = LogManager.getLogger(ADIndexMemoryPressureAwareResultHandler.class);

    @Inject
    public ADIndexMemoryPressureAwareResultHandler(
        Client client,
        ADIndexManagement anomalyDetectionIndices,
        ClusterService clusterService
    ) {
        super(client, anomalyDetectionIndices, clusterService);
    }

    @Override
    protected void bulk(ADResultBulkRequest currentBulkRequest, ActionListener<ResultBulkResponse> listener) {
        if (currentBulkRequest.numberOfActions() <= 0) {
            listener.onFailure(new TimeSeriesException("no result to save"));
            return;
        }
        client.execute(ADResultBulkAction.INSTANCE, currentBulkRequest, ActionListener.<ResultBulkResponse>wrap(response -> {
            LOG.debug(CommonMessages.SUCCESS_SAVING_RESULT_MSG);
            listener.onResponse(response);
        }, exception -> {
            LOG.error("Error in bulking results", exception);
            listener.onFailure(exception);
        }));
    }
}
