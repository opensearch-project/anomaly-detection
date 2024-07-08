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

package org.opensearch.forecast.transport.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.forecast.transport.ForecastResultBulkAction;
import org.opensearch.forecast.transport.ForecastResultBulkRequest;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.transport.ResultBulkResponse;
import org.opensearch.timeseries.transport.handler.IndexMemoryPressureAwareResultHandler;

public class ForecastIndexMemoryPressureAwareResultHandler extends
    IndexMemoryPressureAwareResultHandler<ForecastResult, ForecastResultWriteRequest, ForecastResultBulkRequest, ResultBulkResponse, ForecastIndex, ForecastIndexManagement> {
    private static final Logger LOG = LogManager.getLogger(ForecastIndexMemoryPressureAwareResultHandler.class);

    @Inject
    public ForecastIndexMemoryPressureAwareResultHandler(
        Client client,
        ForecastIndexManagement anomalyDetectionIndices,
        ClusterService clusterService
    ) {
        super(client, anomalyDetectionIndices, clusterService);
    }

    @Override
    public void bulk(ForecastResultBulkRequest currentBulkRequest, ActionListener<ResultBulkResponse> listener) {
        if (currentBulkRequest.numberOfActions() <= 0) {
            listener.onFailure(new TimeSeriesException("no result to save"));
            return;
        }
        // we retry failed bulk requests in ResultWriteWorker
        client.execute(ForecastResultBulkAction.INSTANCE, currentBulkRequest, ActionListener.<ResultBulkResponse>wrap(response -> {
            LOG.debug(CommonMessages.SUCCESS_SAVING_RESULT_MSG);
            listener.onResponse(response);
        }, exception -> {
            LOG.error("Error in bulking results", exception);
            listener.onFailure(exception);
        }));
    }
}
