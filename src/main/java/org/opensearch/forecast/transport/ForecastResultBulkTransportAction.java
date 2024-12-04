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

package org.opensearch.forecast.transport;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_INDEX_PRESSURE_HARD_LIMIT;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_INDEX_PRESSURE_SOFT_LIMIT;

import java.util.List;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.index.IndexingPressure;
import org.opensearch.timeseries.transport.ResultBulkTransportAction;
import org.opensearch.transport.TransportService;

public class ForecastResultBulkTransportAction extends
    ResultBulkTransportAction<ForecastResult, ForecastResultWriteRequest, ForecastResultBulkRequest> {

    @Inject
    public ForecastResultBulkTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        Settings settings,
        ClusterService clusterService,
        Client client
    ) {
        super(
            ForecastResultBulkAction.NAME,
            transportService,
            actionFilters,
            indexingPressure,
            settings,
            client,
            FORECAST_INDEX_PRESSURE_SOFT_LIMIT.get(settings),
            FORECAST_INDEX_PRESSURE_HARD_LIMIT.get(settings),
            ForecastIndex.RESULT.getIndexName(),
            ForecastResultBulkRequest::new
        );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_INDEX_PRESSURE_SOFT_LIMIT, it -> softLimit = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_INDEX_PRESSURE_HARD_LIMIT, it -> hardLimit = it);
    }

    @Override
    public BulkRequest prepareBulkRequest(float indexingPressurePercent, ForecastResultBulkRequest request) {
        BulkRequest bulkRequest = new BulkRequest();
        List<ForecastResultWriteRequest> results = request.getResults();

        if (indexingPressurePercent <= softLimit) {
            for (ForecastResultWriteRequest resultWriteRequest : results) {
                addResult(bulkRequest, resultWriteRequest.getResult(), resultWriteRequest.getResultIndex());
            }
        } else if (indexingPressurePercent <= hardLimit) {
            // exceed soft limit (60%) but smaller than hard limit (90%)
            float acceptProbability = 1 - indexingPressurePercent;
            for (ForecastResultWriteRequest resultWriteRequest : results) {
                ForecastResult result = resultWriteRequest.getResult();
                if (random.nextFloat() < acceptProbability) {
                    addResult(bulkRequest, result, resultWriteRequest.getResultIndex());
                }
            }
        } else {
            // if exceeding hard limit, only index error result
            for (ForecastResultWriteRequest resultWriteRequest : results) {
                ForecastResult result = resultWriteRequest.getResult();
                if (result.isHighPriority()) {
                    addResult(bulkRequest, result, resultWriteRequest.getResultIndex());
                }
            }
        }

        return bulkRequest;
    }

}
