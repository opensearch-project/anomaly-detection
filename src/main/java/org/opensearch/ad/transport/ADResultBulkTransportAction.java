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

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_INDEX_PRESSURE_HARD_LIMIT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_INDEX_PRESSURE_SOFT_LIMIT;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexingPressure;
import org.opensearch.timeseries.transport.ResultBulkTransportAction;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

public class ADResultBulkTransportAction extends ResultBulkTransportAction<AnomalyResult, ADResultWriteRequest, ADResultBulkRequest> {

    private static final Logger LOG = LogManager.getLogger(ADResultBulkTransportAction.class);

    @Inject
    public ADResultBulkTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        Settings settings,
        ClusterService clusterService,
        Client client
    ) {
        super(
            ADResultBulkAction.NAME,
            transportService,
            actionFilters,
            indexingPressure,
            settings,
            client,
            AD_INDEX_PRESSURE_SOFT_LIMIT.get(settings),
            AD_INDEX_PRESSURE_HARD_LIMIT.get(settings),
            ADCommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ADResultBulkRequest::new
        );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_INDEX_PRESSURE_SOFT_LIMIT, it -> softLimit = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_INDEX_PRESSURE_HARD_LIMIT, it -> hardLimit = it);
    }

    @Override
    protected BulkRequest prepareBulkRequest(float indexingPressurePercent, ADResultBulkRequest request) {
        BulkRequest bulkRequest = new BulkRequest();
        List<ADResultWriteRequest> results = request.getResults();

        if (indexingPressurePercent <= softLimit) {
            for (ADResultWriteRequest resultWriteRequest : results) {
                addResult(bulkRequest, resultWriteRequest.getResult(), resultWriteRequest.getResultIndex());
            }
        } else if (indexingPressurePercent <= hardLimit) {
            // exceed soft limit (60%) but smaller than hard limit (90%)
            float acceptProbability = 1 - indexingPressurePercent;
            for (ADResultWriteRequest resultWriteRequest : results) {
                AnomalyResult result = resultWriteRequest.getResult();
                if (result.isHighPriority() || random.nextFloat() < acceptProbability) {
                    addResult(bulkRequest, result, resultWriteRequest.getResultIndex());
                }
            }
        } else {
            // if exceeding hard limit, only index non-zero grade or error result
            for (ADResultWriteRequest resultWriteRequest : results) {
                AnomalyResult result = resultWriteRequest.getResult();
                if (result.isHighPriority()) {
                    addResult(bulkRequest, result, resultWriteRequest.getResultIndex());
                }
            }
        }

        return bulkRequest;
    }

    private void addResult(BulkRequest bulkRequest, AnomalyResult result, String resultIndex) {
        String index = resultIndex == null ? indexName : resultIndex;
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(index).source(result.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            bulkRequest.add(indexRequest);
        } catch (IOException e) {
            LOG.error("Failed to prepare bulk index request for index " + index, e);
        }
    }
}
