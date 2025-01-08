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
    private final ClusterService clusterService;
    private final Client client;

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
        this.clusterService = clusterService;
        this.client = client;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_INDEX_PRESSURE_SOFT_LIMIT, it -> softLimit = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_INDEX_PRESSURE_HARD_LIMIT, it -> hardLimit = it);
    }

    /**
     * Prepares a {@link BulkRequest} for indexing anomaly detection results.
     *
     * This method processes a list of anomaly detection results provided in the {@link ADResultBulkRequest}.
     * Each result is evaluated based on the current indexing pressure and result priority. If a flattened
     * result index exists for the result, the result is also added to the flattened index.
     *
     * @param indexingPressurePercent the current percentage of indexing pressure. This value influences
     *                                whether a result is indexed based on predefined thresholds and probabilities.
     * @param request                 the {@link ADResultBulkRequest} containing anomaly detection results
     *                                to be processed.
     * @return a {@link BulkRequest} containing all results that are eligible for indexing.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     *     <li>Results are added to the bulk request if the indexing pressure is within acceptable limits
     *         or the result has high priority.</li>
     *     <li>If a flattened result index exists for a result, it is added to the flattened index in addition
     *         to the primary index.</li>
     * </ul>
     *
     * <p><b>Indexing Pressure Thresholds:</b></p>
     * <ul>
     *     <li>Below the soft limit: All results are added.</li>
     *     <li>Between the soft limit and the hard limit: High-priority results are always added, and
     *         other results are added based on a probability that decreases with increasing pressure.</li>
     *     <li>Above the hard limit: Only high-priority results are added.</li>
     * </ul>
     *
     * @see ADResultBulkRequest
     * @see BulkRequest
     * @see ADResultWriteRequest
     */
    @Override
    protected BulkRequest prepareBulkRequest(float indexingPressurePercent, ADResultBulkRequest request) {
        BulkRequest bulkRequest = new BulkRequest();
        List<ADResultWriteRequest> results = request.getResults();

        for (ADResultWriteRequest resultWriteRequest : results) {
            AnomalyResult result = resultWriteRequest.getResult();
            String resultIndex = resultWriteRequest.getResultIndex();

            if (shouldAddResult(indexingPressurePercent, result)) {
                addResult(bulkRequest, result, resultIndex);
                addToFlattenedIndexIfExists(bulkRequest, result, resultIndex);
            }
        }

        return bulkRequest;
    }

    private boolean shouldAddResult(float indexingPressurePercent, AnomalyResult result) {
        if (indexingPressurePercent <= softLimit) {
            // Always add when below soft limit
            return true;
        } else if (indexingPressurePercent <= hardLimit) {
            // exceed soft limit (60%) but smaller than hard limit (90%)
            float acceptProbability = 1 - indexingPressurePercent;
            return result.isHighPriority() || random.nextFloat() < acceptProbability;
        } else {
            // if exceeding hard limit, only index non-zero grade or error result
            return result.isHighPriority();
        }
    }

    private void addToFlattenedIndexIfExists(BulkRequest bulkRequest, AnomalyResult result, String resultIndex) {
        String flattenedResultIndexName = resultIndex + "_flattened_" + result.getDetectorId().toLowerCase();
        if (clusterService.state().metadata().hasIndex(flattenedResultIndexName)) {
            addResult(bulkRequest, result, flattenedResultIndexName);
        }
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
