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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.INDEX_PRESSURE_HARD_LIMIT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.IndexingPressure.MAX_INDEXING_BYTES;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ResultWriteRequest;
import org.opensearch.ad.util.BulkUtil;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.IndexingPressure;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class ADResultBulkTransportAction extends HandledTransportAction<ADResultBulkRequest, ADResultBulkResponse> {

    private static final Logger LOG = LogManager.getLogger(ADResultBulkTransportAction.class);
    private IndexingPressure indexingPressure;
    private final long primaryAndCoordinatingLimits;
    private float softLimit;
    private float hardLimit;
    private String indexName;
    private Client client;
    private Random random;

    @Inject
    public ADResultBulkTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        Settings settings,
        ClusterService clusterService,
        Client client
    ) {
        super(ADResultBulkAction.NAME, transportService, actionFilters, ADResultBulkRequest::new, ThreadPool.Names.SAME);
        this.indexingPressure = indexingPressure;
        this.primaryAndCoordinatingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.softLimit = INDEX_PRESSURE_SOFT_LIMIT.get(settings);
        this.hardLimit = INDEX_PRESSURE_HARD_LIMIT.get(settings);
        this.indexName = CommonName.ANOMALY_RESULT_INDEX_ALIAS;
        this.client = client;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INDEX_PRESSURE_SOFT_LIMIT, it -> softLimit = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INDEX_PRESSURE_HARD_LIMIT, it -> hardLimit = it);
        // random seed is 42. Can be any number
        this.random = new Random(42);
    }

    @Override
    protected void doExecute(Task task, ADResultBulkRequest request, ActionListener<ADResultBulkResponse> listener) {
        // Concurrent indexing memory limit = 10% of heap
        // indexing pressure = indexing bytes / indexing limit
        // Write all until index pressure (global indexing memory pressure) is less than 80% of 10% of heap. Otherwise, index
        // all non-zero anomaly grade index requests and index zero anomaly grade index requests with probability (1 - index pressure).
        long totalBytes = indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes() + indexingPressure.getCurrentReplicaBytes();
        float indexingPressurePercent = (float) totalBytes / primaryAndCoordinatingLimits;
        List<ResultWriteRequest> results = request.getAnomalyResults();

        if (results == null || results.size() < 1) {
            listener.onResponse(new ADResultBulkResponse());
        }

        BulkRequest bulkRequest = new BulkRequest();

        if (indexingPressurePercent <= softLimit) {
            for (ResultWriteRequest resultWriteRequest : results) {
                addResult(bulkRequest, resultWriteRequest.getResult(), resultWriteRequest.getResultIndex());
            }
        } else if (indexingPressurePercent <= hardLimit) {
            // exceed soft limit (60%) but smaller than hard limit (90%)
            float acceptProbability = 1 - indexingPressurePercent;
            for (ResultWriteRequest resultWriteRequest : results) {
                AnomalyResult result = resultWriteRequest.getResult();
                if (result.isHighPriority() || random.nextFloat() < acceptProbability) {
                    addResult(bulkRequest, result, resultWriteRequest.getResultIndex());
                }
            }
        } else {
            // if exceeding hard limit, only index non-zero grade or error result
            for (ResultWriteRequest resultWriteRequest : results) {
                AnomalyResult result = resultWriteRequest.getResult();
                if (result.isHighPriority()) {
                    addResult(bulkRequest, result, resultWriteRequest.getResultIndex());
                }
            }
        }

        if (bulkRequest.numberOfActions() > 0) {
            client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.wrap(bulkResponse -> {
                List<IndexRequest> failedRequests = BulkUtil.getFailedIndexRequest(bulkRequest, bulkResponse);
                listener.onResponse(new ADResultBulkResponse(failedRequests));
            }, e -> {
                LOG.error("Failed to bulk index AD result", e);
                listener.onFailure(e);
            }));
        } else {
            listener.onResponse(new ADResultBulkResponse());
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
