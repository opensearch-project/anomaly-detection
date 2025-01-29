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

package org.opensearch.timeseries.transport;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.IndexingPressure.MAX_INDEXING_BYTES;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexingPressure;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;
import org.opensearch.timeseries.util.BulkUtil;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

@SuppressWarnings("rawtypes")
public abstract class ResultBulkTransportAction<ResultType extends IndexableResult, ADResultWriteRequestType extends ResultWriteRequest<ResultType>, ResultBulkRequestType extends ResultBulkRequest<ResultType, ADResultWriteRequestType>>
    extends HandledTransportAction<ResultBulkRequestType, ResultBulkResponse> {
    private static final Logger LOG = LogManager.getLogger(ResultBulkTransportAction.class);
    protected IndexingPressure indexingPressure;
    private final long primaryAndCoordinatingLimits;
    protected float softLimit;
    protected float hardLimit;
    protected String indexName;
    private Client client;
    protected Random random;
    protected NodeStateManager nodeStateManager;

    public ResultBulkTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        Settings settings,
        Client client,
        float softLimit,
        float hardLimit,
        String indexName,
        Writeable.Reader<ResultBulkRequestType> requestReader
    ) {
        super(actionName, transportService, actionFilters, requestReader, ThreadPool.Names.SAME);
        this.indexingPressure = indexingPressure;
        this.primaryAndCoordinatingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.client = client;

        this.softLimit = softLimit;
        this.hardLimit = hardLimit;
        this.indexName = indexName;

        // random seed is 42. Can be any number
        this.random = new Random(42);
    }

    @Override
    protected void doExecute(Task task, ResultBulkRequestType request, ActionListener<ResultBulkResponse> listener) {
        // Concurrent indexing memory limit = 10% of heap
        // indexing pressure = indexing bytes / indexing limit
        // Write all until index pressure (global indexing memory pressure) is less than 80% of 10% of heap. Otherwise, index
        // all non-zero anomaly grade index requests and index zero anomaly grade index requests with probability (1 - index pressure).
        long totalBytes = indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes() + indexingPressure.getCurrentReplicaBytes();
        float indexingPressurePercent = (float) totalBytes / primaryAndCoordinatingLimits;
        List<? extends ResultWriteRequest> results = request.getResults();

        if (results == null || results.size() < 1) {
            listener.onResponse(new ResultBulkResponse());
        }

        BulkRequest bulkRequest = prepareBulkRequest(indexingPressurePercent, request);

        if (bulkRequest.numberOfActions() > 0) {
            client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.wrap(bulkResponse -> {
                List<IndexRequest> failedRequests = BulkUtil.getFailedIndexRequest(bulkRequest, bulkResponse);
                listener.onResponse(new ResultBulkResponse(failedRequests));
            }, e -> {
                LOG.error("Failed to bulk index AD result", e);
                listener.onFailure(e);
            }));
        } else {
            listener.onResponse(new ResultBulkResponse());
        }
    }

    protected abstract BulkRequest prepareBulkRequest(float indexingPressurePercent, ResultBulkRequestType request);

    protected void addResult(BulkRequest bulkRequest, ToXContentObject result, String resultIndex) {
        String index = resultIndex == null ? indexName : resultIndex;
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(index).source(result.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            bulkRequest.add(indexRequest);
        } catch (IOException e) {
            LOG.error("Failed to prepare bulk index request for index " + index, e);
        }
    }
}
