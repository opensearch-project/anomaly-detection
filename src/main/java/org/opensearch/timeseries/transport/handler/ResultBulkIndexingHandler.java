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

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.IndexUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;

/**
 *
 * Utility method to bulk index results
 *
 */
public class ResultBulkIndexingHandler<ResultType extends IndexableResult, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>>
    extends ResultIndexingHandler<ResultType, IndexType, IndexManagementType> {

    private static final Logger LOG = LogManager.getLogger(ResultBulkIndexingHandler.class);

    public ResultBulkIndexingHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        String indexName,
        IndexManagementType timeSeriesIndices,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService,
        Setting<TimeValue> backOffDelaySetting,
        Setting<Integer> maxRetrySetting
    ) {
        super(
            client,
            settings,
            threadPool,
            indexName,
            timeSeriesIndices,
            clientUtil,
            indexUtils,
            clusterService,
            backOffDelaySetting,
            maxRetrySetting
        );
    }

    /**
     * Bulk index results. Create result index first if it doesn't exist.
     *
     * @param resultIndex result index
     * @param results results to save
     * @param configId Config Id
     * @param listener action listener
     */
    public void bulk(String resultIndex, List<ResultType> results, String configId, ActionListener<BulkResponse> listener) {
        if (results == null || results.size() == 0) {
            listener.onResponse(null);
            return;
        }

        try {
            if (resultIndex != null) {
                // Only create custom result index when creating detector, won’t recreate custom AD result index in realtime
                // job and historical analysis later if it’s deleted. If user delete the custom AD result index, and AD plugin
                // recreate it, that may bring confusion.
                if (!timeSeriesIndices.doesIndexExist(resultIndex)) {
                    throw new EndRunException(configId, CommonMessages.CAN_NOT_FIND_RESULT_INDEX + resultIndex, true);
                }
                if (!timeSeriesIndices.isValidResultIndexMapping(resultIndex)) {
                    throw new EndRunException(configId, "wrong index mapping of custom result index", true);
                }
                bulk(resultIndex, results, listener);
                return;
            }
            if (!timeSeriesIndices.doesDefaultResultIndexExist()) {
                timeSeriesIndices.initDefaultResultIndexDirectly(ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        bulk(results, listener);
                    } else {
                        String error = "Creating result index with mappings call not acknowledged";
                        LOG.error(error);
                        listener.onFailure(new TimeSeriesException(error));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        bulk(results, listener);
                    } else {
                        listener.onFailure(exception);
                    }
                }));
            } else {
                bulk(results, listener);
            }
        } catch (TimeSeriesException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            String error = "Failed to bulk index result";
            LOG.error(error, e);
            listener.onFailure(new TimeSeriesException(error, e));
        }
    }

    private void bulk(List<ResultType> anomalyResults, ActionListener<BulkResponse> listener) {
        bulk(defaultResultIndexName, anomalyResults, listener);
    }

    private void bulk(String resultIndex, List<ResultType> results, ActionListener<BulkResponse> listener) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        results.forEach(analysisResult -> {
            try (XContentBuilder builder = jsonBuilder()) {
                IndexRequest indexRequest = new IndexRequest(resultIndex)
                    .source(analysisResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
                bulkRequestBuilder.add(indexRequest);
            } catch (Exception e) {
                String error = "Failed to prepare request to bulk index results";
                LOG.error(error, e);
                throw new TimeSeriesException(error);
            }
        });
        client.bulk(bulkRequestBuilder.request(), ActionListener.wrap(r -> {
            if (r.hasFailures()) {
                String failureMessage = r.buildFailureMessage();
                LOG.warn("Failed to bulk index result " + failureMessage);
                listener.onFailure(new TimeSeriesException(failureMessage));
            } else {
                listener.onResponse(r);
            }

        }, e -> {
            LOG.error("bulk index result failed", e);
            listener.onFailure(e);
        }));
    }
}
