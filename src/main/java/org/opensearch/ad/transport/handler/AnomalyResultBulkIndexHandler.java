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

import static org.opensearch.ad.constant.ADCommonName.ANOMALY_RESULT_INDEX_ALIAS;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.timeseries.constant.CommonMessages.CAN_NOT_FIND_RESULT_INDEX;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class AnomalyResultBulkIndexHandler extends AnomalyIndexHandler<AnomalyResult> {
    private static final Logger LOG = LogManager.getLogger(AnomalyResultBulkIndexHandler.class);

    private ADIndexManagement anomalyDetectionIndices;

    public AnomalyResultBulkIndexHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService,
        ADIndexManagement anomalyDetectionIndices
    ) {
        super(client, settings, threadPool, ANOMALY_RESULT_INDEX_ALIAS, anomalyDetectionIndices, clientUtil, indexUtils, clusterService);
        this.anomalyDetectionIndices = anomalyDetectionIndices;
    }

    /**
     * Bulk index anomaly results. Create anomaly result index first if it doesn't exist.
     *
     * @param resultIndex anomaly result index
     * @param anomalyResults anomaly results
     * @param listener action listener
     */
    public void bulkIndexAnomalyResult(String resultIndex, List<AnomalyResult> anomalyResults, ActionListener<BulkResponse> listener) {
        if (anomalyResults == null || anomalyResults.size() == 0) {
            listener.onResponse(null);
            return;
        }
        String detectorId = anomalyResults.get(0).getConfigId();
        try {
            if (resultIndex != null) {
                // Only create custom AD result index when create detector, won’t recreate custom AD result index in realtime
                // job and historical analysis later if it’s deleted. If user delete the custom AD result index, and AD plugin
                // recreate it, that may bring confusion.
                if (!anomalyDetectionIndices.doesIndexExist(resultIndex)) {
                    throw new EndRunException(detectorId, CAN_NOT_FIND_RESULT_INDEX + resultIndex, true);
                }
                if (!anomalyDetectionIndices.isValidResultIndexMapping(resultIndex)) {
                    throw new EndRunException(detectorId, "wrong index mapping of custom AD result index", true);
                }
                bulkSaveDetectorResult(resultIndex, anomalyResults, listener);
                return;
            }
            if (!anomalyDetectionIndices.doesDefaultResultIndexExist()) {
                anomalyDetectionIndices.initDefaultResultIndexDirectly(ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        bulkSaveDetectorResult(anomalyResults, listener);
                    } else {
                        String error = "Creating anomaly result index with mappings call not acknowledged";
                        LOG.error(error);
                        listener.onFailure(new TimeSeriesException(error));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        bulkSaveDetectorResult(anomalyResults, listener);
                    } else {
                        listener.onFailure(exception);
                    }
                }));
            } else {
                bulkSaveDetectorResult(anomalyResults, listener);
            }
        } catch (TimeSeriesException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            String error = "Failed to bulk index anomaly result";
            LOG.error(error, e);
            listener.onFailure(new TimeSeriesException(error, e));
        }
    }

    private void bulkSaveDetectorResult(List<AnomalyResult> anomalyResults, ActionListener<BulkResponse> listener) {
        bulkSaveDetectorResult(ANOMALY_RESULT_INDEX_ALIAS, anomalyResults, listener);
    }

    private void bulkSaveDetectorResult(String resultIndex, List<AnomalyResult> anomalyResults, ActionListener<BulkResponse> listener) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        anomalyResults.forEach(anomalyResult -> {
            try (XContentBuilder builder = jsonBuilder()) {
                IndexRequest indexRequest = new IndexRequest(resultIndex)
                    .source(anomalyResult.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
                bulkRequestBuilder.add(indexRequest);
            } catch (Exception e) {
                String error = "Failed to prepare request to bulk index anomaly results";
                LOG.error(error, e);
                throw new TimeSeriesException(error);
            }
        });
        client.bulk(bulkRequestBuilder.request(), ActionListener.wrap(r -> {
            if (r.hasFailures()) {
                String failureMessage = r.buildFailureMessage();
                LOG.warn("Failed to bulk index AD result " + failureMessage);
                listener.onFailure(new TimeSeriesException(failureMessage));
            } else {
                listener.onResponse(r);
            }

        }, e -> {
            LOG.error("bulk index ad result failed", e);
            listener.onFailure(e);
        }));
    }

}
