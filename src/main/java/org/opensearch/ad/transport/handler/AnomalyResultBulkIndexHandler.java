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

import static org.opensearch.ad.constant.CommonErrorMessages.CAN_NOT_FIND_RESULT_INDEX;
import static org.opensearch.ad.constant.CommonName.ANOMALY_RESULT_INDEX_ALIAS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.common.settings.Settings;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.threadpool.ThreadPool;

public class AnomalyResultBulkIndexHandler extends AnomalyIndexHandler<AnomalyResult> {
    private static final Logger LOG = LogManager.getLogger(AnomalyResultBulkIndexHandler.class);

    private AnomalyDetectionIndices anomalyDetectionIndices;
    private OpenSearchAsyncClient sdkJavaAsyncClient;
    private Settings settings;

    public AnomalyResultBulkIndexHandler(
        SDKRestClient client,
        OpenSearchAsyncClient sdkJavaAsyncClient,
        Settings settings,
        ThreadPool threadPool,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        SDKClusterService clusterService,
        AnomalyDetectionIndices anomalyDetectionIndices
    ) {
        super(client, settings, threadPool, ANOMALY_RESULT_INDEX_ALIAS, anomalyDetectionIndices, clientUtil, indexUtils, clusterService);
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.sdkJavaAsyncClient = sdkJavaAsyncClient;
        this.settings = settings;
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
        String detectorId = anomalyResults.get(0).getDetectorId();
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
            if (!anomalyDetectionIndices.doesDefaultAnomalyResultIndexExist()) {
                anomalyDetectionIndices.initDefaultAnomalyResultIndexDirectly(ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        bulkSaveDetectorResult(anomalyResults, listener);
                    } else {
                        String error = "Creating anomaly result index with mappings call not acknowledged";
                        LOG.error(error);
                        listener.onFailure(new AnomalyDetectionException(error));
                    }
                }, exception -> {
                    if (exception.getMessage().contains("resource_already_exists_exception")) {
                        // It is possible the index has been created while we sending the create request
                        bulkSaveDetectorResult(anomalyResults, listener);
                    } else {
                        listener.onFailure(exception);
                    }
                }));
            } else {
                bulkSaveDetectorResult(anomalyResults, listener);
            }
        } catch (AnomalyDetectionException e) {
            listener.onFailure(e);
        } catch (Exception e) {
            String error = "Failed to bulk index anomaly result";
            LOG.error(error, e);
            listener.onFailure(new AnomalyDetectionException(error, e));
        }
    }

    private void bulkSaveDetectorResult(List<AnomalyResult> anomalyResults, ActionListener<BulkResponse> listener) {
        bulkSaveDetectorResult(ANOMALY_RESULT_INDEX_ALIAS, anomalyResults, listener);
    }

    private void bulkSaveDetectorResult(String resultIndex, List<AnomalyResult> anomalyResults, ActionListener<BulkResponse> listener) {
        List<BulkOperation> operations = new ArrayList<>();
        anomalyResults.forEach(anomalyResult -> {
            BulkOperation operation = new BulkOperation.Builder().index(i -> i.index(resultIndex).document(anomalyResult)).build();
            operations.add(operation);
        });
        BulkRequest bulkRequest = new BulkRequest.Builder().operations(operations).build();
        try {
            CompletableFuture<BulkResponse> response = sdkJavaAsyncClient.bulk(bulkRequest);
            BulkResponse bulkResponse = response
                .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
                .get();

            if (bulkResponse.errors()) {
                StringBuilder sb = new StringBuilder();
                sb.append("failure in bulk execution:");
                for (int i = 0; i < bulkResponse.items().size(); i++) {
                    BulkResponseItem item = bulkResponse.items().get(i);
                    if (item.error() != null) {
                        sb
                            .append("\n[")
                            .append(i)
                            .append("]: index [")
                            .append(item.index())
                            .append("], id [")
                            .append(item.id())
                            .append("], message [")
                            .append(item.error().reason())
                            .append("]");
                    }
                }
                String failureMessage = sb.toString();
                LOG.warn("Failed to bulk index AD result " + failureMessage);
                listener.onFailure(new AnomalyDetectionException(failureMessage));
            } else {
                listener.onResponse(bulkResponse);
            }

        } catch (Exception e) {
            LOG.error("bulk index ad result failed", e);
            listener.onFailure(e);
        }
    }

}
