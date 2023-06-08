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

package org.opensearch.ad.rest.handler;

import static org.opensearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;

/**
 * Common handler to process AD request.
 */
public class AnomalyDetectorActionHandler {

    private final Logger logger = LogManager.getLogger(AnomalyDetectorActionHandler.class);

    /**
     * Get detector job for update/delete AD job.
     * If AD job exist, will return error message; otherwise, execute function.
     *
     * @param sdkClusterService SDK cluster service
     * @param client SDK Rest client
     * @param detectorId detector identifier
     * @param listener Listener to send response
     * @param function AD function
     * @param xContentRegistry Registry which is used for XContentParser
     */

    public void getDetectorJob(
        SDKClusterService sdkClusterService,
        SDKRestClient client,
        String detectorId,
        ActionListener listener,
        AnomalyDetectorFunction function,
        NamedXContentRegistry xContentRegistry
    ) {
        if (anomalyDetectorJobIndexExists(client)) {
            GetRequest request = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);
            client
                .get(
                    request,
                    ActionListener
                        .wrap(response -> onGetAdJobResponseForWrite(response, listener, function, xContentRegistry), exception -> {
                            logger.error("Fail to get anomaly detector job: " + detectorId, exception);
                            listener.onFailure(exception);
                        })
                );
        } else {
            function.execute();
        }
    }

    private void onGetAdJobResponseForWrite(
        GetResponse response,
        ActionListener listener,
        AnomalyDetectorFunction function,
        NamedXContentRegistry xContentRegistry
    ) {
        if (response.isExists()) {
            String adJobId = response.getId();
            if (adJobId != null) {
                // check if AD job is running on the detector, if yes, we can't delete the detector
                try (
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetectorJob adJob = AnomalyDetectorJob.parse(parser);
                    if (adJob.isEnabled()) {
                        listener.onFailure(new OpenSearchStatusException("Detector job is running: " + adJobId, RestStatus.BAD_REQUEST));
                        return;
                    }
                } catch (IOException e) {
                    String message = "Failed to parse anomaly detector job " + adJobId;
                    logger.error(message, e);
                    listener.onFailure(new OpenSearchStatusException(message, RestStatus.BAD_REQUEST));
                }
            }
        }
        function.execute();
    }

    private boolean anomalyDetectorJobIndexExists(SDKRestClient sdkRestClient) {
        GetIndexRequest getindexRequest = new GetIndexRequest(ANOMALY_DETECTOR_JOB_INDEX);

        CompletableFuture<Boolean> existsFuture = new CompletableFuture<>();
        sdkRestClient.indices().exists(getindexRequest, ActionListener.wrap(response -> { existsFuture.complete(response); }, exception -> {
            existsFuture.completeExceptionally(exception);
        }));

        Boolean existsResponse = existsFuture
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(Settings.EMPTY).getMillis(), TimeUnit.MILLISECONDS)
            .join();

        return existsResponse.booleanValue();
    }
}
