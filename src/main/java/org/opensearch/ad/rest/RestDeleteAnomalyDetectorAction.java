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

package org.opensearch.ad.rest;

import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.transport.DeleteAnomalyDetectorAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.DeleteConfigRequest;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to delete anomaly detector.
 */
public class RestDeleteAnomalyDetectorAction extends BaseRestHandler {

    public static final String DELETE_ANOMALY_DETECTOR_ACTION = "delete_anomaly_detector";

    public RestDeleteAnomalyDetectorAction() {}

    @Override
    public String getName() {
        return DELETE_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID);
        DeleteConfigRequest deleteAnomalyDetectorRequest = new DeleteConfigRequest(detectorId);
        return channel -> client
            .execute(DeleteAnomalyDetectorAction.INSTANCE, deleteAnomalyDetectorRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList
            .of(
                // delete anomaly detector document
                new ReplacedRoute(
                    RestRequest.Method.DELETE,
                    String.format(Locale.ROOT, "%s/{%s}", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID),
                    RestRequest.Method.DELETE,
                    String.format(Locale.ROOT, "%s/{%s}", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID)
                )
            );
    }
}
