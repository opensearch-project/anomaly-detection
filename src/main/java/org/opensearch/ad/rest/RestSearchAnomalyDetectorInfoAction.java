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

import static org.opensearch.timeseries.util.RestHandlerUtils.COUNT;
import static org.opensearch.timeseries.util.RestHandlerUtils.MATCH;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

import com.google.common.collect.ImmutableList;

public class RestSearchAnomalyDetectorInfoAction extends BaseRestHandler {

    public static final String SEARCH_ANOMALY_DETECTOR_INFO_ACTION = "search_anomaly_detector_info";

    private static final Logger logger = LogManager.getLogger(RestSearchAnomalyDetectorInfoAction.class);

    public RestSearchAnomalyDetectorInfoAction() {}

    @Override
    public String getName() {
        return SEARCH_ANOMALY_DETECTOR_INFO_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, org.opensearch.client.node.NodeClient client) throws IOException {
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }

        String detectorName = request.param("name", null);
        String rawPath = request.rawPath();

        SearchAnomalyDetectorInfoRequest searchAnomalyDetectorInfoRequest = new SearchAnomalyDetectorInfoRequest(detectorName, rawPath);
        return channel -> client
            .execute(SearchAnomalyDetectorInfoAction.INSTANCE, searchAnomalyDetectorInfoRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<RestHandler.Route> routes() {
        return ImmutableList.of();
    }

    @Override
    public List<RestHandler.ReplacedRoute> replacedRoutes() {
        return ImmutableList
            .of(
                // get the count of number of detectors
                new ReplacedRoute(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, COUNT),
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, COUNT)
                ),
                // get if a detector name exists with name
                new ReplacedRoute(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, MATCH),
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, MATCH)
                )
            );
    }
}
