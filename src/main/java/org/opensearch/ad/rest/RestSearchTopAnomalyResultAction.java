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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.SearchTopAnomalyResultAction;
import org.opensearch.ad.transport.SearchTopAnomalyResultRequest;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import com.google.common.collect.ImmutableList;

/**
 * The REST handler to search top entity anomaly results for HC detectors.
 */
public class RestSearchTopAnomalyResultAction extends BaseRestHandler {

    private static final String URL_PATH = String
        .format(
            Locale.ROOT,
            "%s/{%s}/%s/%s",
            AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI,
            RestHandlerUtils.DETECTOR_ID,
            RestHandlerUtils.RESULTS,
            RestHandlerUtils.TOP_ANOMALIES
        );
    private final String SEARCH_TOP_ANOMALY_DETECTOR_ACTION = "search_top_anomaly_result";

    public RestSearchTopAnomalyResultAction() {}

    @Override
    public String getName() {
        return SEARCH_TOP_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        // Throw error if disabled
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        // Get the typed request
        SearchTopAnomalyResultRequest searchTopAnomalyResultRequest = getSearchTopAnomalyResultRequest(request);

        return channel -> client
            .execute(SearchTopAnomalyResultAction.INSTANCE, searchTopAnomalyResultRequest, new RestToXContentListener<>(channel));

    }

    private SearchTopAnomalyResultRequest getSearchTopAnomalyResultRequest(RestRequest request) throws IOException {
        String detectorId;
        if (request.hasParam(RestHandlerUtils.DETECTOR_ID)) {
            detectorId = request.param(RestHandlerUtils.DETECTOR_ID);
        } else {
            throw new IllegalStateException(CommonErrorMessages.AD_ID_MISSING_MSG);
        }
        boolean historical = request.paramAsBoolean("historical", false);
        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return SearchTopAnomalyResultRequest.parse(parser, detectorId, historical);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(RestRequest.Method.POST, URL_PATH), new Route(RestRequest.Method.GET, URL_PATH));
    }
}
