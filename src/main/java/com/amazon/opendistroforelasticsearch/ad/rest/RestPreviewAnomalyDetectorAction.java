/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.rest;

import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.PREVIEW;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestToXContentListener;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorExecutionInput;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.transport.PreviewAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.PreviewAnomalyDetectorRequest;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.ImmutableList;

public class RestPreviewAnomalyDetectorAction extends BaseRestHandler {

    public static final String PREVIEW_ANOMALY_DETECTOR_ACTION = "preview_anomaly_detector";

    private static final Logger logger = LogManager.getLogger(RestPreviewAnomalyDetectorAction.class);

    public RestPreviewAnomalyDetectorAction() {}

    @Override
    public String getName() {
        return PREVIEW_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, org.opensearch.client.node.NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        AnomalyDetectorExecutionInput input = getAnomalyDetectorExecutionInput(request);

        return channel -> {
            String rawPath = request.rawPath();
            String error = validateAdExecutionInput(input);
            if (StringUtils.isNotBlank(error)) {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, error));
                return;
            }
            PreviewAnomalyDetectorRequest previewRequest = new PreviewAnomalyDetectorRequest(
                input.getDetector(),
                input.getDetectorId(),
                input.getPeriodStart(),
                input.getPeriodEnd()
            );
            client.execute(PreviewAnomalyDetectorAction.INSTANCE, previewRequest, new RestToXContentListener<>(channel));
        };
    }

    private AnomalyDetectorExecutionInput getAnomalyDetectorExecutionInput(RestRequest request) throws IOException {
        String detectorId = null;
        if (request.hasParam(RestHandlerUtils.DETECTOR_ID)) {
            detectorId = request.param(RestHandlerUtils.DETECTOR_ID);
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        AnomalyDetectorExecutionInput input = AnomalyDetectorExecutionInput.parse(parser, detectorId);
        if (detectorId != null) {
            input.setDetectorId(detectorId);
        }
        return input;
    }

    private String validateAdExecutionInput(AnomalyDetectorExecutionInput input) {
        if (StringUtils.isBlank(input.getDetectorId())) {
            return "Must set anomaly detector id";
        }
        if (input.getPeriodStart() == null || input.getPeriodEnd() == null) {
            return "Must set both period start and end date with epoch of milliseconds";
        }
        if (!input.getPeriodStart().isBefore(input.getPeriodEnd())) {
            return "Period start date should be before end date";
        }
        return null;
    }

    @Override
    public List<RestHandler.Route> routes() {
        return ImmutableList
            .of(
                // preview detector
                new Route(
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s",
                            AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI,
                            RestHandlerUtils.DETECTOR_ID,
                            PREVIEW
                        )
                )
            );
    }
}
