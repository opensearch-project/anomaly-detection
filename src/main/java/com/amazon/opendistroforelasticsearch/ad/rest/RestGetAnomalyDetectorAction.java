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

import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.ENTITY;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.PROFILE;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.TYPE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestToXContentListener;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.transport.GetAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.GetAnomalyDetectorRequest;
import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to retrieve an anomaly detector.
 */
public class RestGetAnomalyDetectorAction extends BaseRestHandler {

    private static final String GET_ANOMALY_DETECTOR_ACTION = "get_anomaly_detector";
    private static final Logger logger = LogManager.getLogger(RestGetAnomalyDetectorAction.class);

    public RestGetAnomalyDetectorAction() {}

    @Override
    public String getName() {
        return GET_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        String detectorId = request.param(DETECTOR_ID);
        String typesStr = request.param(TYPE);
        String entityValue = request.param(ENTITY);
        String rawPath = request.rawPath();
        boolean returnJob = request.paramAsBoolean("job", false);
        boolean returnTask = request.paramAsBoolean("task", false);
        boolean all = request.paramAsBoolean("_all", false);
        GetAnomalyDetectorRequest getAnomalyDetectorRequest = new GetAnomalyDetectorRequest(
            detectorId,
            RestActions.parseVersion(request),
            returnJob,
            returnTask,
            typesStr,
            rawPath,
            all,
            entityValue
        );

        return channel -> client
            .execute(GetAnomalyDetectorAction.INSTANCE, getAnomalyDetectorRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        String path = String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID);
        return ImmutableList
            .of(
                new Route(RestRequest.Method.GET, path),
                new Route(RestRequest.Method.HEAD, path),
                new Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE)
                ),
                // types is a profile names. See a complete list of supported profiles names in
                // com.amazon.opendistroforelasticsearch.ad.model.ProfileName.
                new Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}/%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE, TYPE)
                )
            );
    }
}
