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


package org.opensearch.ad.rest;

import com.google.common.collect.ImmutableList;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.ValidateAnomalyDetectorAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorRequest;

import static org.opensearch.ad.util.RestHandlerUtils.*;
import static org.opensearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class RestValidateAnomalyDetectorAction extends AbstractAnomalyDetectorAction {
    private static final String VALIDATE_ANOMALY_DETECTOR_ACTION = "validate_anomaly_detector_action";

    public RestValidateAnomalyDetectorAction(Settings settings, ClusterService clusterService) {
        super(settings, clusterService);
    }

    @Override
    public String getName() {
        return VALIDATE_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList
                .of(
                        // validate detector
                        new ReplacedRoute(
                                RestRequest.Method.POST,
                                String.format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, VALIDATE),
                                RestRequest.Method.POST,
                                String.format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, VALIDATE)
                        ),
                        // validate detector with type
                        new ReplacedRoute(
                                RestRequest.Method.POST,
                                String.format(Locale.ROOT, "%s/%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, VALIDATE, TYPE),
                                RestRequest.Method.POST,
                                String.format(Locale.ROOT, "%s/%s/{%s}", AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, VALIDATE, TYPE)
                        )
                );
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        AnomalyDetector detector = AnomalyDetector.parse(parser);
        String typesStr = request.param(TYPE);

        ValidateAnomalyDetectorRequest validateAnomalyDetectorRequest = new ValidateAnomalyDetectorRequest(
                detector,
                typesStr,
                maxSingleEntityDetectors,
                maxMultiEntityDetectors,
                maxAnomalyFeatures,
                requestTimeout
        );
        return channel -> client
                .execute(ValidateAnomalyDetectorAction.INSTANCE, validateAnomalyDetectorRequest, new RestToXContentListener<>(channel));
    }
}
