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

import static org.opensearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static org.opensearch.ad.util.RestHandlerUtils.PROFILE;
import static org.opensearch.ad.util.RestHandlerUtils.TYPE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.constant.CommonName;

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
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }
        String detectorId = request.param(DETECTOR_ID);
        String typesStr = request.param(TYPE);

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
            buildEntity(request, detectorId)
        );

        return channel -> client
            .execute(GetAnomalyDetectorAction.INSTANCE, getAnomalyDetectorRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // Opensearch-only API. Considering users may provide entity in the search body, support POST as well.
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE)
                ),
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE, TYPE)
                )
            );
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        String path = String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID);
        String newPath = String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID);
        return ImmutableList
            .of(
                new ReplacedRoute(RestRequest.Method.GET, newPath, RestRequest.Method.GET, path),
                new ReplacedRoute(RestRequest.Method.HEAD, newPath, RestRequest.Method.HEAD, path),
                new ReplacedRoute(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE),
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID, PROFILE)
                ),
                // types is a profile names. See a complete list of supported profiles names in
                // org.opensearch.ad.model.ProfileName.
                new ReplacedRoute(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}/%s/{%s}", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, PROFILE, TYPE),
                    RestRequest.Method.GET,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s/{%s}",
                            AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI,
                            DETECTOR_ID,
                            PROFILE,
                            TYPE
                        )
                )
            );
    }

    private Entity buildEntity(RestRequest request, String detectorId) throws IOException {
        if (Strings.isEmpty(detectorId)) {
            throw new IllegalStateException(ADCommonMessages.AD_ID_MISSING_MSG);
        }

        String entityName = request.param(ADCommonName.CATEGORICAL_FIELD);
        String entityValue = request.param(CommonName.ENTITY_KEY);

        if (entityName != null && entityValue != null) {
            // single-stream profile request:
            // GET _plugins/_anomaly_detection/detectors/<detectorId>/_profile/init_progress?category_field=<field-name>&entity=<value>
            return Entity.createSingleAttributeEntity(entityName, entityValue);
        } else if (request.hasContent()) {
            /* HCAD profile request:
             * GET _plugins/_anomaly_detection/detectors/<detectorId>/_profile/init_progress
             * {
             *     "entity": [{
             *         "name": "clientip",
             *         "value": "13.24.0.0"
             *      }]
             * }
             */
            Optional<Entity> entity = Entity.fromJsonObject(request.contentParser());
            if (entity.isPresent()) {
                return entity.get();
            }
        }
        // not a valid profile request with correct entity information
        return null;
    }
}
