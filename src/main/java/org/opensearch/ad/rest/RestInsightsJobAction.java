/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.ad.rest;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;
import static org.opensearch.timeseries.util.RestHandlerUtils.DETECTOR_ID;
import static org.opensearch.timeseries.util.RestHandlerUtils.FREQUENCY;
import static org.opensearch.timeseries.util.RestHandlerUtils.FROM;
import static org.opensearch.timeseries.util.RestHandlerUtils.INDEX;
import static org.opensearch.timeseries.util.RestHandlerUtils.INSIGHTS_RESULTS;
import static org.opensearch.timeseries.util.RestHandlerUtils.INSIGHTS_START;
import static org.opensearch.timeseries.util.RestHandlerUtils.INSIGHTS_STATUS;
import static org.opensearch.timeseries.util.RestHandlerUtils.INSIGHTS_STOP;
import static org.opensearch.timeseries.util.RestHandlerUtils.SIZE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.InsightsJobAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.rest.RestJobAction;
import org.opensearch.timeseries.transport.InsightsJobRequest;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to handle request to start, get results, check status, and stop insights job.
 * POST /_plugins/_anomaly_detection/insights/_start - Start insights job
 * GET /_plugins/_anomaly_detection/insights/_status - Get insights job status
 * GET /_plugins/_anomaly_detection/insights/_results - Get latest insights results
 * POST /_plugins/_anomaly_detection/insights/_stop - Stop insights job
 */
public class RestInsightsJobAction extends RestJobAction {
    public static final String INSIGHTS_JOB_ACTION = "insights_job_action";
    private volatile TimeValue requestTimeout;
    private volatile boolean insightsEnabled;

    public RestInsightsJobAction(Settings settings, ClusterService clusterService) {
        this.requestTimeout = AD_REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_REQUEST_TIMEOUT, it -> requestTimeout = it);
        this.insightsEnabled = AnomalyDetectorSettings.INSIGHTS_ENABLED.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AnomalyDetectorSettings.INSIGHTS_ENABLED, it -> insightsEnabled = it);
    }

    @Override
    public String getName() {
        return INSIGHTS_JOB_ACTION;
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList
            .of(
                // Start insights job
                new ReplacedRoute(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/insights/%s", TimeSeriesAnalyticsPlugin.AD_BASE_URI, INSIGHTS_START),
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/insights/%s", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, INSIGHTS_START)
                ),
                // Get insights job status
                new ReplacedRoute(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/insights/%s", TimeSeriesAnalyticsPlugin.AD_BASE_URI, INSIGHTS_STATUS),
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/insights/%s", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, INSIGHTS_STATUS)
                ),
                // Stop insights job
                new ReplacedRoute(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/insights/%s", TimeSeriesAnalyticsPlugin.AD_BASE_URI, INSIGHTS_STOP),
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/insights/%s", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, INSIGHTS_STOP)
                ),
                // Get insights results
                new ReplacedRoute(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/insights/%s", TimeSeriesAnalyticsPlugin.AD_BASE_URI, INSIGHTS_RESULTS),
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/insights/%s", TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, INSIGHTS_RESULTS)
                )
            );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }

        if (!insightsEnabled) {
            throw new IllegalStateException(
                "Insights feature is disabled. Enable it via cluster setting 'plugins.anomaly_detection.insights_enabled'."
            );
        }

        String rawPath = request.rawPath();
        InsightsJobRequest insightsJobRequest;

        if (rawPath.contains(INSIGHTS_START)) {
            insightsJobRequest = parseStartRequest(request, rawPath);
        } else if (rawPath.contains(INSIGHTS_STATUS)) {
            insightsJobRequest = new InsightsJobRequest(rawPath);
        } else if (rawPath.contains(INSIGHTS_RESULTS)) {
            String detectorId = request.param(DETECTOR_ID);
            String index = request.param(INDEX);
            int from = request.paramAsInt(FROM, 0);
            int size = request.paramAsInt(SIZE, 20);

            insightsJobRequest = new InsightsJobRequest(detectorId, index, from, size, rawPath);
        } else if (rawPath.contains(INSIGHTS_STOP)) {
            insightsJobRequest = new InsightsJobRequest(rawPath);
        } else {
            throw new IllegalArgumentException("Invalid request path: " + rawPath);
        }

        return channel -> client.execute(InsightsJobAction.INSTANCE, insightsJobRequest, new RestToXContentListener<>(channel));
    }

    private InsightsJobRequest parseStartRequest(RestRequest request, String rawPath) throws IOException {
        // Default frequency is 24 hours
        String frequency = "24h";

        if (request.hasContent()) {
            XContentParser parser = request.contentParser();
            XContentParser.Token token;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();

                    if (FREQUENCY.equals(fieldName)) {
                        frequency = parser.text();
                    }
                }
            }
        }

        return new InsightsJobRequest(frequency, rawPath);
    }
}
