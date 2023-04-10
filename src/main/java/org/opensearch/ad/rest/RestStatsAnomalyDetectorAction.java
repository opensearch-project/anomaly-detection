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

import static org.opensearch.ad.util.RestHandlerUtils.NODE_ID;
import static org.opensearch.ad.util.RestHandlerUtils.STAT;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.transport.ADStatsRequest;
import org.opensearch.ad.transport.StatsAnomalyDetectorAction;
import org.opensearch.ad.transport.StatsAnomalyDetectorResponse;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;
import org.opensearch.sdk.SDKClient.SDKRestClient;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

/**
 * RestStatsAnomalyDetectorAction consists of the REST handler to get the stats from the anomaly detector extension.
 */
public class RestStatsAnomalyDetectorAction extends BaseExtensionRestHandler {

    private static final String STATS_ANOMALY_DETECTOR_ACTION = "stats_anomaly_detector";
    private final Logger logger = LogManager.getLogger(RestStatsAnomalyDetectorAction.class);
    @Inject
    private ADStats adStats;
    private DiscoveryNodeFilterer nodeFilter;
    private SDKRestClient sdkRestClient;
    private Settings settings;
    @Inject
    private DiscoveryNodeFilterer discoveryNodeFilterer;

    public RestStatsAnomalyDetectorAction(ExtensionsRunner extensionsRunner, SDKRestClient sdkRestClient) {
        this.sdkRestClient = sdkRestClient;
        this.settings = extensionsRunner.getEnvironmentSettings();
    }

    public String getName() {
        return STATS_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    public List<RouteHandler> routeHandlers() {
        return ImmutableList
            .of(
                new RouteHandler(RestRequest.Method.GET, String.format(Locale.ROOT, "/{%s}/%s", NODE_ID, "stats"), handleRequest),
                new RouteHandler(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "/{%s}/%s/{%s}", NODE_ID, "stats", STAT),
                    handleRequest
                ),
                new RouteHandler(RestRequest.Method.GET, "/stats", handleRequest),
                new RouteHandler(RestRequest.Method.GET, String.format(Locale.ROOT, "/%s/{%s}", "stats", STAT), handleRequest)
            );
    }

    private Function<RestRequest, ExtensionRestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            // TODO: handle the AD-specific exceptions separately
            return exceptionalRequest(request, e);
        }
    };

    protected ExtensionRestResponse prepareRequest(RestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        ADStatsRequest adStatsRequest = getRequest(request);
        CompletableFuture<StatsAnomalyDetectorResponse> statsFutureResponse = new CompletableFuture<>();
        sdkRestClient
            .execute(
                StatsAnomalyDetectorAction.INSTANCE,
                adStatsRequest,
                ActionListener.wrap(response -> statsFutureResponse.complete(response), ex -> statsFutureResponse.completeExceptionally(ex))
            );

        StatsAnomalyDetectorResponse statsAnomalyDetectorResponse = statsFutureResponse
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
            .join();

        XContentBuilder statsAnomalyDetectorResponseBuilder = statsAnomalyDetectorResponse
            .toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
        ExtensionRestResponse response = new ExtensionRestResponse(request, RestStatus.OK, statsAnomalyDetectorResponseBuilder);

        return response;
    }

    /**
     * Creates a ADStatsRequest from a RestRequest
     *
     * @param request RestRequest
     * @return ADStatsRequest Request containing stats to be retrieved
     */
    private ADStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query the stats for
        String nodesIdsStr = request.param(NODE_ID);
        Set<String> validStats = adStats.getStats().keySet();

        ADStatsRequest adStatsRequest;
        if (!Strings.isEmpty(nodesIdsStr)) {
            String[] nodeIdsArr = nodesIdsStr.split(",");
            adStatsRequest = new ADStatsRequest(nodeIdsArr);
        } else {
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            adStatsRequest = new ADStatsRequest(dataNodes);
        }

        adStatsRequest.timeout(request.param("timeout"));

        // parse the stats the user wants to see
        HashSet<String> statsSet = null;
        String statsStr = request.param("stat");
        if (!Strings.isEmpty(statsStr)) {
            statsSet = new HashSet<>(Arrays.asList(statsStr.split(",")));
        }

        if (statsSet == null) {
            adStatsRequest.addAll(validStats); // retrieve all stats if none are specified
        } else if (statsSet.size() == 1 && statsSet.contains(ADStatsRequest.ALL_STATS_KEY)) {
            adStatsRequest.addAll(validStats);
        } else if (statsSet.contains(ADStatsRequest.ALL_STATS_KEY)) {
            throw new IllegalArgumentException(
                "Request " + request.path() + " contains " + ADStatsRequest.ALL_STATS_KEY + " and individual stats"
            );
        } else {
            Set<String> invalidStats = new TreeSet<>();
            for (String stat : statsSet) {
                if (validStats.contains(stat)) {
                    adStatsRequest.addStat(stat);
                } else {
                    invalidStats.add(stat);
                }
            }

            if (!invalidStats.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidStats, adStatsRequest.getStatsToBeRetrieved(), STAT));
            }
        }
        return adStatsRequest;
    }

}
