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

import static org.opensearch.ad.indices.ADIndexManagement.ALL_AD_RESULTS_INDEX_PATTERN;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.transport.DeleteAnomalyResultsAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to delete anomaly result with specific query.
 * Currently AD dashboard plugin doesn't call this API. User can use this API to delete
 * anomaly results to free up disk space.
 *
 * User needs to delete anomaly result from custom result index by themselves as they
 * can directly access these custom result index.
 * Same strategy for custom result index rollover. Suggest user using ISM plugin to
 * manage custom result index.
 *
 * TODO: build better user experience to reduce user's effort to maintain custom result index.
 */
public class RestDeleteAnomalyResultsAction extends BaseRestHandler {

    private static final String DELETE_AD_RESULTS_ACTION = "delete_anomaly_results";
    private static final Logger logger = LogManager.getLogger(RestDeleteAnomalyResultsAction.class);

    public RestDeleteAnomalyResultsAction() {}

    @Override
    public String getName() {
        return DELETE_AD_RESULTS_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(ALL_AD_RESULTS_INDEX_PATTERN)
            .setQuery(searchSourceBuilder.query())
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN);
        return channel -> client.execute(DeleteAnomalyResultsAction.INSTANCE, deleteRequest, ActionListener.wrap(r -> {
            XContentBuilder contentBuilder = r.toXContent(channel.newBuilder().startObject(), ToXContent.EMPTY_PARAMS);
            contentBuilder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, contentBuilder));
        }, e -> {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (IOException exception) {
                logger.error("Failed to send back delete anomaly result exception result", exception);
            }
        }));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(RestRequest.Method.DELETE, TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI + "/results"));
    }
}
