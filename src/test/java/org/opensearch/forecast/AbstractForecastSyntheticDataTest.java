/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.FORECAST_BASE_URI;
import static org.opensearch.timeseries.util.RestHandlerUtils.COUNT;
import static org.opensearch.timeseries.util.RestHandlerUtils.MATCH;
import static org.opensearch.timeseries.util.RestHandlerUtils.PROFILE;
import static org.opensearch.timeseries.util.RestHandlerUtils.RESULTS;
import static org.opensearch.timeseries.util.RestHandlerUtils.RUN_ONCE;
import static org.opensearch.timeseries.util.RestHandlerUtils.SEARCH;
import static org.opensearch.timeseries.util.RestHandlerUtils.START_JOB;
import static org.opensearch.timeseries.util.RestHandlerUtils.STATS;
import static org.opensearch.timeseries.util.RestHandlerUtils.STOP_JOB;
import static org.opensearch.timeseries.util.RestHandlerUtils.SUGGEST;
import static org.opensearch.timeseries.util.RestHandlerUtils.TOP_FORECASTS;
import static org.opensearch.timeseries.util.RestHandlerUtils.VALIDATE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.ForecastTaskProfile;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.search.SearchHit;
import org.opensearch.timeseries.AbstractSyntheticDataTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.Config;

import com.google.common.collect.ImmutableMap;

public class AbstractForecastSyntheticDataTest extends AbstractSyntheticDataTest {
    public static final int MAX_RETRY_TIMES = 200;
    protected static final String SUGGEST_INTERVAL_URI;
    protected static final String SUGGEST_INTERVAL_HORIZON_HISTORY_URI;
    protected static final String SUGGEST_INTERVAL_HORIZON_HISTORY_DELAY_URI;
    protected static final String VALIDATE_FORECASTER;
    protected static final String VALIDATE_FORECASTER_MODEL;
    protected static final String CREATE_FORECASTER;
    protected static final String RUN_ONCE_FORECASTER;
    protected static final String START_FORECASTER;
    protected static final String STOP_FORECASTER;
    protected static final String UPDATE_FORECASTER;
    protected static final String SEARCH_RESULTS;
    protected static final String GET_FORECASTER;
    protected static final String TOP_FORECASTER;
    protected static final String PROFILE_ALL_FORECASTER;
    protected static final String PROFILE_FORECASTER;
    protected static final String STATS_FORECASTER;
    protected static final String NODE_STATS_FORECASTER;
    protected static final String ONE_STAT_FORECASTER;
    protected static final String NODE_ONE_STAT_FORECASTER;
    protected static final String SEARCH_TASK_FORECASTER;
    protected static final String SEARCH_FORECASTER;
    protected static final String DELETE_FORECASTER;
    protected static final String COUNT_FORECASTER;
    protected static final String MATCH_FORECASTER;
    protected static final String SEARCH_INDEX_MATCH_ALL;

    static {
        SUGGEST_INTERVAL_URI = String
            .format(
                Locale.ROOT,
                "%s/%s/%s",
                TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
                SUGGEST,
                Forecaster.FORECAST_INTERVAL_FIELD
            );
        SUGGEST_INTERVAL_HORIZON_HISTORY_URI = String
            .format(
                Locale.ROOT,
                "%s/%s/%s,%s,%s",
                TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
                SUGGEST,
                Forecaster.FORECAST_INTERVAL_FIELD,
                Forecaster.HORIZON_FIELD,
                Config.HISTORY_INTERVAL_FIELD
            );
        SUGGEST_INTERVAL_HORIZON_HISTORY_DELAY_URI = String
            .format(
                Locale.ROOT,
                "%s/%s/%s,%s,%s,%s",
                TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
                SUGGEST,
                Forecaster.FORECAST_INTERVAL_FIELD,
                Forecaster.HORIZON_FIELD,
                Config.HISTORY_INTERVAL_FIELD,
                Config.WINDOW_DELAY_FIELD
            );
        VALIDATE_FORECASTER = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, VALIDATE);
        VALIDATE_FORECASTER_MODEL = String
            .format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, VALIDATE, "model");
        CREATE_FORECASTER = TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI;
        RUN_ONCE_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s", RUN_ONCE);
        START_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s", START_JOB);
        STOP_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s", STOP_JOB);
        UPDATE_FORECASTER = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s");
        SEARCH_RESULTS = "opensearch-forecast-result*/_search";
        GET_FORECASTER = String.format(Locale.ROOT, "%s/%s?task=true", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s");
        TOP_FORECASTER = String
            .format(Locale.ROOT, "%s/%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s", RESULTS, TOP_FORECASTS);
        PROFILE_ALL_FORECASTER = String
            .format(Locale.ROOT, "%s/%s/%s?_all=true&pretty", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s", PROFILE);
        PROFILE_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s", PROFILE);
        STATS_FORECASTER = String.format(Locale.ROOT, "%s/%s", FORECAST_BASE_URI, STATS);
        NODE_STATS_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s", FORECAST_BASE_URI, "%s", STATS);
        ONE_STAT_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s", FORECAST_BASE_URI, STATS, "%s");
        NODE_ONE_STAT_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s/%s", FORECAST_BASE_URI, "%s", STATS, "%s");
        SEARCH_TASK_FORECASTER = String.format(Locale.ROOT, "%s/tasks/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, SEARCH);
        SEARCH_FORECASTER = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, SEARCH);
        DELETE_FORECASTER = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s");
        COUNT_FORECASTER = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, COUNT);
        MATCH_FORECASTER = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, MATCH);
        SEARCH_INDEX_MATCH_ALL = "%s/_search?pretty";
    }

    protected ForecastTaskProfile getForecastTaskProfile(String forecasterId, RestClient client) throws IOException, ParseException {
        Response profileResponse = TestHelpers
            .makeRequest(
                client,
                "GET",
                TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI + "/" + forecasterId + "/_profile/" + ForecastCommonName.FORECAST_TASK,
                ImmutableMap.of(),
                "",
                null
            );
        return parseForecastTaskProfile(profileResponse);
    }

    protected ForecastTaskProfile parseForecastTaskProfile(Response profileResponse) throws IOException, ParseException {
        String profileResult = EntityUtils.toString(profileResponse.getEntity());
        XContentParser parser = TestHelpers.parser(profileResult);
        ForecastTaskProfile forecastTaskProfile = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if ("forecast_task".equals(fieldName)) {
                forecastTaskProfile = ForecastTaskProfile.parse(parser);
            } else {
                parser.skipChildren();
            }
        }
        return forecastTaskProfile;
    }

    protected List<Object> waitUntilTaskReachState(String forecasterId, Set<String> targetStates, RestClient client)
        throws InterruptedException {
        List<Object> results = new ArrayList<>();
        int i = 0;
        ForecastTaskProfile forecastTaskProfile = null;
        // Increase retryTimes if some task can't reach done state
        while ((forecastTaskProfile == null || !targetStates.contains(forecastTaskProfile.getTask().getState())) && i < MAX_RETRY_TIMES) {
            try {
                forecastTaskProfile = getForecastTaskProfile(forecasterId, client);
            } catch (Exception e) {
                logger.error("failed to get ForecastTaskProfile", e);
            } finally {
                Thread.sleep(1000);
            }
            i++;
        }
        assertNotNull(forecastTaskProfile);
        results.add(forecastTaskProfile);
        results.add(i);
        return results;
    }

    protected List<SearchHit> toHits(Response response) throws UnsupportedOperationException, IOException {
        SearchResponse searchResponse = SearchResponse
            .fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
        long total = searchResponse.getHits().getTotalHits().value();
        if (total == 0) {
            return new ArrayList<>();
        }
        return Arrays.asList(searchResponse.getHits().getHits());
    }

}
