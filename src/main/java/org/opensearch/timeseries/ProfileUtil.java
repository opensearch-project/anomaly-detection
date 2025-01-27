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

package org.opensearch.timeseries;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Config;
import org.opensearch.transport.client.Client;

public class ProfileUtil {
    /**
     * Create search request to check if we have at least 1 anomaly score larger than 0 after start time.
     * For example, when startTime is AD job enabled time, we check if model has been initialized ever.
     * Note this function is only meant to check for status of real time analysis.
     *
     * @param detectorId detector id
     * @param startTime startTime the time when we start searching for result (e.g., enabledTime: the time when AD job is enabled in milliseconds)
     * @return the search request
     */
    private static SearchRequest createADRealtimeResultRequest(String detectorId, long startTime, String resultIndex) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
        filterQuery.filter(QueryBuilders.rangeQuery(CommonName.EXECUTION_END_TIME_FIELD).gte(startTime));
        filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.ANOMALY_SCORE_FIELD).gt(0));
        // Historical analysis result also stored in result index, which has non-null task_id.
        // For realtime detection result, we should filter task_id == null
        ExistsQueryBuilder taskIdExistsFilter = QueryBuilders.existsQuery(CommonName.TASK_ID_FIELD);
        filterQuery.mustNot(taskIdExistsFilter);

        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1);

        SearchRequest request = new SearchRequest(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
        request.source(source);
        if (resultIndex != null) {
            request.indices(resultIndex);
        }
        return request;
    }

    /**
     * Create search request to check if we have at least 1 forecast after start time.
     * For example, when startTime is forecast, job enabled time, we check if model has been initialized ever.
     * Note this function is only meant to check for status of real time analysis.
     *
     * @param forecasterId forecaster id
     * @param startTime the time when we start searching for result (e.g., enabledTime: the time when forecast job is enabled in milliseconds)
     * @return the search request
     */
    private static SearchRequest createForecastRealtimeResultEverRequest(String forecasterId, long startTime, String resultIndex) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(ForecastCommonName.FORECASTER_ID_KEY, forecasterId));
        filterQuery.filter(QueryBuilders.rangeQuery(CommonName.EXECUTION_END_TIME_FIELD).gte(startTime));
        ExistsQueryBuilder forecastsExistFilter = QueryBuilders.existsQuery(ForecastResult.VALUE_FIELD);
        filterQuery.must(forecastsExistFilter);
        // Historical/run-once analysis result also stored in result index, which has non-null task_id.
        // For realtime detection result, we should filter task_id == null
        ExistsQueryBuilder taskIdExistsFilter = QueryBuilders.existsQuery(CommonName.TASK_ID_FIELD);
        filterQuery.mustNot(taskIdExistsFilter);

        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1);

        SearchRequest request = new SearchRequest(ForecastIndex.RESULT.getIndexName());
        request.source(source);
        if (resultIndex != null) {
            request.indices(resultIndex);
        }
        return request;
    }

    public static void confirmRealtimeResultStatus(
        Config config,
        long startTime,
        Client client,
        AnalysisType analysisType,
        ActionListener<SearchResponse> listener
    ) {
        SearchRequest searchLatestResult = null;
        if (analysisType.isAD()) {
            searchLatestResult = createADRealtimeResultRequest(config.getId(), startTime, config.getCustomResultIndexPattern());
        } else if (analysisType.isForecast()) {
            searchLatestResult = createForecastRealtimeResultEverRequest(config.getId(), startTime, config.getCustomResultIndexPattern());
        } else {
            throw new IllegalArgumentException("Analysis type is not supported, type: : " + analysisType);
        }

        client.search(searchLatestResult, listener);
    }
}
