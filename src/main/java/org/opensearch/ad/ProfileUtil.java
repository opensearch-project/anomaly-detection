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

package org.opensearch.ad;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.constant.CommonName;

public class ProfileUtil {
    /**
     * Create search request to check if we have at least 1 anomaly score larger than 0 after AD job enabled time.
     * Note this function is only meant to check for status of real time analysis.
     *
     * @param detectorId detector id
     * @param enabledTime the time when AD job is enabled in milliseconds
     * @return the search request
     */
    private static SearchRequest createRealtimeInittedEverRequest(String detectorId, long enabledTime, String resultIndex) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
        filterQuery.filter(QueryBuilders.rangeQuery(CommonName.EXECUTION_END_TIME_FIELD).gte(enabledTime));
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

    public static void confirmDetectorRealtimeInitStatus(
        AnomalyDetector detector,
        long enabledTime,
        Client client,
        ActionListener<SearchResponse> listener
    ) {
        SearchRequest searchLatestResult = createRealtimeInittedEverRequest(detector.getId(), enabledTime, detector.getCustomResultIndex());
        client.search(searchLatestResult, listener);
    }
}
