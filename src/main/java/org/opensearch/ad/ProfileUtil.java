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

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.client.Client;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

public class ProfileUtil {
    /**
     * Create search request to check if we have at least 1 anomaly score larger than 0 after AD job enabled time
     * @param detectorId detector id
     * @param enabledTime the time when AD job is enabled in milliseconds
     * @return the search request
     */
    private static SearchRequest createInittedEverRequest(String detectorId, long enabledTime, String resultIndex) {
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        filterQuery.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
        filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(enabledTime));
        filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.ANOMALY_SCORE_FIELD).gt(0));

        SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1);

        SearchRequest request = new SearchRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        request.source(source);
        if (resultIndex != null) {
            request.indices(resultIndex);
        }
        return request;
    }

    public static void confirmDetectorInitStatus(
        AnomalyDetector detector,
        long enabledTime,
        Client client,
        ActionListener<SearchResponse> listener
    ) {
        SearchRequest searchLatestResult = createInittedEverRequest(detector.getDetectorId(), enabledTime, detector.getResultIndex());
        client.search(searchLatestResult, listener);
    }
}
