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

package org.opensearch.ad.transport;

import org.junit.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.HistoricalAnalysisIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.time.Instant;
import java.util.List;

import static org.opensearch.ad.TestHelpers.matchAllRequest;

public class SearchTopAnomalyResultActionTests extends ADIntegTestCase {

//    @Test
//    public void testSearchResultAction() throws IOException {
//        createADResultIndex();
//        String adResultId = createADResult(TestHelpers.randomAnomalyDetectResult());
//
//        SearchResponse searchResponse = client().execute(SearchAnomalyResultAction.INSTANCE, matchAllRequest()).actionGet(10000);
//        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);
//
//        assertEquals(adResultId, searchResponse.getInternalResponse().hits().getAt(0).getId());
//    }

    @Test
    public void testNoIndex() {
        deleteIndexIfExists(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        List<String> categoryFields = new ArrayList<>(Arrays.asList("test-category-field"));
        SearchTopAnomalyResultRequest req = new SearchTopAnomalyResultRequest(
                "test-detector-id",
                "test-task-id",
                false,
                10,
                categoryFields,
                "severity",
                Instant.now().minusMillis(100_000),
                Instant.now()
                );
        SearchTopAnomalyResultResponse response = client().execute(SearchTopAnomalyResultAction.INSTANCE, req).actionGet(10000);
        assertEquals(0, response.getAnomalyResultBuckets().size());
    }

}
