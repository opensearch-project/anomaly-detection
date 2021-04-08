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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.matchAllRequest;

import java.io.IOException;

import org.junit.Test;
import org.opensearch.action.search.SearchResponse;

import com.amazon.opendistroforelasticsearch.ad.HistoricalDetectorIntegTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;

public class SearchAnomalyResultActionTests extends HistoricalDetectorIntegTestCase {

    @Test
    public void testSearchResultAction() throws IOException {
        createADResultIndex();
        String adResultId = createADResult(TestHelpers.randomAnomalyDetectResult());

        SearchResponse searchResponse = client().execute(SearchAnomalyResultAction.INSTANCE, matchAllRequest()).actionGet(10000);
        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);

        assertEquals(adResultId, searchResponse.getInternalResponse().hits().getAt(0).getId());
    }

    @Test
    public void testNoIndex() {
        deleteIndexIfExists(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        SearchResponse searchResponse = client().execute(SearchAnomalyResultAction.INSTANCE, matchAllRequest()).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value);
    }

}
