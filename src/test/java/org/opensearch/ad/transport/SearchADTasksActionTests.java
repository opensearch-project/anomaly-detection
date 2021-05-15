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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.opensearch.ad.TestHelpers.matchAllRequest;

import java.io.IOException;

import org.junit.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.HistoricalDetectorIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;

public class SearchADTasksActionTests extends HistoricalDetectorIntegTestCase {

    @Test
    public void testSearchADTasksAction() throws IOException {
        createDetectionStateIndex();
        String adTaskId = createADTask(TestHelpers.randomAdTask());

        SearchResponse searchResponse = client().execute(SearchADTasksAction.INSTANCE, matchAllRequest()).actionGet(10000);
        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);
        assertEquals(adTaskId, searchResponse.getInternalResponse().hits().getAt(0).getId());
    }

    @Test
    public void testNoIndex() {
        deleteIndexIfExists(CommonName.DETECTION_STATE_INDEX);
        SearchResponse searchResponse = client().execute(SearchADTasksAction.INSTANCE, matchAllRequest()).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value);
    }

}
