/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;

import com.google.common.collect.ImmutableList;

public class SearchTopAnomalyResultTransportActionTests extends ADIntegTestCase {

    // The majority of the test cases are covered in the REST-level tests for the
    // search top anomaly result API. We are limited in what we can test at the transport
    // layer due to the lang-painless module not being installed on the test clusters
    // brought up from running OpenSearchIntegTestCases (which this class extends).
    public void testSearchOnNonExistingResultIndex() throws IOException {
        deleteIndexIfExists(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        String testIndexName = randomAlphaOfLength(10).toLowerCase();
        ImmutableList<String> categoryFields = ImmutableList.of("test-field-1", "test-field-2");
        String detectorId = createDetector(
            TestHelpers
                .randomAnomalyDetector(
                    ImmutableList.of(testIndexName),
                    ImmutableList.of(TestHelpers.randomFeature(true)),
                    null,
                    Instant.now(),
                    1,
                    false,
                    categoryFields
                )
        );
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            null,
            false,
            1,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(10, ChronoUnit.DAYS),
            Instant.now()
        );
        SearchTopAnomalyResultResponse searchResponse = client()
            .execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest)
            .actionGet(10_000);
        assertEquals(searchResponse.getAnomalyResultBuckets().size(), 0);
    }
}
