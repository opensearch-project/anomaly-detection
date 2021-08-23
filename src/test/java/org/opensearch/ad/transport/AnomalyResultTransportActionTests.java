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

import static org.opensearch.ad.TestHelpers.randomQuery;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AnomalyResultTransportActionTests extends ADIntegTestCase {
    private static final Logger LOG = LogManager.getLogger(AnomalyResultTransportActionTests.class);

    private String testIndex;
    private Instant testDataTimeStamp;
    private long start;
    private long end;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testIndex = "test_data";
        testDataTimeStamp = Instant.now();
        start = testDataTimeStamp.minus(10, ChronoUnit.MINUTES).toEpochMilli();
        end = testDataTimeStamp.plus(10, ChronoUnit.MINUTES).toEpochMilli();
        ingestTestData();
    }

    private void ingestTestData() throws IOException, InterruptedException {
        Thread.sleep(1000); // sleep some time to build AD version hash ring
        createTestDataIndex(testIndex);
        double value = randomDouble();
        String type = randomAlphaOfLength(5);
        boolean isError = randomBoolean();
        String message = randomAlphaOfLength(10);
        String id = indexDoc(
            testIndex,
            ImmutableMap
                .of(timeField, testDataTimeStamp.toEpochMilli(), "value", value, "type", type, "is_error", isError, "message", message)
        );
        GetResponse doc = getDoc(testIndex, id);
        Map<String, Object> sourceAsMap = doc.getSourceAsMap();
        assertEquals(testDataTimeStamp.toEpochMilli(), sourceAsMap.get(timeField));
        assertEquals(value, sourceAsMap.get("value"));
        assertEquals(type, sourceAsMap.get("type"));
        assertEquals(isError, sourceAsMap.get("is_error"));
        assertEquals(message, sourceAsMap.get("message"));
        createDetectorIndex();
    }

    public void testFeatureQueryWithTermsAggregation() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"terms\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Failed to parse aggregation");
    }

    public void testFeatureWithSumOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"sum\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithSumOfTypeField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"sum\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [sum]");
    }

    public void testFeatureWithMaxOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"max\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithMaxOfTypeField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"max\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [max]");
    }

    public void testFeatureWithMinOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"min\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithMinOfTypeField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"min\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [min]");
    }

    public void testFeatureWithAvgOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"avg\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithAvgOfTypeField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"avg\":{\"field\":\"type\"}}}");
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [avg]");
    }

    public void testFeatureWithCountOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"value_count\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureWithCardinalityOfTextField() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"cardinality\":{\"field\":\"message\"}}}");
        assertErrorMessage(adId, "Text fields are not optimised for operations");
    }

    public void testFeatureQueryWithTermsAggregationForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"terms\":{\"field\":\"type\"}}}", true);
        assertErrorMessage(adId, "Failed to parse aggregation", true);
    }

    public void testFeatureWithSumOfTextFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"sum\":{\"field\":\"message\"}}}", true);
        assertErrorMessage(adId, "Text fields are not optimised for operations", true);
    }

    public void testFeatureWithSumOfTypeFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"sum\":{\"field\":\"type\"}}}", true);
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [sum]", true);
    }

    public void testFeatureWithMaxOfTextFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"max\":{\"field\":\"message\"}}}", true);
        assertErrorMessage(adId, "Text fields are not optimised for operations", true);
    }

    public void testFeatureWithMaxOfTypeFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"max\":{\"field\":\"type\"}}}", true);
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [max]", true);
    }

    public void testFeatureWithMinOfTextFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"min\":{\"field\":\"message\"}}}", true);
        assertErrorMessage(adId, "Text fields are not optimised for operations", true);
    }

    public void testFeatureWithMinOfTypeFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"min\":{\"field\":\"type\"}}}", true);
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [min]", true);
    }

    public void testFeatureWithAvgOfTextFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"avg\":{\"field\":\"message\"}}}", true);
        assertErrorMessage(adId, "Text fields are not optimised for operations", true);
    }

    public void testFeatureWithAvgOfTypeFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"avg\":{\"field\":\"type\"}}}", true);
        assertErrorMessage(adId, "Field [type] of type [keyword] is not supported for aggregation [avg]", true);
    }

    public void testFeatureWithCountOfTextFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"value_count\":{\"field\":\"message\"}}}", true);
        assertErrorMessage(adId, "Text fields are not optimised for operations", true);
    }

    public void testFeatureWithCardinalityOfTextFieldForHCDetector() throws IOException {
        String adId = createDetectorWithFeatureAgg("{\"test\":{\"cardinality\":{\"field\":\"message\"}}}", true);
        assertErrorMessage(adId, "Text fields are not optimised for operations", true);
    }

    private String createDetectorWithFeatureAgg(String aggQuery) throws IOException {
        return createDetectorWithFeatureAgg(aggQuery, false);
    }

    private String createDetectorWithFeatureAgg(String aggQuery, boolean hcDetector) throws IOException {
        AggregationBuilder aggregationBuilder = TestHelpers.parseAggregation(aggQuery);
        Feature feature = new Feature(randomAlphaOfLength(5), randomAlphaOfLength(10), true, aggregationBuilder);
        AnomalyDetector detector = hcDetector
            ? randomHCDetector(ImmutableList.of(testIndex), ImmutableList.of(feature))
            : randomDetector(ImmutableList.of(testIndex), ImmutableList.of(feature));
        String adId = createDetector(detector);
        return adId;
    }

    private AnomalyDetector randomDetector(List<String> indices, List<Feature> features) throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            timeField,
            indices,
            features,
            randomQuery("{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"value\"}}]}}"),
            new IntervalTimeConfiguration(OpenSearchRestTestCase.randomLongBetween(1, 5), ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(OpenSearchRestTestCase.randomLongBetween(1, 5), ChronoUnit.MINUTES),
            8,
            null,
            randomInt(),
            Instant.now(),
            null,
            null
        );
    }

    private AnomalyDetector randomHCDetector(List<String> indices, List<Feature> features) throws IOException {
        return new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            timeField,
            indices,
            features,
            randomQuery("{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"value\"}}]}}"),
            new IntervalTimeConfiguration(OpenSearchRestTestCase.randomLongBetween(1, 5), ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(OpenSearchRestTestCase.randomLongBetween(1, 5), ChronoUnit.MINUTES),
            8,
            null,
            randomInt(),
            Instant.now(),
            ImmutableList.of(categoryField),
            null
        );
    }

    private void assertErrorMessage(String adId, String errorMessage, boolean hcDetector) {
        AnomalyResultRequest resultRequest = new AnomalyResultRequest(adId, start, end);
        // wait at most 20 seconds
        int numberofTries = 40;
        Exception e = null;
        if (hcDetector) {
            while (numberofTries-- > 0) {
                try {
                    // HCAD records failures asynchronously. Before a failure is recorded, HCAD returns immediately without failure.
                    client().execute(AnomalyResultAction.INSTANCE, resultRequest).actionGet(30_000);
                    Thread.sleep(500);
                } catch (Exception exp) {
                    e = exp;
                    break;
                }
            }
        } else {
            e = expectThrowsAnyOf(
                ImmutableList.of(NotSerializableExceptionWrapper.class, AnomalyDetectionException.class),
                () -> client().execute(AnomalyResultAction.INSTANCE, resultRequest).actionGet(30_000)
            );
        }
        assertTrue("Unexpected error: " + e.getMessage(), ExceptionUtil.getErrorMessage(e).contains(errorMessage));
    }

    private void assertErrorMessage(String adId, String errorMessage) {
        assertErrorMessage(adId, errorMessage, false);
    }
}
