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

package com.amazon.opendistroforelasticsearch.ad;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.aggregations.AggregationBuilder;

import com.amazon.opendistroforelasticsearch.ad.mock.model.MockSimpleLog;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public abstract class HistoricalDetectorRestTestCase extends AnomalyDetectorRestTestCase {

    protected String historicalDetectorTestIndex = "test_historical_detector_data";
    protected int detectionIntervalInMinutes = 1;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1);
        // ingest test data
        ingestTestDataForHistoricalDetector(historicalDetectorTestIndex, detectionIntervalInMinutes);
    }

    public ToXContentObject[] getHistoricalAnomalyDetector(String detectorId, boolean returnTask, RestClient client) throws IOException {
        BasicHeader header = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        return getAnomalyDetector(detectorId, header, false, returnTask, client);
    }

    public ADTaskProfile getADTaskProfile(String detectorId) throws IOException {
        Response profileResponse = TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/_profile/ad_task",
                ImmutableMap.of(),
                "",
                null
            );
        return parseADTaskProfile(profileResponse);
    }

    public Response ingestSimpleMockLog(
        String indexName,
        int startDays,
        int totalDoc,
        long intervalInMinutes,
        ToDoubleFunction<Integer> valueFunc,
        ToIntFunction<Integer> categoryFunc
    ) throws IOException {
        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                indexName,
                null,
                toHttpEntity(MockSimpleLog.INDEX_MAPPING),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );

        Response statsResponse = TestHelpers.makeRequest(client(), "GET", indexName, ImmutableMap.of(), "", null);
        assertEquals(RestStatus.OK, restStatus(statsResponse));
        String result = EntityUtils.toString(statsResponse.getEntity());
        assertTrue(result.contains(indexName));

        StringBuilder bulkRequestBuilder = new StringBuilder();
        Instant startTime = Instant.now().minus(startDays, ChronoUnit.DAYS);
        for (int i = 0; i < totalDoc; i++) {
            bulkRequestBuilder.append("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"" + i + "\" } }\n");
            MockSimpleLog simpleLog = new MockSimpleLog(
                startTime,
                valueFunc.applyAsDouble(i),
                "category" + categoryFunc.applyAsInt(i),
                randomBoolean(),
                randomAlphaOfLength(5)
            );
            bulkRequestBuilder.append(TestHelpers.toJsonString(simpleLog));
            bulkRequestBuilder.append("\n");
            startTime = startTime.plus(intervalInMinutes, ChronoUnit.MINUTES);
        }
        Response bulkResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                "_bulk?refresh=true",
                null,
                toHttpEntity(bulkRequestBuilder.toString()),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
        return bulkResponse;
    }

    public ADTaskProfile parseADTaskProfile(Response profileResponse) throws IOException {
        String profileResult = EntityUtils.toString(profileResponse.getEntity());
        XContentParser parser = TestHelpers.parser(profileResult);
        ADTaskProfile adTaskProfile = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if ("ad_task".equals(fieldName)) {
                adTaskProfile = ADTaskProfile.parse(parser);
            } else {
                parser.skipChildren();
            }
        }
        return adTaskProfile;
    }

    protected void ingestTestDataForHistoricalDetector(String indexName, int detectionIntervalInMinutes) throws IOException {
        ingestSimpleMockLog(indexName, 10, 3000, detectionIntervalInMinutes, (i) -> {
            if (i % 500 == 0) {
                return randomDoubleBetween(100, 1000, true);
            } else {
                return randomDoubleBetween(1, 10, true);
            }
        }, (i) -> 1);
    }

    protected AnomalyDetector createHistoricalDetector() throws IOException {
        AggregationBuilder aggregationBuilder = TestHelpers
            .parseAggregation("{\"test\":{\"max\":{\"field\":\"" + MockSimpleLog.VALUE_FIELD + "\"}}}");
        Feature feature = new Feature(randomAlphaOfLength(5), randomAlphaOfLength(10), true, aggregationBuilder);
        Instant endTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Instant startTime = endTime.minus(10, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                dateRange,
                ImmutableList.of(feature),
                historicalDetectorTestIndex,
                detectionIntervalInMinutes,
                MockSimpleLog.TIME_FIELD
            );
        return createAnomalyDetector(detector, true, client());
    }

    protected String startHistoricalDetector(String detectorId) throws IOException {
        Response startDetectorResponse = startAnomalyDetector(detectorId, client());
        Map<String, Object> startDetectorResponseMap = responseAsMap(startDetectorResponse);
        String taskId = (String) startDetectorResponseMap.get("_id");
        assertNotNull(taskId);
        return taskId;
    }

    protected ADTaskProfile waitUntilGetTaskProfile(String detectorId) throws InterruptedException {
        int i = 0;
        ADTaskProfile adTaskProfile = null;
        while (adTaskProfile == null && i < 200) {
            try {
                adTaskProfile = getADTaskProfile(detectorId);
            } catch (Exception e) {} finally {
                Thread.sleep(100);
            }
            i++;
        }
        assertNotNull(adTaskProfile);
        return adTaskProfile;
    }

    protected ADTaskProfile waitUntilTaskFinished(String detectorId) throws InterruptedException {
        int i = 0;
        ADTaskProfile adTaskProfile = null;
        while ((adTaskProfile == null || TestHelpers.historicalDetectorRunningStats.contains(adTaskProfile.getAdTask().getState()))
            && i < 30) {
            try {
                adTaskProfile = getADTaskProfile(detectorId);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                Thread.sleep(1000);
            }
            i++;
        }
        assertNotNull(adTaskProfile);
        return adTaskProfile;
    }
}
