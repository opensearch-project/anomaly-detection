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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToDoubleFunction;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.Feature;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.aggregations.AggregationBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public abstract class HistoricalAnalysisRestTestCase extends AnomalyDetectorRestTestCase {

    public static final int MAX_RETRY_TIMES = 200;
    protected String historicalAnalysisTestIndex = "test_historical_analysis_data";
    protected int detectionIntervalInMinutes = 1;
    protected int categoryFieldDocCount = 2;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1);
        // ingest test data
        ingestTestDataForHistoricalAnalysis(historicalAnalysisTestIndex, detectionIntervalInMinutes);
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

    public Response searchTaskResult(String resultIndex, String taskId) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_RESULT_URI + "/_search/" + resultIndex,
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"task_id\":\"" + taskId + "\"}}]}},\"track_total_hits\":true}"
                    ),
                null
            );
        return response;
    }

    public Response ingestSimpleMockLog(
        String indexName,
        int startDays,
        int totalDoc,
        long intervalInMinutes,
        ToDoubleFunction<Integer> valueFunc,
        int ipSize,
        int categorySize
    ) throws IOException {
        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                indexName,
                null,
                TestHelpers.toHttpEntity(MockSimpleLog.INDEX_MAPPING),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );

        Response statsResponse = TestHelpers.makeRequest(client(), "GET", indexName, ImmutableMap.of(), "", null);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(statsResponse));
        String result = EntityUtils.toString(statsResponse.getEntity());
        assertTrue(result.contains(indexName));

        StringBuilder bulkRequestBuilder = new StringBuilder();
        Instant startTime = Instant.now().minus(startDays, ChronoUnit.DAYS);
        for (int i = 0; i < totalDoc; i++) {
            for (int m = 0; m < ipSize; m++) {
                String ip = "192.168.1." + m;
                for (int n = 0; n < categorySize; n++) {
                    String category = "category" + n;
                    String docId = randomAlphaOfLength(10);
                    bulkRequestBuilder.append("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"" + docId + "\" } }\n");
                    MockSimpleLog simpleLog1 = new MockSimpleLog(
                        startTime,
                        valueFunc.applyAsDouble(i),
                        ip,
                        category,
                        randomBoolean(),
                        randomAlphaOfLength(5)
                    );
                    bulkRequestBuilder.append(TestHelpers.toJsonString(simpleLog1));
                    bulkRequestBuilder.append("\n");
                }
            }
            startTime = startTime.plus(intervalInMinutes, ChronoUnit.MINUTES);
        }
        Response bulkResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                "_bulk?refresh=true",
                null,
                TestHelpers.toHttpEntity(bulkRequestBuilder.toString()),
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

    protected void ingestTestDataForHistoricalAnalysis(String indexName, int detectionIntervalInMinutes) throws IOException {
        ingestSimpleMockLog(indexName, 10, 3000, detectionIntervalInMinutes, (i) -> {
            if (i % 500 == 0) {
                return randomDoubleBetween(100, 1000, true);
            } else {
                return randomDoubleBetween(1, 10, true);
            }
        }, categoryFieldDocCount, categoryFieldDocCount);
    }

    protected AnomalyDetector createAnomalyDetector() throws IOException, IllegalAccessException {
        return createAnomalyDetector(0);
    }

    protected AnomalyDetector createAnomalyDetector(int categoryFieldSize) throws IOException, IllegalAccessException {
        return createAnomalyDetector(categoryFieldSize, null);
    }

    protected AnomalyDetector createAnomalyDetector(int categoryFieldSize, String resultIndex) throws IOException, IllegalAccessException {
        AggregationBuilder aggregationBuilder = TestHelpers
            .parseAggregation("{\"test\":{\"max\":{\"field\":\"" + MockSimpleLog.VALUE_FIELD + "\"}}}");
        Feature feature = new Feature(randomAlphaOfLength(5), randomAlphaOfLength(10), true, aggregationBuilder);
        List<String> categoryField = null;
        switch (categoryFieldSize) {
            case 0:
                break;
            case 1:
                categoryField = ImmutableList.of(MockSimpleLog.CATEGORY_FIELD);
                break;
            case 2:
                categoryField = ImmutableList.of(MockSimpleLog.IP_FIELD, MockSimpleLog.CATEGORY_FIELD);
                break;
            default:
                throw new IllegalAccessException("Wrong category field size");
        }
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                ImmutableList.of(feature),
                historicalAnalysisTestIndex,
                detectionIntervalInMinutes,
                MockSimpleLog.TIME_FIELD,
                categoryField,
                resultIndex
            );
        return createAnomalyDetector(detector, true, client());
    }

    protected String startHistoricalAnalysis(String detectorId) throws IOException {
        Instant endTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Instant startTime = endTime.minus(10, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        Response startDetectorResponse = startAnomalyDetector(detectorId, dateRange, client());
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

    // TODO: change response to pair
    protected List<Object> waitUntilTaskDone(String detectorId) throws InterruptedException {
        return waitUntilTaskReachState(detectorId, TestHelpers.HISTORICAL_ANALYSIS_DONE_STATS);
    }

    protected List<Object> waitUntilTaskReachState(String detectorId, Set<String> targetStates) throws InterruptedException {
        List<Object> results = new ArrayList<>();
        int i = 0;
        ADTaskProfile adTaskProfile = null;
        // Increase retryTimes if some task can't reach done state
        while ((adTaskProfile == null || !targetStates.contains(adTaskProfile.getAdTask().getState())) && i < MAX_RETRY_TIMES) {
            try {
                adTaskProfile = getADTaskProfile(detectorId);
            } catch (Exception e) {
                logger.error("failed to get ADTaskProfile", e);
            } finally {
                Thread.sleep(1000);
            }
            i++;
        }
        assertNotNull(adTaskProfile);
        results.add(adTaskProfile);
        results.add(i);
        return results;
    }
}
