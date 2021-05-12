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

package com.amazon.opendistroforelasticsearch.ad.rest;

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.AD_BASE_STATS_URI;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;

import java.io.IOException;
import java.util.Map;

import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.rest.RestStatus;

import com.amazon.opendistroforelasticsearch.ad.HistoricalDetectorRestTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.google.common.collect.ImmutableMap;

public class HistoricalDetectorRestApiIT extends HistoricalDetectorRestTestCase {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 5);
        updateClusterSettings(MAX_BATCH_TASK_PER_NODE.getKey(), 10);
    }

    @SuppressWarnings("unchecked")
    public void testHistoricalDetectorWorkflow() throws Exception {
        // create historical detector
        AnomalyDetector detector = createHistoricalDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        String taskId = startHistoricalDetector(detectorId);

        // get task profile
        ADTaskProfile adTaskProfile = waitUntilGetTaskProfile(detectorId);

        ADTask adTask = adTaskProfile.getAdTask();
        assertEquals(taskId, adTask.getTaskId());
        assertTrue(TestHelpers.historicalDetectorRunningStats.contains(adTask.getState()));

        // get task stats
        Response statsResponse = TestHelpers.makeRequest(client(), "GET", AD_BASE_STATS_URI, ImmutableMap.of(), "", null);
        String statsResult = EntityUtils.toString(statsResponse.getEntity());
        Map<String, Object> stringObjectMap = TestHelpers.parseStatsResult(statsResult);
        assertTrue((long) stringObjectMap.get("historical_single_entity_detector_count") > 0);
        Map<String, Object> nodes = (Map<String, Object>) stringObjectMap.get("nodes");
        long totalBatchTaskExecution = 0;
        for (String key : nodes.keySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodes.get(key);
            totalBatchTaskExecution += (long) nodeStats.get("ad_total_batch_task_execution_count");
        }
        assertTrue(totalBatchTaskExecution > 0);

        // get historical detector with AD task
        ToXContentObject[] result = getHistoricalAnomalyDetector(detectorId, true, client());
        AnomalyDetector parsedDetector = (AnomalyDetector) result[0];
        AnomalyDetectorJob parsedJob = (AnomalyDetectorJob) result[1];
        ADTask parsedADTask = (ADTask) result[2];
        assertNull(parsedJob);
        assertNotNull(parsedDetector);
        assertNotNull(parsedADTask);
        assertEquals(taskId, parsedADTask.getTaskId());

        // get task profile
        ADTaskProfile endTaskProfile = waitUntilTaskFinished(detectorId);
        ADTask stoppedAdTask = endTaskProfile.getAdTask();
        assertEquals(taskId, stoppedAdTask.getTaskId());
        assertEquals(ADTaskState.FINISHED.name(), stoppedAdTask.getState());
    }

    @SuppressWarnings("unchecked")
    public void testStopHistoricalDetector() throws Exception {
        // create historical detector
        AnomalyDetector detector = createHistoricalDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        String taskId = startHistoricalDetector(detectorId);

        waitUntilGetTaskProfile(detectorId);

        // stop historical detector
        Response stopDetectorResponse = stopAnomalyDetector(detectorId, client());
        assertEquals(RestStatus.OK, restStatus(stopDetectorResponse));

        // get task profile
        ADTaskProfile stoppedAdTaskProfile = waitUntilTaskFinished(detectorId);
        ADTask stoppedAdTask = stoppedAdTaskProfile.getAdTask();
        assertEquals(taskId, stoppedAdTask.getTaskId());
        assertEquals(ADTaskState.STOPPED.name(), stoppedAdTask.getState());
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1);

        waitUntilTaskFinished(detectorId);

        // get AD stats
        Response statsResponse = TestHelpers.makeRequest(client(), "GET", AD_BASE_STATS_URI, ImmutableMap.of(), "", null);
        String statsResult = EntityUtils.toString(statsResponse.getEntity());
        Map<String, Object> stringObjectMap = TestHelpers.parseStatsResult(statsResult);
        assertTrue((long) stringObjectMap.get("historical_single_entity_detector_count") > 0);
        Map<String, Object> nodes = (Map<String, Object>) stringObjectMap.get("nodes");
        long cancelledTaskCount = 0;
        for (String key : nodes.keySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodes.get(key);
            cancelledTaskCount += (long) nodeStats.get("ad_canceled_batch_task_count");
        }
        assertTrue(cancelledTaskCount >= 1);
    }

    public void testUpdateHistoricalDetector() throws IOException {
        // create historical detector
        AnomalyDetector detector = createHistoricalDetector();
        String detectorId = detector.getDetectorId();

        // update historical detector
        AnomalyDetector newDetector = randomAnomalyDetector(detector);
        Response updateResponse = TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "?refresh=true",
                ImmutableMap.of(),
                toHttpEntity(newDetector),
                null
            );
        Map<String, Object> responseBody = entityAsMap(updateResponse);
        assertEquals(detector.getDetectorId(), responseBody.get("_id"));
        assertEquals((detector.getVersion().intValue() + 1), (int) responseBody.get("_version"));

        // get historical detector
        AnomalyDetector updatedDetector = getAnomalyDetector(detector.getDetectorId(), client());
        assertNotEquals(updatedDetector.getLastUpdateTime(), detector.getLastUpdateTime());
        assertEquals(newDetector.getName(), updatedDetector.getName());
        assertEquals(newDetector.getDescription(), updatedDetector.getDescription());
    }

    public void testUpdateRunningHistoricalDetector() throws Exception {
        // create historical detector
        AnomalyDetector detector = createHistoricalDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        startHistoricalDetector(detectorId);

        // update historical detector
        AnomalyDetector newDetector = randomAnomalyDetector(detector);
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Detector is running",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "PUT",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "?refresh=true",
                        ImmutableMap.of(),
                        toHttpEntity(newDetector),
                        null
                    )
            );

        waitUntilTaskFinished(detectorId);
    }

    public void testDeleteHistoricalDetector() throws IOException {
        // create historical detector
        AnomalyDetector detector = createHistoricalDetector();
        String detectorId = detector.getDetectorId();

        // delete detector
        Response response = TestHelpers
            .makeRequest(client(), "DELETE", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId, ImmutableMap.of(), "", null);
        assertEquals(RestStatus.OK, restStatus(response));
    }

    public void testDeleteRunningHistoricalDetector() throws Exception {
        // create historical detector
        AnomalyDetector detector = createHistoricalDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        startHistoricalDetector(detectorId);

        // delete detector
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Detector is running",
                () -> TestHelpers
                    .makeRequest(client(), "DELETE", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId, ImmutableMap.of(), "", null)
            );

        waitUntilTaskFinished(detectorId);
    }

    public void testSearchTasks() throws IOException, InterruptedException {
        // create historical detector
        AnomalyDetector detector = createHistoricalDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        String taskId = startHistoricalDetector(detectorId);

        waitUntilTaskFinished(detectorId);

        String query = String.format("{\"query\":{\"term\":{\"detector_id\":{\"value\":\"%s\"}}}}", detectorId);
        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI + "/tasks/_search", ImmutableMap.of(), query, null);
        String searchResult = EntityUtils.toString(response.getEntity());
        assertTrue(searchResult.contains(taskId));
        assertTrue(searchResult.contains(detector.getDetectorId()));
    }

    private AnomalyDetector randomAnomalyDetector(AnomalyDetector detector) {
        return new AnomalyDetector(
            detector.getDetectorId(),
            null,
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            detector.getTimeField(),
            detector.getIndices(),
            detector.getFeatureAttributes(),
            detector.getFilterQuery(),
            detector.getDetectionInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime(),
            detector.getCategoryField(),
            detector.getUser(),
            detector.getDetectorType(),
            detector.getDetectionDateRange()
        );
    }

}
