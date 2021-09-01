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

import static org.opensearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.ad.util.RestHandlerUtils.PROFILE;
import static org.opensearch.ad.util.RestHandlerUtils.START_JOB;
import static org.opensearch.ad.util.RestHandlerUtils.STOP_JOB;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.HistoricalAnalysisIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.mock.transport.MockAnomalyDetectorJobAction;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.client.Client;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class AnomalyDetectorJobTransportActionTests extends HistoricalAnalysisIntegTestCase {
    private Instant startTime;
    private Instant endTime;
    private String type = "error";
    private int maxOldAdTaskDocsPerDetector = 2;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, 2000);
        createDetectorIndex();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(MAX_BATCH_TASK_PER_NODE.getKey(), 1)
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), maxOldAdTaskDocsPerDetector)
            .build();
    }

    public void testDetectorIndexNotFound() {
        deleteDetectorIndex();
        String detectorId = randomAlphaOfLength(5);
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId);
        IndexNotFoundException exception = expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(3000)
        );
        assertTrue(exception.getMessage().contains("no such index [.opendistro-anomaly-detectors]"));
    }

    public void testDetectorNotFound() {
        String detectorId = randomAlphaOfLength(5);
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId);
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
        );
        assertTrue(exception.getMessage().contains(FAIL_TO_FIND_DETECTOR_MSG));
    }

    @Ignore
    public void testValidHistoricalAnalysis() throws IOException, InterruptedException {
        ADTask adTask = startHistoricalAnalysis(startTime, endTime);
        Thread.sleep(10000);
        ADTask finishedTask = getADTask(adTask.getTaskId());
        assertEquals(ADTaskState.FINISHED.name(), finishedTask.getState());
    }

    @Ignore
    public void testStartHistoricalAnalysisWithUser() throws IOException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        Client nodeClient = getDataNodeClient();
        if (nodeClient != null) {
            AnomalyDetectorJobResponse response = nodeClient.execute(MockAnomalyDetectorJobAction.INSTANCE, request).actionGet(100000);
            ADTask adTask = getADTask(response.getId());
            assertNotNull(adTask.getStartedBy());
            assertNotNull(adTask.getUser());
        }
    }

    @Ignore
    public void testRunMultipleTasksForHistoricalAnalysis() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId);
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        assertNotNull(response.getId());
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
        );
        assertTrue(exception.getMessage().contains(DETECTOR_IS_RUNNING));
        assertEquals(DETECTOR_IS_RUNNING, exception.getMessage());
        Thread.sleep(20000);
        List<ADTask> adTasks = searchADTasks(detectorId, null, 100);
        assertEquals(1, adTasks.size());
        assertEquals(ADTaskState.FINISHED.name(), adTasks.get(0).getState());
    }

    @Ignore
    public void testRaceConditionByStartingMultipleTasks() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        client().execute(AnomalyDetectorJobAction.INSTANCE, request);
        client().execute(AnomalyDetectorJobAction.INSTANCE, request);

        Thread.sleep(5000);
        List<ADTask> adTasks = searchADTasks(detectorId, null, 100);

        assertEquals(1, adTasks.size());
        assertTrue(adTasks.get(0).getLatest());
        assertNotEquals(ADTaskState.FAILED.name(), adTasks.get(0).getState());
    }

    public void testCleanOldTaskDocs() throws InterruptedException, IOException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);

        createDetectionStateIndex();
        List<ADTaskState> states = ImmutableList.of(ADTaskState.FAILED, ADTaskState.FINISHED, ADTaskState.STOPPED);
        for (ADTaskState state : states) {
            ADTask task = randomADTask(randomAlphaOfLength(5), detector, detectorId, dateRange, state);
            createADTask(task);
        }
        long count = countDocs(CommonName.DETECTION_STATE_INDEX);
        assertEquals(states.size(), count);

        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(detectorId, randomLong(), randomLong(), START_JOB);

        AtomicReference<AnomalyDetectorJobResponse> response = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client().execute(AnomalyDetectorJobAction.INSTANCE, request, ActionListener.wrap(r -> {
            latch.countDown();
            response.set(r);
        }, e -> { latch.countDown(); }));
        latch.await();
        Thread.sleep(10000);
        count = countDocs(CommonName.DETECTION_STATE_INDEX);
        // we have one latest task, so total count should add 1
        assertEquals(maxOldAdTaskDocsPerDetector + 1, count);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        // delete index will clear search context, this can avoid in-flight contexts error
        deleteIndexIfExists(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
        deleteIndexIfExists(CommonName.DETECTION_STATE_INDEX);
    }

    @Ignore
    public void testStartRealtimeDetector() throws IOException {
        String detectorId = startRealtimeDetector();
        GetResponse doc = getDoc(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId);
        AnomalyDetectorJob job = toADJob(doc);
        assertTrue(job.isEnabled());
        assertEquals(detectorId, job.getName());
    }

    private String startRealtimeDetector() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId);
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        assertEquals(detectorId, response.getId());
        return response.getId();
    }

    public void testRealtimeDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomDetector(ImmutableList.of(), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no features configured");
    }

    public void testHistoricalDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomDetector(ImmutableList.of(), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no features configured");
    }

    public void testRealtimeDetectorWithoutEnabledFeature() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(TestHelpers.randomFeature(false)), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no enabled features configured");
    }

    public void testHistoricalDetectorWithoutEnabledFeature() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(TestHelpers.randomFeature(false)), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no enabled features configured");
    }

    private void testInvalidDetector(AnomalyDetector detector, String error) throws IOException {
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId);
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
        );
        assertEquals(error, exception.getMessage());
    }

    private AnomalyDetectorJobRequest startDetectorJobRequest(String detectorId) {
        return new AnomalyDetectorJobRequest(detectorId, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, START_JOB);
    }

    private AnomalyDetectorJobRequest startDetectorJobRequest(String detectorId, DetectionDateRange dateRange) {
        return new AnomalyDetectorJobRequest(detectorId, dateRange, false, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, START_JOB);
    }

    private AnomalyDetectorJobRequest stopDetectorJobRequest(String detectorId) {
        return new AnomalyDetectorJobRequest(detectorId, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, STOP_JOB);
    }

    @Ignore
    public void testStopRealtimeDetector() throws IOException {
        String detectorId = startRealtimeDetector();
        AnomalyDetectorJobRequest request = stopDetectorJobRequest(detectorId);
        client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        GetResponse doc = getDoc(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId);
        AnomalyDetectorJob job = toADJob(doc);
        assertFalse(job.isEnabled());
        assertEquals(detectorId, job.getName());
    }

    @Ignore
    public void testStopHistoricalDetector() throws IOException, InterruptedException {
        updateTransientSettings(ImmutableMap.of(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 5));
        ADTask adTask = startHistoricalAnalysis(startTime, endTime);
        assertEquals(ADTaskState.INIT.name(), adTask.getState());
        assertNull(adTask.getStartedBy());
        assertNull(adTask.getUser());
        AnomalyDetectorJobRequest request = stopDetectorJobRequest(adTask.getDetectorId());
        client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        waitUntil(() -> {
            try {
                ADTask task = getADTask(adTask.getTaskId());
                return !TestHelpers.historicalAnalysisRunningStats.contains(task.getState());
            } catch (IOException e) {
                return false;
            }
        }, 20, TimeUnit.SECONDS);
        ADTask stoppedTask = getADTask(adTask.getTaskId());
        assertEquals(ADTaskState.STOPPED.name(), stoppedTask.getState());
        assertEquals(0, getExecutingADTask());
    }

    @Ignore
    public void testProfileHistoricalDetector() throws IOException, InterruptedException {
        ADTask adTask = startHistoricalAnalysis(startTime, endTime);
        GetAnomalyDetectorRequest request = taskProfileRequest(adTask.getDetectorId());
        GetAnomalyDetectorResponse response = client().execute(GetAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertTrue(response.getDetectorProfile().getAdTaskProfiles().size() > 0);

        ADTask finishedTask = getADTask(adTask.getTaskId());
        int i = 0;
        while (TestHelpers.historicalAnalysisRunningStats.contains(finishedTask.getState()) && i < 10) {
            finishedTask = getADTask(adTask.getTaskId());
            Thread.sleep(2000);
            i++;
        }
        assertEquals(ADTaskState.FINISHED.name(), finishedTask.getState());

        response = client().execute(GetAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        System.out.println("------------------------------");
        System.out.println(response.getDetectorProfile().getAdTaskProfiles().keySet());
        System.out.println("------------------------------");
        // assertNull(response.getDetectorProfile().getAdTaskProfile().getNodeId());
        // ADTask profileAdTask = response.getDetectorProfile().getAdTaskProfile().getAdTask();
        // assertEquals(finishedTask.getTaskId(), profileAdTask.getTaskId());
        // assertEquals(finishedTask.getDetectorId(), profileAdTask.getDetectorId());
        // assertEquals(finishedTask.getDetector(), profileAdTask.getDetector());
        // assertEquals(finishedTask.getState(), profileAdTask.getState());
    }

    @Ignore
    public void testProfileWithMultipleRunningTask() throws IOException {
        ADTask adTask1 = startHistoricalAnalysis(startTime, endTime);
        ADTask adTask2 = startHistoricalAnalysis(startTime, endTime);

        GetAnomalyDetectorRequest request1 = taskProfileRequest(adTask1.getDetectorId());
        GetAnomalyDetectorRequest request2 = taskProfileRequest(adTask2.getDetectorId());
        GetAnomalyDetectorResponse response1 = client().execute(GetAnomalyDetectorAction.INSTANCE, request1).actionGet(10000);
        GetAnomalyDetectorResponse response2 = client().execute(GetAnomalyDetectorAction.INSTANCE, request2).actionGet(10000);
        // ADTaskProfile taskProfile1 = response1.getDetectorProfile().getAdTaskProfile();
        // ADTaskProfile taskProfile2 = response2.getDetectorProfile().getAdTaskProfile();
        // assertNotNull(taskProfile1.getNodeId());
        // assertNotNull(taskProfile2.getNodeId());
        // assertNotEquals(taskProfile1.getNodeId(), taskProfile2.getNodeId());
    }

    private GetAnomalyDetectorRequest taskProfileRequest(String detectorId) throws IOException {
        return new GetAnomalyDetectorRequest(detectorId, Versions.MATCH_ANY, false, false, "", PROFILE, true, null);
    }

    private long getExecutingADTask() {
        ADStatsRequest adStatsRequest = new ADStatsRequest(getDataNodesArray());
        Set<String> validStats = ImmutableSet.of(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName());
        adStatsRequest.addAll(validStats);
        StatsAnomalyDetectorResponse statsResponse = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5000);
        AtomicLong totalExecutingTask = new AtomicLong(0);
        statsResponse
            .getAdStatsResponse()
            .getADStatsNodesResponse()
            .getNodes()
            .forEach(
                node -> { totalExecutingTask.getAndAdd((Long) node.getStatsMap().get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName())); }
            );
        return totalExecutingTask.get();
    }
}
