/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.opensearch.ad.HistoricalAnalysisIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;

import com.google.common.collect.ImmutableList;

public class SearchTopAnomalyResultActionTests extends HistoricalAnalysisIntegTestCase {
    private String testIndex;
    private String detectorId;
    private String taskId;
    private Instant testDataTimeStamp;
    private Instant startTime;
    private Instant endTime;
    private ImmutableList<String> categoryFields;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testIndex = "test_data";
        taskId = "test-task-id";
        testDataTimeStamp = Instant.now();
        startTime = testDataTimeStamp.minus(10, ChronoUnit.MINUTES);
        endTime = testDataTimeStamp.plus(10, ChronoUnit.MINUTES);
        categoryFields = ImmutableList.of("test-field-1", "test-field-2");
        ingestTestData();
        createSystemIndices();
        createAndIndexDetector();
    }

    private void ingestTestData() {
        ingestTestData(testIndex, startTime, 1, "test", 1);
    }

    private void createSystemIndices() throws IOException {
        createDetectorIndex();
        createADResultIndex();
    }

    private void createAndIndexDetector() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(testIndex),
                ImmutableList.of(TestHelpers.randomFeature(true)),
                null,
                Instant.now(),
                1,
                false,
                categoryFields
            );
        detectorId = createDetector(detector);

    }

    public void testInstanceAndNameValid() {
        Assert.assertNotNull(SearchTopAnomalyResultAction.INSTANCE.name());
        Assert.assertEquals(SearchTopAnomalyResultAction.INSTANCE.name(), SearchTopAnomalyResultAction.NAME);
    }

    public void testSearchOnNonExistingResultIndex() {
        deleteIndexIfExists(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
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

    // TODO: figure out why lang-painless module isn't loaded with this type of integ test case

    // Empty result index is created for each test, so by immediately searching, it is searching
    // over an empty (but existing) index
    @Ignore
    public void testSearchOnEmptyResultIndex() {
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
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

    @Ignore
    public void testSearchOnNonMatchingResults() throws IOException {
        AnomalyResult nonMatchingResult = TestHelpers.randomHCADAnomalyDetectResult(detectorId + "-invalid", taskId, 0.5, 0.5, null);
        createADResult(nonMatchingResult);
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
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

    // public void testNoIndex() throws IOException {
    // ingestTestData("test-index", Instant.now().minus(2, ChronoUnit.DAYS), 1, "test", 3000);
    // // Create detector
    // AnomalyDetector randomDetector = TestHelpers.randomDetector(
    // ImmutableList.of(),
    // "test-index",
    // 10,
    // "timestamp"
    // );
    // createDetectorIndex();
    // String detectorId = createDetector(randomDetector);
    //
    // // Create result index
    // //deleteIndexIfExists(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
    //
    //
    // SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
    // detectorId,
    // "test-task-id",
    // false,
    // 1,
    // Arrays.asList("test-field"),
    // SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
    // Instant.now().minus(10, ChronoUnit.DAYS),
    // Instant.now()
    // );
    //
    // SearchTopAnomalyResultResponse searchResponse = client().execute(SearchTopAnomalyResultAction.INSTANCE,
    // searchRequest).actionGet(20_000);
    // logger.info("returned buckets: " + searchResponse.getAnomalyResultBuckets());
    //
    //// // If the
    //// expectThrows(
    //// IllegalArgumentException.class,
    //// () -> client()
    //// .execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest)
    //// .actionGet(20_000)
    //// );
    // }

}
