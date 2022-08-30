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
package org.opensearch.ad.transport;


@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
@Ignore
public class DeleteAnomalyDetectorTransportActionTests extends HistoricalAnalysisIntegTestCase {
    private Instant startTime;
    private Instant endTime;
    private String type = "error";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, 2000);
        createDetectorIndex();
    }

    public void testDeleteAnomalyDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null);
        testDeleteDetector(detector);
    }

    public void testDeleteAnomalyDetectorWithoutEnabledFeature() throws IOException {
        Feature feature = TestHelpers.randomFeature(false);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature));
        testDeleteDetector(detector);
    }

    public void testDeleteAnomalyDetectorWithEnabledFeature() throws IOException {
        Feature feature = TestHelpers.randomFeature(true);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature));
        testDeleteDetector(detector);
    }

    private void testDeleteDetector(AnomalyDetector detector) throws IOException {
        String detectorId = createDetector(detector);
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest(detectorId);
        DeleteResponse deleteResponse = client().execute(DeleteAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertEquals("deleted", deleteResponse.getResult().getLowercase());
    }
}
*/
