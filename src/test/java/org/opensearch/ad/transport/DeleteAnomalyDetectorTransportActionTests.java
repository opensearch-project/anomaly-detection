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

package org.opensearch.ad.transport;

import static org.opensearch.timeseries.TestHelpers.randomIntervalSchedule;
import static org.opensearch.timeseries.TestHelpers.randomIntervalTimeConfiguration;
import static org.opensearch.timeseries.TestHelpers.randomUser;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.junit.Before;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ad.HistoricalAnalysisIntegTestCase;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.DeleteConfigRequest;

import com.google.common.collect.ImmutableList;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class DeleteAnomalyDetectorTransportActionTests extends HistoricalAnalysisIntegTestCase {
    private Instant startTime;
    private String type = "error";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, 2000);
        createDetectorIndex();
    }

    public void testDeleteAnomalyDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null);
        testDeleteDetector(detector);
    }

    public void testDeleteAnomalyDetectorWithDisabledFeature() throws IOException {
        Feature feature = TestHelpers.randomFeature(false);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature));
        testDeleteDetector(detector);
    }

    public void testDeleteAnomalyDetectorWithEnabledFeature() throws IOException {
        Feature feature = TestHelpers.randomFeature(true);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature));
        testDeleteDetector(detector);
    }

    public void testDeleteAnomalyDetectorNotFound() {
        String bogusId = "this-id-does-not-exist";

        client().admin().indices().delete(new DeleteIndexRequest(testIndex, ADIndex.CONFIG.getIndexName())).actionGet(10_000);

        DeleteConfigRequest request = new DeleteConfigRequest(bogusId);
        IndexNotFoundException response = expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(DeleteAnomalyDetectorAction.INSTANCE, request).actionGet(10_000L)
        );
        assertEquals(RestStatus.NOT_FOUND, response.status());
    }

    public void testDeleteAnomalyDetectorWithJobEnabled() throws IOException {
        Feature feature = TestHelpers.randomFeature(false);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature));
        String detectorId = createDetector(detector);

        Job job = new Job(
            detectorId,
            randomIntervalSchedule(),
            randomIntervalTimeConfiguration(),
            true,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            60L,
            randomUser(),
            null,
            AnalysisType.AD
        );

        IndexRequest jobReq = new IndexRequest(CommonName.JOB_INDEX)
            .id(detectorId)
            .source(job.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        client().index(jobReq).actionGet();

        DeleteConfigRequest deleteReq = new DeleteConfigRequest(detectorId);
        OpenSearchStatusException ex = expectThrows(
            OpenSearchStatusException.class,
            () -> client().execute(DeleteAnomalyDetectorAction.INSTANCE, deleteReq).actionGet(10_000L)
        );

        assertEquals(RestStatus.BAD_REQUEST, ex.status());
        String msg = ex.getMessage().toLowerCase(Locale.ROOT);
        assertTrue(msg.contains("historical is running") || msg.contains("run once") || msg.contains("running"));

    }

    public void testDeleteAnomalyDetectorWithJobDisabled() throws IOException {
        Feature feature = TestHelpers.randomFeature(false);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature));
        String detectorId = createDetector(detector);

        Job job = new Job(
            detectorId,
            randomIntervalSchedule(),
            randomIntervalTimeConfiguration(),
            false,
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            Instant.now().truncatedTo(ChronoUnit.SECONDS),
            60L,
            randomUser(),
            null,
            AnalysisType.AD
        );

        IndexRequest jobReq = new IndexRequest(CommonName.JOB_INDEX)
            .id(detectorId)
            .source(job.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        client().index(jobReq).actionGet();

        DeleteConfigRequest deleteReq = new DeleteConfigRequest(detectorId);
        DeleteResponse res = client().execute(DeleteAnomalyDetectorAction.INSTANCE, deleteReq).actionGet(10_000L);

        assertEquals(RestStatus.OK, res.status());
    }

    private void testDeleteDetector(AnomalyDetector detector) throws IOException {
        String detectorId = createDetector(detector);
        DeleteConfigRequest request = new DeleteConfigRequest(detectorId);
        DeleteResponse deleteResponse = client().execute(DeleteAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertEquals("deleted", deleteResponse.getResult().getLowercase());
    }
}
