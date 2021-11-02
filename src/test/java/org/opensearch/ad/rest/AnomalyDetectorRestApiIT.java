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

package org.opensearch.ad.rest;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG;
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.DUPLICATE_DETECTOR_MSG;
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.junit.Assert;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AnomalyDetectorRestApiIT extends AnomalyDetectorRestTestCase {

    public void testCreateAnomalyDetectorWithNotExistingIndices() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "index_not_found_exception",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI,
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(detector),
                        null
                    )
            );
    }

    public void testCreateAnomalyDetectorWithEmptyIndices() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/" + detector.getIndices().get(0),
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"settings\":{\"number_of_shards\":1}," + " \"mappings\":{\"properties\":" + "{\"field1\":{\"type\":\"text\"}}}}"
                    ),
                null
            );

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Can't create anomaly detector as no document is found in the indices",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI,
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(detector),
                        null
                    )
            );
    }

    public void testCreateAnomalyDetectorWithDuplicateName() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        AnomalyDetector detectorDuplicateName = new AnomalyDetector(
            AnomalyDetector.NO_ID,
            randomLong(),
            detector.getName(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            detector.getIndices(),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            randomIntBetween(1, AnomalyDetectorSettings.MAX_SHINGLE_SIZE),
            TestHelpers.randomUiMetadata(),
            randomInt(),
            null,
            null,
            TestHelpers.randomUser(),
            null
        );

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Cannot create anomaly detector with name",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI,
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(detectorDuplicateName),
                        null
                    )
            );
    }

    public void testCreateAnomalyDetector() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"name\": \"test\"}"));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_DETECTORS_URI,
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(detector),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);
        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), TestHelpers.toHttpEntity(detector), null);
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        int version = (int) responseMap.get("_version");
        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
        assertTrue("incorrect version", version > 0);
    }

    public void testUpdateAnomalyDetectorCategoryField() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"name\": \"test\"}"));
        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), TestHelpers.toHttpEntity(detector), null);
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        AnomalyDetector newDetector = new AnomalyDetector(
            id,
            null,
            detector.getName(),
            detector.getDescription(),
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
            ImmutableList.of(randomAlphaOfLength(5)),
            detector.getUser(),
            null
        );
        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "PUT",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + id + "?refresh=true",
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(newDetector),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.CAN_NOT_CHANGE_CATEGORY_FIELD));
    }

    public void testGetAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(ResponseException.class, () -> getAnomalyDetector(detector.getDetectorId(), client()));
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        AnomalyDetector createdDetector = getAnomalyDetector(detector.getDetectorId(), client());
        assertEquals("Incorrect Location header", detector, createdDetector);
    }

    public void testGetNotExistingAnomalyDetector() throws Exception {
        createRandomAnomalyDetector(true, true, client());
        TestHelpers.assertFailWith(ResponseException.class, null, () -> getAnomalyDetector(randomAlphaOfLength(5), client()));
    }

    public void testUpdateAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        String newDescription = randomAlphaOfLength(5);

        AnomalyDetector newDetector = new AnomalyDetector(
            detector.getDetectorId(),
            detector.getVersion(),
            detector.getName(),
            newDescription,
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
            null,
            detector.getUser(),
            null
        );

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "PUT",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "?refresh=true",
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(newDetector),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);
        Thread.sleep(2000);

        Response updateResponse = TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "?refresh=true",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(newDetector),
                null
            );

        assertEquals("Update anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(updateResponse));
        Map<String, Object> responseBody = entityAsMap(updateResponse);
        assertEquals("Updated anomaly detector id doesn't match", detector.getDetectorId(), responseBody.get("_id"));
        assertEquals("Version not incremented", (detector.getVersion().intValue() + 1), (int) responseBody.get("_version"));

        AnomalyDetector updatedDetector = getAnomalyDetector(detector.getDetectorId(), client());
        assertNotEquals("Anomaly detector last update time not changed", updatedDetector.getLastUpdateTime(), detector.getLastUpdateTime());
        assertEquals("Anomaly detector description not updated", newDescription, updatedDetector.getDescription());
    }

    public void testUpdateAnomalyDetectorNameToExisting() throws Exception {
        AnomalyDetector detector1 = createRandomAnomalyDetector(true, true, client());

        AnomalyDetector detector2 = createRandomAnomalyDetector(true, true, client());

        AnomalyDetector newDetector1WithDetector2Name = new AnomalyDetector(
            detector1.getDetectorId(),
            detector1.getVersion(),
            detector2.getName(),
            detector1.getDescription(),
            detector1.getTimeField(),
            detector1.getIndices(),
            detector1.getFeatureAttributes(),
            detector1.getFilterQuery(),
            detector1.getDetectionInterval(),
            detector1.getWindowDelay(),
            detector1.getShingleSize(),
            detector1.getUiMetadata(),
            detector1.getSchemaVersion(),
            detector1.getLastUpdateTime(),
            null,
            detector1.getUser(),
            null
        );

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Cannot create anomaly detector with name",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI,
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(newDetector1WithDetector2Name),
                        null
                    )
            );
    }

    public void testUpdateAnomalyDetectorNameToNew() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        AnomalyDetector detectorWithNewName = new AnomalyDetector(
            detector.getDetectorId(),
            detector.getVersion(),
            randomAlphaOfLength(5),
            detector.getDescription(),
            detector.getTimeField(),
            detector.getIndices(),
            detector.getFeatureAttributes(),
            detector.getFilterQuery(),
            detector.getDetectionInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            Instant.now(),
            null,
            detector.getUser(),
            null
        );

        Thread.sleep(2000); // sleep some time before updating to avoid flaky test
        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "?refresh=true",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detectorWithNewName),
                null
            );

        AnomalyDetector resultDetector = getAnomalyDetector(detectorWithNewName.getDetectorId(), client());
        assertEquals("Detector name updating failed", detectorWithNewName.getName(), resultDetector.getName());
        assertEquals("Updated anomaly detector id doesn't match", detectorWithNewName.getDetectorId(), resultDetector.getDetectorId());
        assertNotEquals(
            "Anomaly detector last update time not changed",
            detectorWithNewName.getLastUpdateTime(),
            resultDetector.getLastUpdateTime()
        );
    }

    public void testUpdateAnomalyDetectorWithNotExistingIndex() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        String newDescription = randomAlphaOfLength(5);

        AnomalyDetector newDetector = new AnomalyDetector(
            detector.getDetectorId(),
            detector.getVersion(),
            detector.getName(),
            newDescription,
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
            null,
            detector.getUser(),
            null
        );

        deleteIndexWithAdminClient(AnomalyDetector.ANOMALY_DETECTORS_INDEX);

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                null,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "PUT",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(newDetector),
                        null
                    )
            );
    }

    public void testSearchAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        SearchSourceBuilder search = (new SearchSourceBuilder()).query(QueryBuilders.termQuery("_id", detector.getDetectorId()));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "GET",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/_search",
                    ImmutableMap.of(),
                    new NStringEntity(search.toString(), ContentType.APPLICATION_JSON),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response searchResponse = TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_search",
                ImmutableMap.of(),
                new NStringEntity(search.toString(), ContentType.APPLICATION_JSON),
                null
            );
        assertEquals("Search anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(searchResponse));
    }

    public void testStatsAnomalyDetector() throws Exception {
        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);
        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers.makeRequest(client(), "GET", AnomalyDetectorPlugin.LEGACY_AD_BASE + "/stats", ImmutableMap.of(), "", null)
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response statsResponse = TestHelpers
            .makeRequest(client(), "GET", AnomalyDetectorPlugin.LEGACY_AD_BASE + "/stats", ImmutableMap.of(), "", null);

        assertEquals("Get stats failed", RestStatus.OK, TestHelpers.restStatus(statsResponse));
    }

    public void testPreviewAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            detector.getDetectorId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(input),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(input),
                null
            );
        assertEquals("Execute anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testPreviewAnomalyDetectorWhichNotExist() throws Exception {
        createRandomAnomalyDetector(true, false, client());
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            randomAlphaOfLength(5),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(input),
                        null
                    )
            );
    }

    public void testExecuteAnomalyDetectorWithNullDetectorId() throws Exception {
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            null,
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(input),
                        null
                    )
            );
    }

    public void testPreviewAnomalyDetectorWithDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            detector.getDetectorId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            detector
        );
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(input),
                null,
                false
            );
        assertEquals("Execute anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testPreviewAnomalyDetectorWithDetectorAndNoFeatures() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            detector.getDetectorId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            TestHelpers.randomAnomalyDetectorWithEmptyFeature()
        );
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Can't preview detector without feature",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(input),
                        null
                    )
            );
    }

    public void testSearchAnomalyResult() throws Exception {
        AnomalyResult anomalyResult = TestHelpers.randomAnomalyDetectResult();
        Response response = TestHelpers
            .makeRequest(
                adminClient(),
                "POST",
                "/.opendistro-anomaly-results/_doc/" + UUIDs.base64UUID(),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(anomalyResult),
                null,
                false
            );
        assertEquals("Post anomaly result failed", RestStatus.CREATED, TestHelpers.restStatus(response));

        SearchSourceBuilder search = (new SearchSourceBuilder())
            .query(QueryBuilders.termQuery("detector_id", anomalyResult.getDetectorId()));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_RESULT_URI + "/_search",
                    ImmutableMap.of(),
                    new NStringEntity(search.toString(), ContentType.APPLICATION_JSON),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response searchResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_RESULT_URI + "/_search",
                ImmutableMap.of(),
                new NStringEntity(search.toString(), ContentType.APPLICATION_JSON),
                null
            );
        assertEquals("Search anomaly result failed", RestStatus.OK, TestHelpers.restStatus(searchResponse));

        SearchSourceBuilder searchAll = SearchSourceBuilder.fromXContent(TestHelpers.parser("{\"query\":{\"match_all\":{}}}"));
        Response searchAllResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_RESULT_URI + "/_search",
                ImmutableMap.of(),
                new NStringEntity(searchAll.toString(), ContentType.APPLICATION_JSON),
                null
            );
        assertEquals("Search anomaly result failed", RestStatus.OK, TestHelpers.restStatus(searchAllResponse));
    }

    public void testDeleteAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "DELETE",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                    ImmutableMap.of(),
                    "",
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Delete anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testDeleteAnomalyDetectorWhichNotExist() throws Exception {
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "DELETE",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + randomAlphaOfLength(5),
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testDeleteAnomalyDetectorWithNoAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());
        Response response = TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Delete anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testDeleteAnomalyDetectorWithRunningAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());

        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Detector job is running",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "DELETE",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testUpdateAnomalyDetectorWithRunningAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());

        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        String newDescription = randomAlphaOfLength(5);

        AnomalyDetector newDetector = new AnomalyDetector(
            detector.getDetectorId(),
            detector.getVersion(),
            detector.getName(),
            newDescription,
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
            null,
            detector.getUser(),
            null
        );

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Detector job is running",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "PUT",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId(),
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(newDetector),
                        null
                    )
            );
    }

    public void testGetDetectorWithAdJob() throws IOException {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());

        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        ToXContentObject[] results = getAnomalyDetector(detector.getDetectorId(), true, client());
        assertEquals("Incorrect Location header", detector, results[0]);
        assertEquals("Incorrect detector job name", detector.getDetectorId(), ((AnomalyDetectorJob) results[1]).getName());
        assertTrue(((AnomalyDetectorJob) results[1]).isEnabled());

        results = getAnomalyDetector(detector.getDetectorId(), false, client());
        assertEquals("Incorrect Location header", detector, results[0]);
        assertEquals("Should not return detector job", null, results[1]);
    }

    public void testStartAdJobWithExistingDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                    ImmutableMap.of(),
                    "",
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));
    }

    public void testStartAdJobWithNonexistingDetectorIndex() throws Exception {
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "no such index [.opendistro-anomaly-detectors]",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + randomAlphaOfLength(10) + "/_start",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testStartAdJobWithNonexistingDetector() throws Exception {
        createRandomAnomalyDetector(true, false, client());
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                FAIL_TO_FIND_DETECTOR_MSG,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + randomAlphaOfLength(10) + "/_start",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testStopAdJob() throws Exception {
        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                    ImmutableMap.of(),
                    "",
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, TestHelpers.restStatus(stopAdJobResponse));

        stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, TestHelpers.restStatus(stopAdJobResponse));
    }

    public void testStopNonExistingAdJobIndex() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "no such index [.opendistro-anomaly-detector-jobs]",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testStopNonExistingAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());
        // sometimes it fails to start detector as not able to find detector, sleep 2 seconds
        Thread.sleep(2000);
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                FAIL_TO_FIND_DETECTOR_MSG,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + randomAlphaOfLength(10) + "/_stop",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testStartDisabledAdjob() throws IOException {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        Response stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, TestHelpers.restStatus(stopAdJobResponse));

        startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));
    }

    public void testStartAdjobWithNullFeatures() throws Exception {
        AnomalyDetector detectorWithoutFeature = TestHelpers.randomAnomalyDetector(null, null, Instant.now());
        String indexName = detectorWithoutFeature.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"name\": \"test\"}"));
        AnomalyDetector detector = createAnomalyDetector(detectorWithoutFeature, true, client());
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Can't start detector job as no features configured",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testStartAdjobWithEmptyFeatures() throws Exception {
        AnomalyDetector detectorWithoutFeature = TestHelpers.randomAnomalyDetector(ImmutableList.of(), null, Instant.now());
        String indexName = detectorWithoutFeature.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"name\": \"test\"}"));
        AnomalyDetector detector = createAnomalyDetector(detectorWithoutFeature, true, client());
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Can't start detector job as no features configured",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testDefaultProfileAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(ResponseException.class, () -> getDetectorProfile(detector.getDetectorId()));
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response profileResponse = getDetectorProfile(detector.getDetectorId());
        assertEquals("Incorrect profile status", RestStatus.OK, TestHelpers.restStatus(profileResponse));
    }

    public void testAllProfileAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        Response profileResponse = getDetectorProfile(detector.getDetectorId(), true);
        assertEquals("Incorrect profile status", RestStatus.OK, TestHelpers.restStatus(profileResponse));
    }

    public void testCustomizedProfileAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        Response profileResponse = getDetectorProfile(detector.getDetectorId(), true, "/models/", client());
        assertEquals("Incorrect profile status", RestStatus.OK, TestHelpers.restStatus(profileResponse));
    }

    public void testSearchAnomalyDetectorCountNoIndex() throws Exception {
        Response countResponse = getSearchDetectorCount();
        Map<String, Object> responseMap = entityAsMap(countResponse);
        Integer count = (Integer) responseMap.get("count");
        assertEquals((long) count, 0);
    }

    public void testSearchAnomalyDetectorCount() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        Response countResponse = getSearchDetectorCount();
        Map<String, Object> responseMap = entityAsMap(countResponse);
        Integer count = (Integer) responseMap.get("count");
        assertEquals((long) count, 1);
    }

    public void testSearchAnomalyDetectorMatchNoIndex() throws Exception {
        Response matchResponse = getSearchDetectorMatch("name");
        Map<String, Object> responseMap = entityAsMap(matchResponse);
        boolean nameExists = (boolean) responseMap.get("match");
        assertEquals(nameExists, false);
    }

    public void testSearchAnomalyDetectorNoMatch() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        Response matchResponse = getSearchDetectorMatch(detector.getName());
        Map<String, Object> responseMap = entityAsMap(matchResponse);
        boolean nameExists = (boolean) responseMap.get("match");
        assertEquals(nameExists, true);
    }

    public void testSearchAnomalyDetectorMatch() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        Response matchResponse = getSearchDetectorMatch(detector.getName() + "newDetector");
        Map<String, Object> responseMap = entityAsMap(matchResponse);
        boolean nameExists = (boolean) responseMap.get("match");
        assertEquals(nameExists, false);
    }

    public void testRunDetectorWithNoEnabledFeature() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client(), false);
        Assert.assertNotNull(detector.getDetectorId());
        Instant now = Instant.now();
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> startAnomalyDetector(detector.getDetectorId(), new DetectionDateRange(now.minus(10, ChronoUnit.DAYS), now), client())
        );
        assertTrue(e.getMessage().contains("Can't start detector job as no enabled features configured"));
    }

    public void testDeleteAnomalyDetectorWhileRunning() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        Assert.assertNotNull(detector.getDetectorId());
        Instant now = Instant.now();
        Response response = startAnomalyDetector(
            detector.getDetectorId(),
            new DetectionDateRange(now.minus(10, ChronoUnit.DAYS), now),
            client()
        );
        Assert.assertEquals(response.getStatusLine().toString(), "HTTP/1.1 200 OK");

        // Deleting detector should fail while its running
        Exception exception = expectThrows(IOException.class, () -> { deleteAnomalyDetector(detector.getDetectorId(), client()); });
        exception.printStackTrace();
        Assert.assertTrue(exception.getMessage().contains("Detector is running"));
    }

    public void testBackwardCompatibilityWithOpenDistro() throws IOException {
        // Create a detector
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"name\": \"test\"}"));

        // Verify the detector is created using legacy _opendistro API
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        int version = (int) responseMap.get("_version");
        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
        assertTrue("incorrect version", version > 0);

        // Get the detector using new _plugins API
        AnomalyDetector createdDetector = getAnomalyDetector(id, client());
        assertEquals("Get anomaly detector failed", createdDetector.getDetectorId(), id);

        // Delete the detector using legacy _opendistro API
        response = TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + createdDetector.getDetectorId(),
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Delete anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(response));

    }

    public void testValidateAnomalyDetectorWithDuplicateName() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        TestHelpers.createIndex(client(), "test-index", TestHelpers.toHttpEntity("{\"timestamp\": " + Instant.now().toEpochMilli() + "}"));
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate",
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"name\":\""
                            + detector.getName()
                            + "\",\"description\":\"Test detector\",\"time_field\":\"timestamp\","
                            + "\"indices\":[\"test-index\"],\"feature_attributes\":[{\"feature_name\":\"cpu-sum\",\""
                            + "feature_enabled\":true,\"aggregation_query\":{\"total_cpu\":{\"sum\":{\"field\":\"cpu\"}}}},"
                            + "{\"feature_name\":\"error-sum\",\"feature_enabled\":true,\"aggregation_query\":"
                            + "{\"total_error\":"
                            + "{\"sum\":{\"field\":\"error\"}}}}],\"filter_query\":{\"bool\":{\"filter\":[{\"exists\":"
                            + "{\"field\":"
                            + "\"cpu\",\"boost\":1}}],\"adjust_pure_negative\":true,\"boost\":1}},\"detection_interval\":"
                            + "{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
                            + "\"window_delay\":{\"period\":{\"interval\":2,\"unit\":\"Minutes\"}}}"
                    ),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("detector", responseMap);
        assertEquals("Validation returned duplicate detector name message", RestStatus.OK, TestHelpers.restStatus(resp));
        String errorMsg = String.format(Locale.ROOT, DUPLICATE_DETECTOR_MSG, detector.getName(), "[" + detector.getDetectorId() + "]");
        assertEquals("duplicate error message", errorMsg, messageMap.get("name").get("message"));
    }

    public void testValidateAnomalyDetectorWithNoTimeField() throws Exception {
        TestHelpers.createIndex(client(), "test-index", TestHelpers.toHttpEntity("{\"timestamp\": " + Instant.now().toEpochMilli() + "}"));
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate",
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"name\":\"test\",\"description\":\"\""
                            + ",\"indices\":[\"test-index\"],\"feature_attributes\":[{\"feature_name\":\"test\","
                            + "\"feature_enabled\":true,\"aggregation_query\":{\"test\":{\"sum\":{\"field\":\"value\"}}}}],"
                            + "\"filter_query\":{},\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
                            + "\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}}}"
                    ),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("detector", responseMap);
        assertEquals("Validation response returned", RestStatus.OK, TestHelpers.restStatus(resp));
        assertEquals("time field missing", CommonErrorMessages.NULL_TIME_FIELD, messageMap.get("time_field").get("message"));
    }

    public void testValidateAnomalyDetectorWithIncorrectShingleSize() throws Exception {
        TestHelpers.createIndex(client(), "test-index", TestHelpers.toHttpEntity("{\"timestamp\": " + Instant.now().toEpochMilli() + "}"));
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate",
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"name\":\""
                            + "test-detector"
                            + "\",\"description\":\"Test detector\",\"time_field\":\"timestamp\","
                            + "\"indices\":[\"test-index\"],\"feature_attributes\":[{\"feature_name\":\"cpu-sum\",\""
                            + "feature_enabled\":true,\"aggregation_query\":{\"total_cpu\":{\"sum\":{\"field\":\"cpu\"}}}},"
                            + "{\"feature_name\":\"error-sum\",\"feature_enabled\":true,\"aggregation_query\":"
                            + "{\"total_error\":"
                            + "{\"sum\":{\"field\":\"error\"}}}}],\"filter_query\":{\"bool\":{\"filter\":[{\"exists\":"
                            + "{\"field\":"
                            + "\"cpu\",\"boost\":1}}],\"adjust_pure_negative\":true,\"boost\":1}},\"detection_interval\":"
                            + "{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
                            + "\"window_delay\":{\"period\":{\"interval\":2,\"unit\":\"Minutes\"}},"
                            + "\"shingle_size\": 2000}"
                    ),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("detector", responseMap);
        String errorMessage = "Shingle size must be a positive integer no larger than "
            + AnomalyDetectorSettings.MAX_SHINGLE_SIZE
            + ". Got 2000";
        assertEquals("shingle size error message", errorMessage, messageMap.get("shingle_size").get("message"));
    }

    public void testValidateAnomalyDetectorWithNoIssue() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"timestamp\": " + Instant.now().toEpochMilli() + "}"));
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate/detector",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        assertEquals("no issue, empty response body", new HashMap<String, Object>(), responseMap);
    }

    public void testValidateAnomalyDetectorOnWrongValidationType() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"timestamp\": " + Instant.now().toEpochMilli() + "}"));
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                CommonErrorMessages.NOT_EXISTENT_VALIDATION_TYPE,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/_validate/model",
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(detector),
                        null
                    )
            );
    }

    public void testValidateAnomalyDetectorWithEmptyIndices() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/" + detector.getIndices().get(0),
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"settings\":{\"number_of_shards\":1}," + " \"mappings\":{\"properties\":" + "{\"field1\":{\"type\":\"text\"}}}}"
                    ),
                null
            );
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("detector", responseMap);
        assertEquals("Validation returned message regarding empty indices", RestStatus.OK, TestHelpers.restStatus(resp));
        String errorMessage = NO_DOCS_IN_USER_INDEX_MSG + "[" + detector.getIndices().get(0) + "]";
        assertEquals("duplicate error message", errorMessage, messageMap.get("indices").get("message"));
    }

    public void testValidateAnomalyDetectorWithInvalidName() throws Exception {
        TestHelpers.createIndex(client(), "test-index", TestHelpers.toHttpEntity("{\"timestamp\": " + Instant.now().toEpochMilli() + "}"));
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate/detector",
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"name\":\"#@$3\",\"description\":\"\",\"time_field\":\"timestamp\""
                            + ",\"indices\":[\"test-index\"],\"feature_attributes\":[{\"feature_name\":\"test\","
                            + "\"feature_enabled\":true,\"aggregation_query\":{\"test\":{\"sum\":{\"field\":\"value\"}}}}],"
                            + "\"filter_query\":{},\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
                            + "\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}}}"
                    ),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("detector", responseMap);
        assertEquals("invalid detector Name", CommonErrorMessages.INVALID_DETECTOR_NAME, messageMap.get("name").get("message"));
    }

    public void testValidateAnomalyDetectorWithFeatureQueryReturningNoData() throws Exception {
        Feature emptyFeature = TestHelpers.randomFeature("f-empty", "cpu", "avg", true);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(emptyFeature));
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"timestamp\": " + Instant.now().toEpochMilli() + "}"));
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate/detector",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("detector", responseMap);
        assertEquals(
            "empty data",
            AbstractAnomalyDetectorActionHandler.FEATURE_WITH_EMPTY_DATA_MSG + "f-empty",
            messageMap.get("feature_attributes").get("message")
        );
    }

    public void testValidateAnomalyDetectorWithFeatureQueryRuntimeException() throws Exception {
        String nonNumericField = "_type";
        Feature nonNumericFeature = TestHelpers.randomFeature("non-numeric-feature", nonNumericField, "avg", true);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(nonNumericFeature));
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"timestamp\": " + Instant.now().toEpochMilli() + "}"));
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate/detector",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("detector", responseMap);
        assertEquals(
            "runtime exception",
            AbstractAnomalyDetectorActionHandler.FEATURE_WITH_INVALID_QUERY_MSG + "non-numeric-feature",
            messageMap.get("feature_attributes").get("message")
        );
    }

    public void testValidateAnomalyDetectorWithWrongCategoryField() throws Exception {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), Arrays.asList("host.keyword"));
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, TestHelpers.toHttpEntity("{\"timestamp\": " + Instant.now().toEpochMilli() + "}"));
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate/detector",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("detector", responseMap);
        assertEquals(
            "non-existing category",
            String.format(AbstractAnomalyDetectorActionHandler.CATEGORY_NOT_FOUND_ERR_MSG, "host.keyword"),
            messageMap.get("category_field").get("message")
        );

    }
}
