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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.junit.Assert;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorRestTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorExecutionInput;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
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
                    .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), toHttpEntity(detector), null)
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
                toHttpEntity("{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":" + "{\"field1\":{\"type\":\"text\"}}}}"),
                null
            );

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Can't create anomaly detector as no document found in indices",
                () -> TestHelpers
                    .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), toHttpEntity(detector), null)
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
            randomIntBetween(1, 2000),
            TestHelpers.randomUiMetadata(),
            randomInt(),
            null,
            null,
            TestHelpers.randomUser()
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
                        toHttpEntity(detectorDuplicateName),
                        null
                    )
            );
    }

    public void testCreateAnomalyDetector() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        String indexName = detector.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, toHttpEntity("{\"name\": \"test\"}"));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), toHttpEntity(detector), null)
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);
        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), toHttpEntity(detector), null);
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        int version = (int) responseMap.get("_version");
        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
        assertTrue("incorrect version", version > 0);
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

    public void testUpdateAnomalyDetectorA() throws Exception {
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
            detector.getUser()
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
                    toHttpEntity(newDetector),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response updateResponse = TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "?refresh=true",
                ImmutableMap.of(),
                toHttpEntity(newDetector),
                null
            );

        assertEquals("Update anomaly detector failed", RestStatus.OK, restStatus(updateResponse));
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
            detector1.getUser()
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
                        toHttpEntity(newDetector1WithDetector2Name),
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
            detector.getUser()
        );

        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "?refresh=true",
                ImmutableMap.of(),
                toHttpEntity(detectorWithNewName),
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
            detector.getUser()
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
                        toHttpEntity(newDetector),
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
        assertEquals("Search anomaly detector failed", RestStatus.OK, restStatus(searchResponse));
    }

    public void testStatsAnomalyDetector() throws Exception {
        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, false);
        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers.makeRequest(client(), "GET", AnomalyDetectorPlugin.AD_BASE_URI + "/stats", ImmutableMap.of(), "", null)
        );
        assertThat(ex.getMessage(), containsString(CommonErrorMessages.DISABLED_ERR_MSG));

        updateClusterSettings(EnabledSetting.AD_PLUGIN_ENABLED, true);

        Response statsResponse = TestHelpers
            .makeRequest(client(), "GET", AnomalyDetectorPlugin.AD_BASE_URI + "/stats", ImmutableMap.of(), "", null);

        assertEquals("Get stats failed", RestStatus.OK, restStatus(statsResponse));
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
                    toHttpEntity(input),
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
                toHttpEntity(input),
                null
            );
        assertEquals("Execute anomaly detector failed", RestStatus.OK, restStatus(response));
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
                        toHttpEntity(input),
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
                        toHttpEntity(input),
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
                toHttpEntity(input),
                null,
                false
            );
        assertEquals("Execute anomaly detector failed", RestStatus.OK, restStatus(response));
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
                        toHttpEntity(input),
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
                toHttpEntity(anomalyResult),
                null,
                false
            );
        assertEquals("Post anomaly result failed", RestStatus.CREATED, restStatus(response));

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
        assertEquals("Search anomaly result failed", RestStatus.OK, restStatus(searchResponse));

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
        assertEquals("Search anomaly result failed", RestStatus.OK, restStatus(searchAllResponse));
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
        assertEquals("Delete anomaly detector failed", RestStatus.OK, restStatus(response));
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
        assertEquals("Delete anomaly detector failed", RestStatus.OK, restStatus(response));
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

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

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

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

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
            detector.getUser()
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
                        toHttpEntity(newDetector),
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

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

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

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));
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
                "AnomalyDetector is not found",
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
        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

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
        assertEquals("Fail to stop AD job", RestStatus.OK, restStatus(stopAdJobResponse));

        stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, restStatus(stopAdJobResponse));
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
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "AnomalyDetector is not found",
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
        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));

        Response stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, restStatus(stopAdJobResponse));

        startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getDetectorId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, restStatus(startAdJobResponse));
    }

    public void testStartAdjobWithNullFeatures() throws Exception {
        AnomalyDetector detectorWithoutFeature = TestHelpers.randomAnomalyDetector(null, null, Instant.now());
        String indexName = detectorWithoutFeature.getIndices().get(0);
        TestHelpers.createIndex(client(), indexName, toHttpEntity("{\"name\": \"test\"}"));
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
        TestHelpers.createIndex(client(), indexName, toHttpEntity("{\"name\": \"test\"}"));
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
        assertEquals("Incorrect profile status", RestStatus.OK, restStatus(profileResponse));
    }

    public void testAllProfileAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        Response profileResponse = getDetectorProfile(detector.getDetectorId(), true);
        assertEquals("Incorrect profile status", RestStatus.OK, restStatus(profileResponse));
    }

    public void testCustomizedProfileAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        Response profileResponse = getDetectorProfile(detector.getDetectorId(), true, "/models/", client());
        assertEquals("Incorrect profile status", RestStatus.OK, restStatus(profileResponse));
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
        ResponseException e = expectThrows(ResponseException.class, () -> startAnomalyDetector(detector.getDetectorId(), client()));
        assertTrue(e.getMessage().contains("Can't start detector job as no enabled features configured"));
    }

    public void testDeleteAnomalyDetectorWhileRunning() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        Assert.assertNotNull(detector.getDetectorId());
        Response response = startAnomalyDetector(detector.getDetectorId(), client());
        Assert.assertEquals(response.getStatusLine().toString(), "HTTP/1.1 200 OK");

        // Deleting detector should fail while its running
        Exception exception = expectThrows(IOException.class, () -> { deleteAnomalyDetector(detector.getDetectorId(), client()); });
        Assert.assertTrue(exception.getMessage().contains("Detector job is running"));
    }
}
