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
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.DUPLICATE_DETECTOR_MSG;
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.rest.handler.AbstractTimeSeriesActionHandler;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.RestHandlerUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AnomalyDetectorRestApiIT extends AnomalyDetectorRestTestCase {

    protected static final String INDEX_NAME = "indexname";
    protected static final String TIME_FIELD = "timestamp";

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

    private AnomalyDetector createIndexAndGetAnomalyDetector(String indexName) throws IOException {
        return createIndexAndGetAnomalyDetector(indexName, ImmutableList.of(TestHelpers.randomFeature(true)));
    }

    private AnomalyDetector createIndexAndGetAnomalyDetector(String indexName, List<Feature> features) throws IOException {
        return createIndexAndGetAnomalyDetector(indexName, features, false);
    }

    private AnomalyDetector createIndexAndGetAnomalyDetector(String indexName, List<Feature> features, boolean useDateNanos)
        throws IOException {
        TestHelpers.createIndexWithTimeField(client(), indexName, TIME_FIELD, useDateNanos);
        String testIndexData = "{\"keyword-field\": \"field-1\", \"ip-field\": \"1.2.3.4\", \"timestamp\": 1}";
        TestHelpers.ingestDataToIndex(client(), indexName, TestHelpers.toHttpEntity(testIndexData));
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TIME_FIELD, indexName, features);
        return detector;
    }

    public void testCreateAnomalyDetectorWithDuplicateName() throws Exception {
        AnomalyDetector detector = createIndexAndGetAnomalyDetector(INDEX_NAME);
        Feature feature = TestHelpers.randomFeature();
        List<Feature> featureList = ImmutableList.of(feature);
        AnomalyDetector detectorDuplicateName = new AnomalyDetector(
            AnomalyDetector.NO_ID,
            randomLong(),
            detector.getName(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            detector.getIndices(),
            featureList,
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE),
            TestHelpers.randomUiMetadata(),
            randomInt(),
            null,
            null,
            TestHelpers.randomUser(),
            null,
            TestHelpers.randomImputationOption(featureList),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
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
        AnomalyDetector detector = createIndexAndGetAnomalyDetector(INDEX_NAME);
        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

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
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), TestHelpers.toHttpEntity(detector), null);
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        int version = (int) responseMap.get("_version");
        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
        assertTrue("incorrect version", version > 0);

        // users cannot specify detector id when creating a detector
        AnomalyDetector detector2 = createIndexAndGetAnomalyDetector(INDEX_NAME);
        String blahId = "__blah__";
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI,
                ImmutableMap.of(RestHandlerUtils.DETECTOR_ID, blahId),
                TestHelpers.toHttpEntity(detector2),
                null
            );
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        id = (String) responseMap.get("_id");
        assertNotEquals("response is missing Id", blahId, id);
    }

    public void testCreateAnomalyDetectorWithDateNanos() throws Exception {
        AnomalyDetector detector = createIndexAndGetAnomalyDetector(INDEX_NAME, ImmutableList.of(TestHelpers.randomFeature(true)), true);
        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

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
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
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
        AnomalyDetector detector = createIndexAndGetAnomalyDetector(INDEX_NAME);
        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), TestHelpers.toHttpEntity(detector), null);
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        List<Feature> features = detector.getFeatureAttributes();
        long expectedFeatures = features.stream().filter(Feature::getEnabled).count();
        AnomalyDetector newDetector = new AnomalyDetector(
            id,
            null,
            detector.getName(),
            detector.getDescription(),
            detector.getTimeField(),
            detector.getIndices(),
            features,
            detector.getFilterQuery(),
            detector.getInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime(),
            ImmutableList.of(randomAlphaOfLength(5)),
            detector.getUser(),
            null,
            TestHelpers.randomImputationOption(features),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            detector.getLastBreakingUIChangeTime()
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
        assertThat(ex.getMessage(), containsString(CommonMessages.CAN_NOT_CHANGE_CATEGORY_FIELD));
    }

    public void testGetAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

        Exception ex = expectThrows(ResponseException.class, () -> getConfig(detector.getId(), client()));
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);

        AnomalyDetector createdDetector = getConfig(detector.getId(), client());
        assertEquals("Incorrect Location header", detector, createdDetector);
    }

    public void testGetNotExistingAnomalyDetector() throws Exception {
        createRandomAnomalyDetector(true, true, client());
        TestHelpers.assertFailWith(ResponseException.class, null, () -> getConfig(randomAlphaOfLength(5), client()));
    }

    public void testUpdateAnomalyDetector() throws Exception {
        AnomalyDetector detector = createAnomalyDetector(createIndexAndGetAnomalyDetector(INDEX_NAME), true, client());
        String newDescription = randomAlphaOfLength(5);
        List<Feature> features = detector.getFeatureAttributes();
        long expectedFeatures = features.stream().filter(Feature::getEnabled).count();
        AnomalyDetector newDetector = new AnomalyDetector(
            detector.getId(),
            detector.getVersion(),
            detector.getName(),
            newDescription,
            detector.getTimeField(),
            detector.getIndices(),
            features,
            detector.getFilterQuery(),
            detector.getInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime(),
            null,
            detector.getUser(),
            null,
            TestHelpers.randomImputationOption(features),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            detector.getLastBreakingUIChangeTime()
        );

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "PUT",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "?refresh=true",
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(newDetector),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);

        Response updateResponse = TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "?refresh=true",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(newDetector),
                null
            );

        assertEquals("Update anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(updateResponse));
        Map<String, Object> responseBody = entityAsMap(updateResponse);
        assertEquals("Updated anomaly detector id doesn't match", detector.getId(), responseBody.get("_id"));
        assertEquals("Version not incremented", (detector.getVersion().intValue() + 1), (int) responseBody.get("_version"));

        AnomalyDetector updatedDetector = getConfig(detector.getId(), client());
        assertNotEquals("Anomaly detector last update time not changed", updatedDetector.getLastUpdateTime(), detector.getLastUpdateTime());
        assertEquals("Anomaly detector description not updated", newDescription, updatedDetector.getDescription());
    }

    public void testUpdateAnomalyDetectorNameToExisting() throws Exception {
        AnomalyDetector detector1 = createIndexAndGetAnomalyDetector("index-test-one");
        AnomalyDetector detector2 = createIndexAndGetAnomalyDetector("index-test-two");
        List<Feature> features = detector1.getFeatureAttributes();
        long expectedFeatures = features.stream().filter(Feature::getEnabled).count();
        AnomalyDetector newDetector1WithDetector2Name = new AnomalyDetector(
            detector1.getId(),
            detector1.getVersion(),
            detector2.getName(),
            detector1.getDescription(),
            detector1.getTimeField(),
            detector1.getIndices(),
            features,
            detector1.getFilterQuery(),
            detector1.getInterval(),
            detector1.getWindowDelay(),
            detector1.getShingleSize(),
            detector1.getUiMetadata(),
            detector1.getSchemaVersion(),
            detector1.getLastUpdateTime(),
            null,
            detector1.getUser(),
            null,
            TestHelpers.randomImputationOption(features),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            detector1.getLastBreakingUIChangeTime()
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
        AnomalyDetector detector = createAnomalyDetector(createIndexAndGetAnomalyDetector(INDEX_NAME), true, client());
        List<Feature> features = detector.getFeatureAttributes();
        long expectedFeatures = features.stream().filter(Feature::getEnabled).count();
        AnomalyDetector detectorWithNewName = new AnomalyDetector(
            detector.getId(),
            detector.getVersion(),
            randomAlphaOfLength(5),
            detector.getDescription(),
            detector.getTimeField(),
            detector.getIndices(),
            features,
            detector.getFilterQuery(),
            detector.getInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            Instant.now(),
            null,
            detector.getUser(),
            null,
            TestHelpers.randomImputationOption(features),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now()
        );

        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "?refresh=true",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detectorWithNewName),
                null
            );

        AnomalyDetector resultDetector = getConfig(detectorWithNewName.getId(), client());
        assertEquals("Detector name updating failed", detectorWithNewName.getName(), resultDetector.getName());
        assertEquals("Updated anomaly detector id doesn't match", detectorWithNewName.getId(), resultDetector.getId());
        assertNotEquals(
            "Anomaly detector last update time not changed",
            detectorWithNewName.getLastUpdateTime(),
            resultDetector.getLastUpdateTime()
        );
    }

    public void testUpdateAnomalyDetectorWithNotExistingIndex() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        String newDescription = randomAlphaOfLength(5);
        List<Feature> features = detector.getFeatureAttributes();
        long expectedFeatures = features.stream().filter(Feature::getEnabled).count();
        AnomalyDetector newDetector = new AnomalyDetector(
            detector.getId(),
            detector.getVersion(),
            detector.getName(),
            newDescription,
            detector.getTimeField(),
            detector.getIndices(),
            features,
            detector.getFilterQuery(),
            detector.getInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime(),
            null,
            detector.getUser(),
            null,
            TestHelpers.randomImputationOption(features),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            detector.getLastBreakingUIChangeTime()
        );

        deleteIndexWithAdminClient(CommonName.CONFIG_INDEX);

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                null,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "PUT",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId(),
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(newDetector),
                        null
                    )
            );
    }

    public void testSearchAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        SearchSourceBuilder search = (new SearchSourceBuilder()).query(QueryBuilders.termQuery("_id", detector.getId()));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "GET",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/_search",
                    ImmutableMap.of(),
                    new StringEntity(search.toString(), ContentType.APPLICATION_JSON),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);

        Response searchResponse = TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_search",
                ImmutableMap.of(),
                new StringEntity(search.toString(), ContentType.APPLICATION_JSON),
                null
            );
        assertEquals("Search anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(searchResponse));
    }

    public void testStatsAnomalyDetector() throws Exception {
        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);
        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers.makeRequest(client(), "GET", TimeSeriesAnalyticsPlugin.LEGACY_AD_BASE + "/stats", ImmutableMap.of(), "", null)
        );
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);

        Response statsResponse = TestHelpers
            .makeRequest(client(), "GET", TimeSeriesAnalyticsPlugin.LEGACY_AD_BASE + "/stats", ImmutableMap.of(), "", null);

        assertEquals("Get stats failed", RestStatus.OK, TestHelpers.restStatus(statsResponse));
    }

    public void testPreviewAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            detector.getId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    String.format(Locale.ROOT, TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(input),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
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
                        String.format(Locale.ROOT, TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
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
                        String.format(Locale.ROOT, TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(input),
                        null
                    )
            );
    }

    public void testPreviewAnomalyDetectorWithDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            detector.getId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            detector
        );
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
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
            detector.getId(),
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
                        String.format(Locale.ROOT, TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
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

        SearchSourceBuilder search = (new SearchSourceBuilder()).query(QueryBuilders.termQuery("detector_id", anomalyResult.getConfigId()));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_RESULT_URI + "/_search",
                    ImmutableMap.of(),
                    new StringEntity(search.toString(), ContentType.APPLICATION_JSON),
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);

        Response searchResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_RESULT_URI + "/_search",
                ImmutableMap.of(),
                new StringEntity(search.toString(), ContentType.APPLICATION_JSON),
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
                new StringEntity(searchAll.toString(), ContentType.APPLICATION_JSON),
                null
            );
        assertEquals("Search anomaly result failed", RestStatus.OK, TestHelpers.restStatus(searchAllResponse));
    }

    public void testDeleteAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(client(), "DELETE", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId(), ImmutableMap.of(), "", null)
        );
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        Response response = TestHelpers
            .makeRequest(client(), "DELETE", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId(), ImmutableMap.of(), "", null);
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
            .makeRequest(client(), "DELETE", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId(), ImmutableMap.of(), "", null);
        assertEquals("Delete anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testDeleteAnomalyDetectorWithRunningAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Job is running",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "DELETE",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId(),
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testUpdateAnomalyDetectorWithRunningAdJob() throws Exception {
        AnomalyDetector detector = createAnomalyDetector(createIndexAndGetAnomalyDetector(INDEX_NAME), true, client());
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        String newDescription = randomAlphaOfLength(5);
        List<Feature> features = detector.getFeatureAttributes();
        long expectedFeatures = features.stream().filter(Feature::getEnabled).count();
        AnomalyDetector newDetector = new AnomalyDetector(
            detector.getId(),
            detector.getVersion(),
            detector.getName(),
            newDescription,
            detector.getTimeField(),
            detector.getIndices(),
            features,
            detector.getFilterQuery(),
            detector.getInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime(),
            null,
            detector.getUser(),
            null,
            TestHelpers.randomImputationOption(features),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            detector.getLastBreakingUIChangeTime()
        );

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Job is running",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "PUT",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId(),
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(newDetector),
                        null
                    )
            );
    }

    public void testGetDetectorWithAdJob() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        ToXContentObject[] results = getConfig(detector.getId(), true, client());
        assertEquals("Incorrect Location header", detector, results[0]);
        assertEquals("Incorrect detector job name", detector.getId(), ((Job) results[1]).getName());
        assertTrue(((Job) results[1]).isEnabled());

        results = getConfig(detector.getId(), false, client());
        assertEquals("Incorrect Location header", detector, results[0]);
        assertEquals("Should not return detector job", null, results[1]);
    }

    public void testStartAdJobWithExistingDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
                    ImmutableMap.of(),
                    "",
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
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
                CommonMessages.FAIL_TO_FIND_CONFIG_MSG,
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
        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);
        AnomalyDetector detector = createRandomAnomalyDetector(true, false, client());
        Response startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_stop",
                    ImmutableMap.of(),
                    "",
                    null
                )
        );
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);

        Response stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, TestHelpers.restStatus(stopAdJobResponse));

        stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_stop",
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
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_stop",
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
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        TestHelpers
            .assertFailWith(
                ResponseException.class,
                CommonMessages.FAIL_TO_FIND_CONFIG_MSG,
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
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to start AD job", RestStatus.OK, TestHelpers.restStatus(startAdJobResponse));

        Response stopAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_stop",
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Fail to stop AD job", RestStatus.OK, TestHelpers.restStatus(stopAdJobResponse));

        startAdJobResponse = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
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
                "Can't start job as no features configured",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
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
                "Can't start job as no features configured",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detector.getId() + "/_start",
                        ImmutableMap.of(),
                        "",
                        null
                    )
            );
    }

    public void testDefaultProfileAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, false);

        Exception ex = expectThrows(ResponseException.class, () -> getDetectorProfile(detector.getId()));
        assertThat(ex.getMessage(), containsString(ADCommonMessages.DISABLED_ERR_MSG));

        updateClusterSettings(ADEnabledSetting.AD_ENABLED, true);

        Response profileResponse = getDetectorProfile(detector.getId());
        assertEquals("Incorrect profile status", RestStatus.OK, TestHelpers.restStatus(profileResponse));
    }

    public void testAllProfileAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        Response profileResponse = getDetectorProfile(detector.getId(), true);
        assertEquals("Incorrect profile status", RestStatus.OK, TestHelpers.restStatus(profileResponse));
    }

    public void testCustomizedProfileAnomalyDetector() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        Response profileResponse = getDetectorProfile(detector.getId(), true, "/models/", client());
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
        Assert.assertNotNull(detector.getId());
        Instant now = Instant.now();
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> startAnomalyDetector(detector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), client())
        );
        assertTrue(e.getMessage().contains("Can't start job as no enabled features configured"));
    }

    public void testDeleteAnomalyDetectorWhileRunning() throws Exception {
        AnomalyDetector detector = createRandomAnomalyDetector(true, true, client());
        Assert.assertNotNull(detector.getId());
        Instant now = Instant.now();
        Response response = startAnomalyDetector(detector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), client());
        org.hamcrest.MatcherAssert.assertThat(response.getStatusLine().toString(), CoreMatchers.containsString("200 OK"));

        // Deleting detector should fail while its running
        Exception exception = expectThrows(IOException.class, () -> { deleteAnomalyDetector(detector.getId(), client()); });
        Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains("Historical is running"));
    }

    public void testBackwardCompatibilityWithOpenDistro() throws IOException {
        // Create a detector
        AnomalyDetector detector = createIndexAndGetAnomalyDetector(INDEX_NAME);
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
        AnomalyDetector createdDetector = getConfig(id, client());
        assertEquals("Get anomaly detector failed", createdDetector.getId(), id);

        // Delete the detector using legacy _opendistro API
        response = TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + createdDetector.getId(),
                ImmutableMap.of(),
                "",
                null
            );
        assertEquals("Delete anomaly detector failed", RestStatus.OK, TestHelpers.restStatus(response));

    }

    public void testValidateAnomalyDetectorWithDuplicateName() throws Exception {
        AnomalyDetector detector = createAnomalyDetector(createIndexAndGetAnomalyDetector(INDEX_NAME), true, client());
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
                            + "\"indices\":[\""
                            + INDEX_NAME
                            + "\"],\"feature_attributes\":[{\"feature_name\":\"cpu-sum\",\""
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
        String errorMsg = String.format(Locale.ROOT, DUPLICATE_DETECTOR_MSG, detector.getName(), "[" + detector.getId() + "]");
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
        assertEquals("time field missing", CommonMessages.NULL_TIME_FIELD, messageMap.get("time_field").get("message"));
    }

    public void testValidateAnomalyDetectorWithMultipleIndicesOneNotFound() throws Exception {
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
                            + "\"indices\":[\"test-index\", \"test-index-2\"],\"feature_attributes\":[{\"feature_name\":\"cpu-sum\",\""
                            + "feature_enabled\":true,\"aggregation_query\":{\"total_cpu\":{\"sum\":{\"field\":\"cpu\"}}}},"
                            + "{\"feature_name\":\"error-sum\",\"feature_enabled\":true,\"aggregation_query\":"
                            + "{\"total_error\":"
                            + "{\"sum\":{\"field\":\"error\"}}}}],\"filter_query\":{\"bool\":{\"filter\":[{\"exists\":"
                            + "{\"field\":"
                            + "\"cpu\",\"boost\":1}}],\"adjust_pure_negative\":true,\"boost\":1}},\"detection_interval\":"
                            + "{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
                            + "\"window_delay\":{\"period\":{\"interval\":2,\"unit\":\"Minutes\"}},"
                            + "\"shingle_size\": 8}"
                    ),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);

        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("detector", responseMap);
        String errorMessage = "index does not exist";
        assertEquals("index does not exist", errorMessage, messageMap.get("indices").get("message"));
    }

    public void testValidateAnomalyDetectorWithMultipleIndices() throws Exception {
        TestHelpers.createIndexWithTimeField(client(), "test-index", TIME_FIELD);
        TestHelpers.createIndexWithTimeField(client(), "test-index-2", TIME_FIELD);

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
                            + "\"indices\":[\"test-index\", \"test-index-2\"],\"feature_attributes\":[{\"feature_name\":\"cpu-sum\",\""
                            + "feature_enabled\":true,\"aggregation_query\":{\"total_cpu\":{\"sum\":{\"field\":\"cpu\"}}}},"
                            + "{\"feature_name\":\"error-sum\",\"feature_enabled\":true,\"aggregation_query\":"
                            + "{\"total_error\":"
                            + "{\"sum\":{\"field\":\"error\"}}}}],\"filter_query\":{\"bool\":{\"filter\":[{\"exists\":"
                            + "{\"field\":"
                            + "\"cpu\",\"boost\":1}}],\"adjust_pure_negative\":true,\"boost\":1}},\"detection_interval\":"
                            + "{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
                            + "\"window_delay\":{\"period\":{\"interval\":2,\"unit\":\"Minutes\"}},"
                            + "\"shingle_size\": 8}"
                    ),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        assertEquals("no issue, empty response body", new HashMap<String, Object>(), responseMap);
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
            + TimeSeriesSettings.MAX_SHINGLE_SIZE
            + ". Got 2000";
        assertEquals("shingle size error message", errorMessage, messageMap.get("shingle_size").get("message"));
    }

    public void testValidateAnomalyDetectorWithNoIssue() throws Exception {
        AnomalyDetector detector = createIndexAndGetAnomalyDetector(INDEX_NAME);
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
        AnomalyDetector detector = createIndexAndGetAnomalyDetector(INDEX_NAME);
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                CommonMessages.NOT_EXISTENT_VALIDATION_TYPE,
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/_validate/models",
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(detector),
                        null
                    )
            );
    }

    public void testValidateAnomalyDetectorWithEmptyIndices() throws Exception {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TIME_FIELD, INDEX_NAME);
        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/" + detector.getIndices().get(0),
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"settings\":{\"number_of_shards\":1},"
                            + " \"mappings\":{\"properties\":"
                            + "{\"timestamp\":{\"type\":\"date\"}}}}"
                            + "{\"field1\":{\"type\":\"text\"}}}}"
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
        String errorMessage = String.format(Locale.ROOT, NO_DOCS_IN_USER_INDEX_MSG, "[" + detector.getIndices().get(0) + "]");
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
        assertEquals("invalid detector Name", CommonMessages.INVALID_NAME, messageMap.get("name").get("message"));
    }

    public void testValidateAnomalyDetectorWithFeatureQueryReturningNoData() throws Exception {
        Feature emptyFeature = TestHelpers.randomFeature("f-empty", "cpu", "avg", true);
        AnomalyDetector detector = createIndexAndGetAnomalyDetector(INDEX_NAME, ImmutableList.of(emptyFeature));
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
            CommonMessages.FEATURE_WITH_EMPTY_DATA_MSG + "f-empty",
            messageMap.get("feature_attributes").get("message")
        );
    }

    public void testValidateAnomalyDetectorWithFeatureQueryRuntimeException() throws Exception {
        Feature nonNumericFeature = TestHelpers.randomFeature("non-numeric-feature", "_index", "avg", true);
        AnomalyDetector detector = createIndexAndGetAnomalyDetector(INDEX_NAME, ImmutableList.of(nonNumericFeature));
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
            CommonMessages.FEATURE_WITH_INVALID_QUERY_MSG + "non-numeric-feature",
            messageMap.get("feature_attributes").get("message")
        );
    }

    public void testValidateAnomalyDetectorWithWrongCategoryField() throws Exception {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetectorUsingCategoryFields(
                randomAlphaOfLength(5),
                TIME_FIELD,
                ImmutableList.of("index-test"),
                Arrays.asList("host.keyword")
            );
        TestHelpers.createIndexWithTimeField(client(), "index-test", TIME_FIELD);
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
            String.format(Locale.ROOT, AbstractTimeSeriesActionHandler.CATEGORY_NOT_FOUND_ERR_MSG, "host.keyword", "[index-test]"),
            messageMap.get("category_field").get("message")
        );

    }

    public void testSearchTopAnomalyResultsWithInvalidInputs() throws IOException {
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        Map<String, String> categoryFieldsAndTypes = new HashMap<String, String>() {
            {
                put("keyword-field", "keyword");
                put("ip-field", "ip");
            }
        };
        String testIndexData = "{\"keyword-field\": \"field-1\", \"ip-field\": \"1.2.3.4\", \"timestamp\": 1}";
        TestHelpers.createIndexWithHCADFields(client(), indexName, categoryFieldsAndTypes);
        TestHelpers.ingestDataToIndex(client(), indexName, TestHelpers.toHttpEntity(testIndexData));
        AnomalyDetector detector = createAnomalyDetector(
            TestHelpers
                .randomAnomalyDetectorUsingCategoryFields(
                    randomAlphaOfLength(10),
                    TIME_FIELD,
                    ImmutableList.of(indexName),
                    categoryFieldsAndTypes.keySet().stream().collect(Collectors.toList())
                ),
            true,
            client()
        );

        // Missing start time
        Exception missingStartTimeException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(detector.getId(), false, "{\"end_time_ms\":2}", client());
        });
        assertTrue(missingStartTimeException.getMessage().contains("Must set both start time and end time with epoch of milliseconds"));

        // Missing end time
        Exception missingEndTimeException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(detector.getId(), false, "{\"start_time_ms\":1}", client());
        });
        assertTrue(missingEndTimeException.getMessage().contains("Must set both start time and end time with epoch of milliseconds"));

        // Start time > end time
        Exception invalidTimeException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(detector.getId(), false, "{\"start_time_ms\":2, \"end_time_ms\":1}", client());
        });
        assertTrue(invalidTimeException.getMessage().contains("Start time should be before end time"));

        // Invalid detector ID
        Exception invalidDetectorIdException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(detector.getId() + "-invalid", false, "{\"start_time_ms\":1, \"end_time_ms\":2}", client());
        });
        assertTrue(invalidDetectorIdException.getMessage().contains("Can't find config with id"));

        // Invalid order field
        Exception invalidOrderException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(
                detector.getId(),
                false,
                "{\"start_time_ms\":1, \"end_time_ms\":2, \"order\":\"invalid-order\"}",
                client()
            );
        });
        assertTrue(invalidOrderException.getMessage().contains("Ordering by invalid-order is not a valid option"));

        // Negative size field
        Exception negativeSizeException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(detector.getId(), false, "{\"start_time_ms\":1, \"end_time_ms\":2, \"size\":-1}", client());
        });
        assertTrue(negativeSizeException.getMessage().contains("Size must be a positive integer"));

        // Zero size field
        Exception zeroSizeException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(detector.getId(), false, "{\"start_time_ms\":1, \"end_time_ms\":2, \"size\":0}", client());
        });
        assertTrue(zeroSizeException.getMessage().contains("Size must be a positive integer"));

        // Too large size field
        Exception tooLargeSizeException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(detector.getId(), false, "{\"start_time_ms\":1, \"end_time_ms\":2, \"size\":9999999}", client());
        });
        assertTrue(tooLargeSizeException.getMessage().contains("Size cannot exceed"));

        // No existing task ID for detector
        Exception noTaskIdException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(detector.getId(), true, "{\"start_time_ms\":1, \"end_time_ms\":2}", client());
        });
        assertTrue(noTaskIdException.getMessage().contains("No historical tasks found for detector ID " + detector.getId()));

        // Invalid category fields
        Exception invalidCategoryFieldsException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(
                detector.getId(),
                false,
                "{\"start_time_ms\":1, \"end_time_ms\":2, \"category_field\":[\"invalid-field\"]}",
                client()
            );
        });
        assertTrue(
            invalidCategoryFieldsException
                .getMessage()
                .contains("Category field invalid-field doesn't exist for detector ID " + detector.getId())
        );

        // Using detector with no category fields
        AnomalyDetector detectorWithNoCategoryFields = createAnomalyDetector(
            TestHelpers
                .randomAnomalyDetectorUsingCategoryFields(
                    randomAlphaOfLength(10),
                    TIME_FIELD,
                    ImmutableList.of(indexName),
                    ImmutableList.of()
                ),
            true,
            client()
        );
        Exception noCategoryFieldsException = expectThrows(IOException.class, () -> {
            searchTopAnomalyResults(detectorWithNoCategoryFields.getId(), false, "{\"start_time_ms\":1, \"end_time_ms\":2}", client());
        });
        assertTrue(
            noCategoryFieldsException
                .getMessage()
                .contains("No category fields found for detector ID " + detectorWithNoCategoryFields.getId())
        );
    }

    public void testSearchTopAnomalyResultsOnNonExistentResultIndex() throws IOException {
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        Map<String, String> categoryFieldsAndTypes = new HashMap<String, String>() {
            {
                put("keyword-field", "keyword");
                put("ip-field", "ip");
            }
        };
        String testIndexData = "{\"keyword-field\": \"test-value\"}";
        TestHelpers.createIndexWithHCADFields(client(), indexName, categoryFieldsAndTypes);
        TestHelpers.ingestDataToIndex(client(), indexName, TestHelpers.toHttpEntity(testIndexData));
        AnomalyDetector detector = createAnomalyDetector(
            TestHelpers
                .randomAnomalyDetectorUsingCategoryFields(
                    randomAlphaOfLength(10),
                    TIME_FIELD,
                    ImmutableList.of(indexName),
                    categoryFieldsAndTypes.keySet().stream().collect(Collectors.toList())
                ),
            true,
            client()
        );

        // Delete any existing result index
        if (indexExistsWithAdminClient(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
            deleteIndexWithAdminClient(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS + "*");
        }
        Response response = searchTopAnomalyResults(
            detector.getId(),
            false,
            "{\"size\":3,\"category_field\":[\"keyword-field\"]," + "\"start_time_ms\":0, \"end_time_ms\":1}",
            client()
        );
        Map<String, Object> responseMap = entityAsMap(response);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> buckets = (ArrayList<Map<String, Object>>) XContentMapValues.extractValue("buckets", responseMap);
        assertEquals(0, buckets.size());
    }

    public void testSearchTopAnomalyResultsOnEmptyResultIndex() throws IOException {
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        Map<String, String> categoryFieldsAndTypes = new HashMap<String, String>() {
            {
                put("keyword-field", "keyword");
                put("ip-field", "ip");
            }
        };
        String testIndexData = "{\"keyword-field\": \"test-value\"}";
        TestHelpers.createIndexWithHCADFields(client(), indexName, categoryFieldsAndTypes);
        TestHelpers.ingestDataToIndex(client(), indexName, TestHelpers.toHttpEntity(testIndexData));
        AnomalyDetector detector = createAnomalyDetector(
            TestHelpers
                .randomAnomalyDetectorUsingCategoryFields(
                    randomAlphaOfLength(10),
                    TIME_FIELD,
                    ImmutableList.of(indexName),
                    categoryFieldsAndTypes.keySet().stream().collect(Collectors.toList())
                ),
            true,
            client()
        );

        if (indexExistsWithAdminClient(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
            deleteIndexWithAdminClient(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS + "*");
        }
        TestHelpers.createEmptyAnomalyResultIndex(adminClient());
        Response response = searchTopAnomalyResults(
            detector.getId(),
            false,
            "{\"size\":3,\"category_field\":[\"keyword-field\"]," + "\"start_time_ms\":0, \"end_time_ms\":1}",
            client()
        );
        Map<String, Object> responseMap = entityAsMap(response);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> buckets = (ArrayList<Map<String, Object>>) XContentMapValues.extractValue("buckets", responseMap);
        assertEquals(0, buckets.size());
    }

    public void testSearchTopAnomalyResultsOnPopulatedResultIndex() throws IOException {
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        Map<String, String> categoryFieldsAndTypes = new HashMap<String, String>() {
            {
                put("keyword-field", "keyword");
                put("ip-field", "ip");
            }
        };
        String testIndexData = "{\"keyword-field\": \"field-1\", \"ip-field\": \"1.2.3.4\", \"timestamp\": 1}";
        TestHelpers.createIndexWithHCADFields(client(), indexName, categoryFieldsAndTypes);
        TestHelpers.ingestDataToIndex(client(), indexName, TestHelpers.toHttpEntity(testIndexData));
        AnomalyDetector detector = createAnomalyDetector(
            TestHelpers
                .randomAnomalyDetectorUsingCategoryFields(
                    randomAlphaOfLength(10),
                    TIME_FIELD,
                    ImmutableList.of(indexName),
                    categoryFieldsAndTypes.keySet().stream().collect(Collectors.toList())
                ),
            true,
            client()
        );

        // Ingest some sample results
        if (!indexExistsWithAdminClient(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
            TestHelpers.createEmptyAnomalyResultIndex(adminClient());
        }
        Map<String, Object> entityAttrs1 = new HashMap<String, Object>() {
            {
                put("keyword-field", "field-1");
                put("ip-field", "1.2.3.4");
            }
        };
        Map<String, Object> entityAttrs2 = new HashMap<String, Object>() {
            {
                put("keyword-field", "field-2");
                put("ip-field", "5.6.7.8");
            }
        };
        Map<String, Object> entityAttrs3 = new HashMap<String, Object>() {
            {
                put("keyword-field", "field-2");
                put("ip-field", "5.6.7.8");
            }
        };
        AnomalyResult anomalyResult1 = TestHelpers
            .randomHCADAnomalyDetectResult(detector.getId(), null, entityAttrs1, 0.5, 0.8, null, 5L, 5L);
        AnomalyResult anomalyResult2 = TestHelpers
            .randomHCADAnomalyDetectResult(detector.getId(), null, entityAttrs2, 0.5, 0.5, null, 5L, 5L);
        AnomalyResult anomalyResult3 = TestHelpers
            .randomHCADAnomalyDetectResult(detector.getId(), null, entityAttrs3, 0.5, 0.2, null, 5L, 5L);

        TestHelpers.ingestDataToIndex(adminClient(), ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, TestHelpers.toHttpEntity(anomalyResult1));
        TestHelpers.ingestDataToIndex(adminClient(), ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, TestHelpers.toHttpEntity(anomalyResult2));
        TestHelpers.ingestDataToIndex(adminClient(), ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, TestHelpers.toHttpEntity(anomalyResult3));

        // Sorting by severity
        Response severityResponse = searchTopAnomalyResults(
            detector.getId(),
            false,
            "{\"category_field\":[\"keyword-field\"]," + "\"start_time_ms\":0, \"end_time_ms\":10, \"order\":\"severity\"}",
            client()
        );
        Map<String, Object> severityResponseMap = entityAsMap(severityResponse);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> severityBuckets = (ArrayList<Map<String, Object>>) XContentMapValues
            .extractValue("buckets", severityResponseMap);
        assertEquals(2, severityBuckets.size());
        @SuppressWarnings("unchecked")
        Map<String, String> severityBucketKey1 = (Map<String, String>) severityBuckets.get(0).get("key");
        @SuppressWarnings("unchecked")
        Map<String, String> severityBucketKey2 = (Map<String, String>) severityBuckets.get(1).get("key");
        assertEquals("field-1", severityBucketKey1.get("keyword-field"));
        assertEquals("field-2", severityBucketKey2.get("keyword-field"));

        // Sorting by occurrence
        Response occurrenceResponse = searchTopAnomalyResults(
            detector.getId(),
            false,
            "{\"category_field\":[\"keyword-field\"]," + "\"start_time_ms\":0, \"end_time_ms\":10, \"order\":\"occurrence\"}",
            client()
        );
        Map<String, Object> occurrenceResponseMap = entityAsMap(occurrenceResponse);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> occurrenceBuckets = (ArrayList<Map<String, Object>>) XContentMapValues
            .extractValue("buckets", occurrenceResponseMap);
        assertEquals(2, occurrenceBuckets.size());
        @SuppressWarnings("unchecked")
        Map<String, String> occurrenceBucketKey1 = (Map<String, String>) occurrenceBuckets.get(0).get("key");
        @SuppressWarnings("unchecked")
        Map<String, String> occurrenceBucketKey2 = (Map<String, String>) occurrenceBuckets.get(1).get("key");
        assertEquals("field-2", occurrenceBucketKey1.get("keyword-field"));
        assertEquals("field-1", occurrenceBucketKey2.get("keyword-field"));

        // Sorting using all category fields
        Response allFieldsResponse = searchTopAnomalyResults(
            detector.getId(),
            false,
            "{\"category_field\":[\"keyword-field\", \"ip-field\"]," + "\"start_time_ms\":0, \"end_time_ms\":10, \"order\":\"severity\"}",
            client()
        );
        Map<String, Object> allFieldsResponseMap = entityAsMap(allFieldsResponse);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> allFieldsBuckets = (ArrayList<Map<String, Object>>) XContentMapValues
            .extractValue("buckets", allFieldsResponseMap);
        assertEquals(2, allFieldsBuckets.size());
        @SuppressWarnings("unchecked")
        Map<String, String> allFieldsBucketKey1 = (Map<String, String>) allFieldsBuckets.get(0).get("key");
        @SuppressWarnings("unchecked")
        Map<String, String> allFieldsBucketKey2 = (Map<String, String>) allFieldsBuckets.get(1).get("key");
        assertEquals("field-1", allFieldsBucketKey1.get("keyword-field"));
        assertEquals("1.2.3.4", allFieldsBucketKey1.get("ip-field"));
        assertEquals("field-2", allFieldsBucketKey2.get("keyword-field"));
        assertEquals("5.6.7.8", allFieldsBucketKey2.get("ip-field"));
    }

    public void testSearchTopAnomalyResultsWithCustomResultIndex() throws IOException {
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        String customResultIndexName = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        Map<String, String> categoryFieldsAndTypes = new HashMap<String, String>() {
            {
                put("keyword-field", "keyword");
                put("ip-field", "ip");
            }
        };
        String testIndexData = "{\"keyword-field\": \"field-1\", \"ip-field\": \"1.2.3.4\", \"timestamp\": 1}";
        TestHelpers.createIndexWithHCADFields(client(), indexName, categoryFieldsAndTypes);
        TestHelpers.ingestDataToIndex(client(), indexName, TestHelpers.toHttpEntity(testIndexData));
        AnomalyDetector detector = createAnomalyDetector(
            TestHelpers
                .randomAnomalyDetectorUsingCategoryFields(
                    randomAlphaOfLength(10),
                    TIME_FIELD,
                    ImmutableList.of(indexName),
                    categoryFieldsAndTypes.keySet().stream().collect(Collectors.toList()),
                    customResultIndexName
                ),
            true,
            client()
        );

        Map<String, Object> entityAttrs = new HashMap<String, Object>() {
            {
                put("keyword-field", "field-1");
                put("ip-field", "1.2.3.4");
            }
        };
        AnomalyResult anomalyResult = TestHelpers
            .randomHCADAnomalyDetectResult(detector.getId(), null, entityAttrs, 0.5, 0.8, null, 5L, 5L);
        TestHelpers.ingestDataToIndex(client(), customResultIndexName, TestHelpers.toHttpEntity(anomalyResult));

        Response response = searchTopAnomalyResults(detector.getId(), false, "{\"start_time_ms\":0, \"end_time_ms\":10}", client());
        Map<String, Object> responseMap = entityAsMap(response);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> buckets = (ArrayList<Map<String, Object>>) XContentMapValues.extractValue("buckets", responseMap);
        assertEquals(1, buckets.size());
        @SuppressWarnings("unchecked")
        Map<String, String> bucketKey1 = (Map<String, String>) buckets.get(0).get("key");
        assertEquals("field-1", bucketKey1.get("keyword-field"));
        assertEquals("1.2.3.4", bucketKey1.get("ip-field"));
    }
}
