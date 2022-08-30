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

import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;

public abstract class AnomalyDetectorRestTestCase extends ODFERestTestCase {

    public static final int MAX_RETRY_TIMES = 10;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(ImmutableList.of(AnomalyDetector.XCONTENT_REGISTRY));
    }

    @Override
    protected Settings restClientSettings() {
        return super.restClientSettings();
    }

    protected AnomalyDetector createRandomAnomalyDetector(Boolean refresh, Boolean withMetadata, String indexName, RestClient client)
        throws IOException {
        return createRandomAnomalyDetector(refresh, withMetadata, client, true, indexName);
    }

    protected AnomalyDetector createRandomAnomalyDetector(Boolean refresh, Boolean withMetadata, RestClient client) throws IOException {
        return createRandomAnomalyDetector(refresh, withMetadata, client, true);
    }

    protected AnomalyDetector createRandomAnomalyDetector(Boolean refresh, Boolean withMetadata, RestClient client, boolean featureEnabled)
        throws IOException {
        return createRandomAnomalyDetector(refresh, withMetadata, client, featureEnabled, null);
    }

    protected AnomalyDetector createRandomAnomalyDetector(
        Boolean refresh,
        Boolean withMetadata,
        RestClient client,
        boolean featureEnabled,
        String indexName
    ) throws IOException {
        Map<String, Object> uiMetadata = null;
        if (withMetadata) {
            uiMetadata = TestHelpers.randomUiMetadata();
        }

        AnomalyDetector detector = null;

        if (indexName == null) {
            detector = TestHelpers.randomAnomalyDetector(uiMetadata, null, featureEnabled);
            TestHelpers
                .makeRequest(
                    client,
                    "POST",
                    "/" + detector.getIndices().get(0) + "/_doc/" + randomAlphaOfLength(5) + "?refresh=true",
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity("{\"name\": \"test\"}"),
                    null,
                    false
                );
        } else {
            detector = TestHelpers
                .randomAnomalyDetector(
                    ImmutableList.of(indexName),
                    ImmutableList.of(TestHelpers.randomFeature(featureEnabled)),
                    uiMetadata,
                    Instant.now(),
                    OpenSearchRestTestCase.randomLongBetween(1, 1000),
                    true,
                    null
                );
        }

        AnomalyDetector createdDetector = createAnomalyDetector(detector, refresh, client);

        if (withMetadata) {
            return getAnomalyDetector(createdDetector.getDetectorId(), new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"), client);
        }
        return getAnomalyDetector(createdDetector.getDetectorId(), new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"), client);
    }

    protected AnomalyDetector createAnomalyDetector(AnomalyDetector detector, Boolean refresh, RestClient client) throws IOException {
        Response response = TestHelpers
            .makeRequest(client, "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), TestHelpers.toHttpEntity(detector), null);
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> detectorJson = jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent())
            .map();
        String detectorId = (String) detectorJson.get("_id");
        AnomalyDetector detectorInIndex = null;
        int i = 0;
        do {
            i++;
            try {
                detectorInIndex = getAnomalyDetector(detectorId, client);
                assertNotNull(detectorInIndex);
                break;
            } catch (Exception e) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    logger.error("Failed to sleep after creating detector", ex);
                }
            }
        } while (i < MAX_RETRY_TIMES);
        assertNotNull("Can't get anomaly detector from index", detectorInIndex);
        // Adding additional sleep time in order to have more time between AD Creation and whichever
        // step comes next in terms of accessing/update/deleting the detector, this will help avoid
        // lots of flaky tests
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            logger.error("Failed to sleep after creating detector", ex);
        }
        return detectorInIndex;
    }

    protected Response startAnomalyDetector(String detectorId, DetectionDateRange dateRange, RestClient client) throws IOException {
        return TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/_start",
                ImmutableMap.of(),
                dateRange == null ? null : TestHelpers.toHttpEntity(dateRange),
                null
            );
    }

    protected Response stopAnomalyDetector(String detectorId, RestClient client, boolean realtime) throws IOException {
        String jobType = realtime ? "" : "?historical";
        return TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/_stop" + jobType,
                ImmutableMap.of(),
                "",
                null
            );
    }

    protected Response deleteAnomalyDetector(String detectorId, RestClient client) throws IOException {
        return TestHelpers.makeRequest(client, "DELETE", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId, ImmutableMap.of(), "", null);
    }

    protected Response previewAnomalyDetector(String detectorId, RestClient client, AnomalyDetectorExecutionInput input)
        throws IOException {
        return TestHelpers
            .makeRequest(
                client,
                "POST",
                String.format(Locale.ROOT, TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(input),
                null
            );
    }

    public AnomalyDetector getAnomalyDetector(String detectorId, RestClient client) throws IOException {
        return (AnomalyDetector) getAnomalyDetector(detectorId, false, client)[0];
    }

    public Response updateAnomalyDetector(String detectorId, AnomalyDetector newDetector, RestClient client) throws IOException {
        BasicHeader header = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        return TestHelpers
            .makeRequest(
                client,
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId,
                null,
                TestHelpers.toJsonString(newDetector),
                ImmutableList.of(header)
            );
    }

    public AnomalyDetector getAnomalyDetector(String detectorId, BasicHeader header, RestClient client) throws IOException {
        return (AnomalyDetector) getAnomalyDetector(detectorId, header, false, false, client)[0];
    }

    public ToXContentObject[] getAnomalyDetector(String detectorId, boolean returnJob, RestClient client) throws IOException {
        BasicHeader header = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        return getAnomalyDetector(detectorId, header, returnJob, false, client);
    }

    public ToXContentObject[] getAnomalyDetector(
        String detectorId,
        BasicHeader header,
        boolean returnJob,
        boolean returnTask,
        RestClient client
    ) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client,
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "?job=" + returnJob + "&task=" + returnTask,
                null,
                "",
                ImmutableList.of(header)
            );
        assertEquals("Unable to get anomaly detector " + detectorId, RestStatus.OK, TestHelpers.restStatus(response));
        XContentParser parser = createAdParser(XContentType.JSON.xContent(), response.getEntity().getContent());
        parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        String id = null;
        Long version = null;
        AnomalyDetector detector = null;
        // AnomalyDetectorJob detectorJob = null;
        ADTask realtimeAdTask = null;
        ADTask historicalAdTask = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case "_id":
                    id = parser.text();
                    break;
                case "_version":
                    version = parser.longValue();
                    break;
                case "anomaly_detector":
                    detector = AnomalyDetector.parse(parser);
                    break;
                /*case "anomaly_detector_job":
                    detectorJob = AnomalyDetectorJob.parse(parser);
                    break;*/
                case "realtime_detection_task":
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        realtimeAdTask = ADTask.parse(parser);
                    }
                    break;
                case "historical_analysis_task":
                    if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                        historicalAdTask = ADTask.parse(parser);
                    }
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new ToXContentObject[] {
            new AnomalyDetector(
                id,
                version,
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
                null,
                detector.getUser(),
                detector.getResultIndex()
            ),
            // detectorJob,
            historicalAdTask,
            realtimeAdTask };
    }

    protected final XContentParser createAdParser(XContent xContent, InputStream data) throws IOException {
        return xContent.createParser(TestHelpers.xContentRegistry(), LoggingDeprecationHandler.INSTANCE, data);
    }

    public void updateClusterSettings(String settingKey, Object value) throws Exception {
        XContentBuilder builder = XContentFactory
            .jsonBuilder()
            .startObject()
            .startObject("persistent")
            .field(settingKey, value)
            .endObject()
            .endObject();
        Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        Thread.sleep(2000); // sleep some time to resolve flaky test
    }

    public Response getDetectorProfile(String detectorId, boolean all, String customizedProfile, RestClient client) throws IOException {
        return TestHelpers
            .makeRequest(
                client,
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/" + RestHandlerUtils.PROFILE + customizedProfile + "?_all=" + all,
                null,
                "",
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response getDetectorProfile(String detectorId) throws IOException {
        return getDetectorProfile(detectorId, false, "", client());
    }

    public Response getDetectorProfile(String detectorId, boolean all) throws IOException {
        return getDetectorProfile(detectorId, all, "", client());
    }

    public Response getSearchDetectorCount() throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + RestHandlerUtils.COUNT,
                null,
                "",
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response getSearchDetectorMatch(String name) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "GET",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + RestHandlerUtils.MATCH,
                ImmutableMap.of("name", name),
                "",
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response searchTopAnomalyResults(String detectorId, boolean historical, String bodyAsJsonString, RestClient client)
        throws IOException {
        return TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI
                    + "/"
                    + detectorId
                    + "/"
                    + RestHandlerUtils.RESULTS
                    + "/"
                    + RestHandlerUtils.TOP_ANOMALIES,
                Collections.singletonMap("historical", String.valueOf(historical)),
                TestHelpers.toHttpEntity(bodyAsJsonString),
                new ArrayList<>()
            );
    }

    public Response createUser(String name, String password, ArrayList<String> backendRoles) throws IOException {
        JsonArray backendRolesString = new JsonArray();
        for (int i = 0; i < backendRoles.size(); i++) {
            backendRolesString.add(backendRoles.get(i));
        }
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_opendistro/_security/api/internalusers/" + name,
                null,
                TestHelpers
                    .toHttpEntity(
                        " {\n"
                            + "\"password\": \""
                            + password
                            + "\",\n"
                            + "\"backend_roles\": "
                            + backendRolesString
                            + ",\n"
                            + "\"attributes\": {\n"
                            + "}} "
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response createRoleMapping(String role, ArrayList<String> users) throws IOException {
        JsonArray usersString = new JsonArray();
        for (int i = 0; i < users.size(); i++) {
            usersString.add(users.get(i));
        }
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_opendistro/_security/api/rolesmapping/" + role,
                null,
                TestHelpers
                    .toHttpEntity(
                        "{\n" + "  \"backend_roles\" : [  ],\n" + "  \"hosts\" : [  ],\n" + "  \"users\" : " + usersString + "\n" + "}"
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response createIndexRole(String role, String index) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_opendistro/_security/api/roles/" + role,
                null,
                TestHelpers
                    .toHttpEntity(
                        "{\n"
                            + "\"cluster_permissions\": [\n"
                            + "],\n"
                            + "\"index_permissions\": [\n"
                            + "{\n"
                            + "\"index_patterns\": [\n"
                            + "\""
                            + index
                            + "\"\n"
                            + "],\n"
                            + "\"dls\": \"\",\n"
                            + "\"fls\": [],\n"
                            + "\"masked_fields\": [],\n"
                            + "\"allowed_actions\": [\n"
                            + "\"crud\",\n"
                            + "\"indices:admin/create\"\n"
                            + "]\n"
                            + "}\n"
                            + "],\n"
                            + "\"tenant_permissions\": []\n"
                            + "}"
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response createSearchRole(String role, String index) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_opendistro/_security/api/roles/" + role,
                null,
                TestHelpers
                    .toHttpEntity(
                        "{\n"
                            + "\"cluster_permissions\": [\n"
                            + "],\n"
                            + "\"index_permissions\": [\n"
                            + "{\n"
                            + "\"index_patterns\": [\n"
                            + "\""
                            + index
                            + "\"\n"
                            + "],\n"
                            + "\"dls\": \"\",\n"
                            + "\"fls\": [],\n"
                            + "\"masked_fields\": [],\n"
                            + "\"allowed_actions\": [\n"
                            + "\"indices:data/read/search\"\n"
                            + "]\n"
                            + "}\n"
                            + "],\n"
                            + "\"tenant_permissions\": []\n"
                            + "}"
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response deleteUser(String user) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                "/_opendistro/_security/api/internalusers/" + user,
                null,
                "",
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response deleteRoleMapping(String user) throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "DELETE",
                "/_opendistro/_security/api/rolesmapping/" + user,
                null,
                "",
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response enableFilterBy() throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "_cluster/settings",
                null,
                TestHelpers
                    .toHttpEntity(
                        "{\n"
                            + "  \"persistent\": {\n"
                            + "       \"opendistro.anomaly_detection.filter_by_backend_roles\" : \"true\"\n"
                            + "   }\n"
                            + "}"
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response disableFilterBy() throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "_cluster/settings",
                null,
                TestHelpers
                    .toHttpEntity(
                        "{\n"
                            + "  \"persistent\": {\n"
                            + "       \"opendistro.anomaly_detection.filter_by_backend_roles\" : \"false\"\n"
                            + "   }\n"
                            + "}"
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    protected AnomalyDetector cloneDetector(AnomalyDetector anomalyDetector, String resultIndex) {
        AnomalyDetector detector = new AnomalyDetector(
            null,
            null,
            randomAlphaOfLength(5),
            randomAlphaOfLength(10),
            anomalyDetector.getTimeField(),
            anomalyDetector.getIndices(),
            anomalyDetector.getFeatureAttributes(),
            anomalyDetector.getFilterQuery(),
            anomalyDetector.getDetectionInterval(),
            anomalyDetector.getWindowDelay(),
            anomalyDetector.getShingleSize(),
            anomalyDetector.getUiMetadata(),
            anomalyDetector.getSchemaVersion(),
            Instant.now(),
            anomalyDetector.getCategoryField(),
            null,
            resultIndex
        );
        return detector;
    }

    protected Response validateAnomalyDetector(AnomalyDetector detector, RestClient client) throws IOException {
        return TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
    }

}
