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

package org.opensearch.ad;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.opensearch.common.xcontent.json.JsonXContent.jsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
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
                    toHttpEntity("{\"name\": \"test\"}"),
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
                    null,
                    OpenSearchRestTestCase.randomLongBetween(1, 1000),
                    null,
                    true
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
            .makeRequest(client, "POST", TestHelpers.AD_BASE_DETECTORS_URI, ImmutableMap.of(), toHttpEntity(detector), null);
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, restStatus(response));

        Map<String, Object> detectorJson = jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent())
            .map();
        return new AnomalyDetector(
            (String) detectorJson.get("_id"),
            ((Integer) detectorJson.get("_version")).longValue(),
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
            detector.getCategoryField(),
            detector.getUser(),
            detector.getDetectorType(),
            detector.getDetectionDateRange()
        );
    }

    protected Response startAnomalyDetector(String detectorId, RestClient client) throws IOException {
        return TestHelpers
            .makeRequest(client, "POST", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/_start", ImmutableMap.of(), "", null);
    }

    protected Response stopAnomalyDetector(String detectorId, RestClient client) throws IOException {
        return TestHelpers
            .makeRequest(client, "POST", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "/_stop", ImmutableMap.of(), "", null);
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
                String.format(TestHelpers.AD_BASE_PREVIEW_URI, input.getDetectorId()),
                ImmutableMap.of(),
                toHttpEntity(input),
                null
            );
    }

    public AnomalyDetector getAnomalyDetector(String detectorId, RestClient client) throws IOException {
        return (AnomalyDetector) getAnomalyDetector(detectorId, false, client)[0];
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
        assertEquals("Unable to get anomaly detector " + detectorId, RestStatus.OK, restStatus(response));
        XContentParser parser = createAdParser(XContentType.JSON.xContent(), response.getEntity().getContent());
        parser.nextToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        String id = null;
        Long version = null;
        AnomalyDetector detector = null;
        AnomalyDetectorJob detectorJob = null;
        ADTask adTask = null;
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
                case "anomaly_detector_job":
                    detectorJob = AnomalyDetectorJob.parse(parser);
                    break;
                case "anomaly_detection_task":
                    adTask = ADTask.parse(parser);
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
                detector.getDetectorType(),
                detector.getDetectionDateRange()
            ),
            detectorJob,
            adTask };
    }

    protected HttpEntity toHttpEntity(ToXContentObject object) throws IOException {
        return new StringEntity(toJsonString(object), APPLICATION_JSON);
    }

    protected HttpEntity toHttpEntity(String jsonString) throws IOException {
        return new StringEntity(jsonString, APPLICATION_JSON);
    }

    protected String toJsonString(ToXContentObject object) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        return TestHelpers.xContentBuilderToString(shuffleXContent(object.toXContent(builder, ToXContent.EMPTY_PARAMS)));
    }

    protected RestStatus restStatus(Response response) {
        return RestStatus.fromCode(response.getStatusLine().getStatusCode());
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
                toHttpEntity(
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
                toHttpEntity(
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
                toHttpEntity(
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
                toHttpEntity(
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
                toHttpEntity(
                    "{\n"
                        + "  \"persistent\": {\n"
                        + "       \"opendistro.anomaly_detection.filter_by_backend_roles\" : \"false\"\n"
                        + "   }\n"
                        + "}"
                ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }
}
