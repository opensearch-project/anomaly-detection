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

package org.opensearch.ad.e2e;

import static org.opensearch.ad.TestHelpers.toHttpEntity;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.opensearch.ad.ODFERestTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AbstractSyntheticDataTest extends ODFERestTestCase {

    /**
     * In real time AD, we mute a node for a detector if that node keeps returning
     * ResourceNotFoundException (5 times in a row).  This is a problem for batch mode
     * testing as we issue a large amount of requests quickly. Due to the speed, we
     * won't be able to finish cold start before the ResourceNotFoundException mutes
     * a node.  Since our test case has only one node, there is no other nodes to fall
     * back on.  Here we disable such fault tolerance by setting max retries before
     * muting to a large number and the actual wait time during muting to 0.
     *
     * @throws IOException when failing to create http request body
     */
    protected void disableResourceNotFoundFaultTolerence() throws IOException {
        XContentBuilder settingCommand = JsonXContent.contentBuilder();

        settingCommand.startObject();
        settingCommand.startObject("persistent");
        settingCommand.field(MAX_RETRY_FOR_UNRESPONSIVE_NODE.getKey(), 100_000);
        settingCommand.field(BACKOFF_MINUTES.getKey(), 0);
        settingCommand.endObject();
        settingCommand.endObject();
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.toString(settingCommand));

        adminClient().performRequest(request);
    }

    protected List<JsonObject> getData(String datasetFileName) throws Exception {
        JsonArray jsonArray = JsonParser
            .parseReader(new FileReader(new File(getClass().getResource(datasetFileName).toURI()), Charset.defaultCharset()))
            .getAsJsonArray();
        List<JsonObject> list = new ArrayList<>(jsonArray.size());
        jsonArray.iterator().forEachRemaining(i -> list.add(i.getAsJsonObject()));
        return list;
    }

    protected Map<String, Object> getDetectionResult(String detectorId, Instant begin, Instant end, RestClient client) {
        try {
            Request request = new Request(
                "POST",
                String.format(Locale.ROOT, "/_opendistro/_anomaly_detection/detectors/%s/_run", detectorId)
            );
            request
                .setJsonEntity(
                    String.format(Locale.ROOT, "{ \"period_start\": %d, \"period_end\": %d }", begin.toEpochMilli(), end.toEpochMilli())
                );
            return entityAsMap(client.performRequest(request));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void bulkIndexTrainData(
        String datasetName,
        List<JsonObject> data,
        int trainTestSplit,
        RestClient client,
        String categoryField
    ) throws Exception {
        Request request = new Request("PUT", datasetName);
        String requestBody = null;
        if (Strings.isEmpty(categoryField)) {
            requestBody = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
                + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        } else {
            requestBody = String
                .format(
                    Locale.ROOT,
                    "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
                        + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" },"
                        + "\"%s\": { \"type\": \"keyword\"} } } }",
                    categoryField
                );
        }

        request.setJsonEntity(requestBody);
        setWarningHandler(request, false);
        client.performRequest(request);
        Thread.sleep(1_000);

        StringBuilder bulkRequestBuilder = new StringBuilder();
        for (int i = 0; i < trainTestSplit; i++) {
            bulkRequestBuilder.append("{ \"index\" : { \"_index\" : \"" + datasetName + "\", \"_id\" : \"" + i + "\" } }\n");
            bulkRequestBuilder.append(data.get(i).toString()).append("\n");
        }
        TestHelpers
            .makeRequest(
                client,
                "POST",
                "_bulk?refresh=true",
                null,
                toHttpEntity(bulkRequestBuilder.toString()),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
        Thread.sleep(1_000);
        waitAllSyncheticDataIngested(trainTestSplit, datasetName, client);
    }

    protected String createDetector(
        String datasetName,
        int intervalMinutes,
        RestClient client,
        String categoryField,
        long windowDelayInMins
    ) throws Exception {
        Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/");
        String requestBody = null;
        if (Strings.isEmpty(categoryField)) {
            requestBody = String
                .format(
                    Locale.ROOT,
                    "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                        + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                        + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                        + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                        + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                        + "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},"
                        + "\"schema_version\": 0 }",
                    datasetName,
                    intervalMinutes,
                    windowDelayInMins
                );
        } else {
            requestBody = String
                .format(
                    Locale.ROOT,
                    "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                        + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                        + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                        + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                        + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                        + "\"category_field\": [\"%s\"], "
                        + "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},"
                        + "\"schema_version\": 0  }",
                    datasetName,
                    intervalMinutes,
                    categoryField,
                    windowDelayInMins
                );
        }

        request.setJsonEntity(requestBody);
        Map<String, Object> response = entityAsMap(client.performRequest(request));
        String detectorId = (String) response.get("_id");
        Thread.sleep(1_000);
        return detectorId;
    }

    protected void waitAllSyncheticDataIngested(int expectedSize, String datasetName, RestClient client) throws Exception {
        int maxWaitCycles = 3;
        do {
            Request request = new Request("POST", String.format(Locale.ROOT, "/%s/_search", datasetName));
            request
                .setJsonEntity(
                    String
                        .format(
                            Locale.ROOT,
                            "{\"query\": {"
                                + "        \"match_all\": {}"
                                + "    },"
                                + "    \"size\": 1,"
                                + "    \"sort\": ["
                                + "       {"
                                + "         \"timestamp\": {"
                                + "           \"order\": \"desc\""
                                + "         }"
                                + "       }"
                                + "   ]}"
                        )
                );
            // Make sure all of the test data has been ingested
            // Expected response:
            // "_index":"synthetic","_type":"_doc","_id":"10080","_score":null,"_source":{"timestamp":"2019-11-08T00:00:00Z","Feature1":156.30028000000001,"Feature2":100.211205,"host":"host1"},"sort":[1573171200000]}
            Response response = client.performRequest(request);
            JsonObject json = JsonParser
                .parseReader(new InputStreamReader(response.getEntity().getContent(), Charset.defaultCharset()))
                .getAsJsonObject();
            JsonArray hits = json.getAsJsonObject("hits").getAsJsonArray("hits");
            if (hits != null
                && hits.size() == 1
                && expectedSize - 1 == hits.get(0).getAsJsonObject().getAsJsonPrimitive("_id").getAsLong()) {
                break;
            } else {
                request = new Request("POST", String.format(Locale.ROOT, "/%s/_refresh", datasetName));
                client.performRequest(request);
            }
            Thread.sleep(1_000);
        } while (maxWaitCycles-- >= 0);
    }

    protected void setWarningHandler(Request request, boolean strictDeprecationMode) {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());
    }
}
