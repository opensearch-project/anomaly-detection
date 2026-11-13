/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.HttpHost;
import org.junit.After;
import org.junit.AfterClass;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.AbstractForecastSyntheticDataTest;

import com.google.gson.JsonObject;

/**
 * Base class of the cross-cluster integration tests, run by the integTestCrossCluster task against two clusters.
 *
 * The REST client of the tests ({@link #client()}) talks to the leader cluster, which runs the plugin. Before every test
 * the follower cluster is registered on the leader as remote cluster {@link #REMOTE_CLUSTER}, so a config on the leader
 * reads follower indices as {@code follower:index}. {@link #followerClient()} talks to the follower directly; the
 * indices created with it are deleted after every test.
 *
 * Test classes must be named {@code *CrossClusterIT}: integTestCrossCluster runs only those, and the single cluster
 * test tasks exclude them.
 */
public abstract class CrossClusterRestTestCase extends AbstractForecastSyntheticDataTest {
    public static final String REMOTE_CLUSTER = "follower";
    protected static final String TIME_FIELD = "timestamp";
    protected static final String VALUE_FIELD = "value";
    protected static final String CATEGORY_FIELD = "city";
    protected static final String[] CATEGORIES = { "Phoenix", "Scottsdale" };

    private static RestClient followerClient;
    private final List<String> followerIndices = new ArrayList<>();

    /**
     * Registers the follower cluster on the leader as remote cluster {@link #REMOTE_CLUSTER}, so a config on the leader
     * reads follower indices as {@code follower:index}. Call it at the start of each test, not from {@code setUp()} or a
     * {@code @Before}: the REST client is built by the framework's own {@code @Before}, which runs after {@code setUp()}.
     */
    protected void connectRemoteCluster() throws Exception {
        String seeds = System.getProperty("tests.cluster.follow.transport_hosts");
        assertNotNull("tests.cluster.follow.transport_hosts is not set; run the test with the integTestCrossCluster task", seeds);
        String seedList = List.of(seeds.split(",")).stream().map(seed -> "\"" + seed.trim() + "\"").collect(Collectors.joining(","));
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(String.format(Locale.ROOT, "{\"persistent\":{\"cluster.remote.%s.seeds\":[%s]}}", REMOTE_CLUSTER, seedList));
        adminClient().performRequest(request);
        assertBusy(() -> {
            Map<String, Object> info = entityAsMap(adminClient().performRequest(new Request("GET", "/_remote/info")));
            @SuppressWarnings("unchecked")
            Map<String, Object> remote = (Map<String, Object>) info.get(REMOTE_CLUSTER);
            assertNotNull("remote cluster " + REMOTE_CLUSTER + " is not registered: " + info, remote);
            assertEquals("remote cluster " + REMOTE_CLUSTER + " is not connected: " + remote, Boolean.TRUE, remote.get("connected"));
        }, 60, TimeUnit.SECONDS);
    }

    @After
    public void wipeFollowerIndices() throws IOException {
        for (String index : followerIndices) {
            try {
                followerClient().performRequest(new Request("DELETE", "/" + index));
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() != RestStatus.NOT_FOUND.getStatus()) {
                    throw e;
                }
            }
        }
        followerIndices.clear();
    }

    @AfterClass
    public static void closeFollowerClient() throws IOException {
        if (followerClient != null) {
            followerClient.close();
            followerClient = null;
        }
    }

    /**
     * A REST client of the follower cluster, authenticated like {@link #client()}.
     */
    protected RestClient followerClient() throws IOException {
        if (followerClient == null) {
            String hosts = System.getProperty("tests.cluster.follow.http_hosts");
            assertNotNull("tests.cluster.follow.http_hosts is not set; run the test with the integTestCrossCluster task", hosts);
            List<HttpHost> httpHosts = new ArrayList<>();
            for (String host : hosts.split(",")) {
                httpHosts.add(parseHost(host.trim()));
            }
            followerClient = buildClient(restClientSettings(), httpHosts.toArray(new HttpHost[0]));
        }
        return followerClient;
    }

    /**
     * The expression as a config on the leader names it to read it on the follower.
     */
    protected static String remote(String expression) {
        return REMOTE_CLUSTER + ":" + expression;
    }

    /**
     * Parses a {@code host:port} entry into an {@link HttpHost} at the test protocol. Splits on the last colon so an
     * unbracketed or bracketed IPv6 address (for example {@code [::1]:9200} or {@code ::1:9200}) parses correctly, and
     * drops a leading scheme if one is present.
     */
    private HttpHost parseHost(String hostPort) {
        String value = hostPort;
        int scheme = value.indexOf("://");
        if (scheme >= 0) {
            value = value.substring(scheme + 3);
        }
        int lastColon = value.lastIndexOf(':');
        if (lastColon < 0) {
            throw new IllegalArgumentException("expected host:port but got '" + hostPort + "'");
        }
        String host = value.substring(0, lastColon);
        int port = Integer.parseInt(value.substring(lastColon + 1));
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        return new HttpHost(getProtocol(), host, port);
    }

    /**
     * Creates {@code index} on the follower with one document per category per minute for the {@code minutes} minutes
     * up to {@code end}. The index is deleted after the test.
     */
    protected void createFollowerIndex(String index, int minutes, Instant end) throws IOException {
        Request create = new Request("PUT", "/" + index);
        create
            .setJsonEntity(
                String
                    .format(
                        Locale.ROOT,
                        "{\"mappings\":{\"properties\":{\"%s\":{\"type\":\"date\"},\"%s\":{\"type\":\"double\"},\"%s\":{\"type\":\"keyword\"}}}}",
                        TIME_FIELD,
                        VALUE_FIELD,
                        CATEGORY_FIELD
                    )
            );
        followerClient().performRequest(create);
        followerIndices.add(index);

        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < minutes; i++) {
            long timestamp = end.minus(i, ChronoUnit.MINUTES).toEpochMilli();
            for (int c = 0; c < CATEGORIES.length; c++) {
                bulk.append("{\"index\":{}}\n");
                bulk
                    .append(
                        String
                            .format(
                                Locale.ROOT,
                                "{\"%s\":%d,\"%s\":%d,\"%s\":\"%s\"}\n",
                                TIME_FIELD,
                                timestamp,
                                VALUE_FIELD,
                                (i % 13) + c * 5 + 1,
                                CATEGORY_FIELD,
                                CATEGORIES[c]
                            )
                    );
            }
        }
        Request request = new Request("POST", "/" + index + "/_bulk?refresh=true");
        request.setJsonEntity(bulk.toString());
        Map<String, Object> response = entityAsMap(followerClient().performRequest(request));
        assertEquals("bulk into " + index + " failed: " + response, Boolean.FALSE, response.get("errors"));
    }

    /**
     * Adds {@code alias} on the follower over {@code indices}.
     */
    protected void createFollowerAlias(String alias, String... indices) throws IOException {
        String actions = List
            .of(indices)
            .stream()
            .map(index -> String.format(Locale.ROOT, "{\"add\":{\"index\":\"%s\",\"alias\":\"%s\"}}", index, alias))
            .collect(Collectors.joining(","));
        Request request = new Request("POST", "/_aliases");
        request.setJsonEntity("{\"actions\":[" + actions + "]}");
        followerClient().performRequest(request);
    }

    /**
     * Loads the {@code rule} dataset into an index named {@link #RULE_DATASET_NAME} on the follower, the realistic data
     * a forecaster needs to train and produce a forecast, and returns the timestamp of the last training document. The
     * index is deleted after the test. Mirrors {@code loadRuleData} but targets the follower.
     */
    protected Instant loadRuleDataToFollower(int trainTestSplit) throws Exception {
        String dataFileName = String.format(Locale.ROOT, "org/opensearch/ad/e2e/data/%s.data", RULE_DATASET_NAME);
        List<JsonObject> data = readJsonArrayWithLimit(dataFileName, trainTestSplit);
        bulkIndexTrainData(RULE_DATASET_NAME, data, trainTestSplit, followerClient(), RULE_DATA_MAPPING);
        followerIndices.add(RULE_DATASET_NAME);
        String trainTimeStr = data.get(trainTestSplit - 1).get("timestamp").getAsString();
        return canBeParsedAsLong(trainTimeStr) ? Instant.ofEpochMilli(Long.parseLong(trainTimeStr)) : Instant.parse(trainTimeStr);
    }
}
