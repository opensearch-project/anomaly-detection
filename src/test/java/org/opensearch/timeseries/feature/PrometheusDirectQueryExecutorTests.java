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

package org.opensearch.timeseries.feature;

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.jce.JceMasterKey;
import static org.mockito.Mockito.mock;

import java.net.http.HttpRequest;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.spec.SecretKeySpec;

import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

public class PrometheusDirectQueryExecutorTests extends AbstractTimeSeriesTest {
    private static final String TEST_MASTER_KEY = "12345678901234567890123456789012";


    public void testExecuteRangeQueryInvalidStepFailsFast() throws Exception {
        String detectorString = "{"
            + "\"name\":\"prom-detector\","
            + "\"source_type\":\"PROMETHEUS\","
            + "\"prometheus_source\":{"
            + "\"query_language\":\"PROMQL\","
            + "\"query\":\"rate(go_gc_heap_allocs_bytes_total{instance=\\\"localhost:9090\\\"}[5m])\","
            + "\"data_connection_id\":\"prome\""
            + "},"
            + "\"feature_attributes\":[{"
            + "\"feature_id\":\"f1\","
            + "\"feature_name\":\"prom_value\","
            + "\"feature_enabled\":true,"
            + "\"aggregation_query\":{\"f1\":{\"avg\":{\"field\":\"value\"}}}"
            + "}],"
            + "\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
            + "\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
            + "\"last_update_time\":1700000000000"
            + "}";

        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();

        executor.executeRangeQuery(
            org.opensearch.ad.model.AnomalyDetector.parse(TestHelpers.parser(detectorString)),
            1700000000000L,
            1700000060000L,
            0L,
            AnalysisType.AD,
            ActionListener.wrap(result -> {
                fail("Expected validation exception for non-positive stepSeconds");
            }, e -> {
                failure.set(e);
                latch.countDown();
            })
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(failure.get());
        assertTrue(failure.get() instanceof IllegalArgumentException);
        assertTrue(failure.get().getMessage().contains("stepSeconds must be positive"));
    }

    public void testParsePrometheusResponseUsesSeriesFilterWhenProvided() throws Exception {
        String responseBody = "{"
            + "\"status\":\"success\","
            + "\"data\":{"
            + "\"resultType\":\"matrix\","
            + "\"result\":["
            + "{"
            + "\"metric\":{\"instance\":\"prometheus-a:9090\",\"job\":\"leaf-prometheus\"},"
            + "\"values\":[[1710000000,\"10\"],[1710000060,\"12\"]]"
            + "},"
            + "{"
            + "\"metric\":{\"instance\":\"prometheus-b:9090\",\"job\":\"leaf-prometheus\"},"
            + "\"values\":[[1710000000,\"20\"],[1710000060,\"22\"]]"
            + "}"
            + "]"
            + "}"
            + "}";

        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );

        NavigableMap<Long, Double> filtered = executor.parsePrometheusResponse(
            responseBody,
            Map.of("instance", "prometheus-b:9090")
        );

        assertEquals(2, filtered.size());
        assertEquals(20.0d, filtered.firstEntry().getValue(), 0.001d);
        assertEquals(22.0d, filtered.lastEntry().getValue(), 0.001d);
    }

    public void testParsePrometheusResponseAveragesSeriesWithoutFilter() throws Exception {
        String responseBody = "{"
            + "\"status\":\"success\","
            + "\"data\":{"
            + "\"resultType\":\"matrix\","
            + "\"result\":["
            + "{"
            + "\"metric\":{\"instance\":\"prometheus-a:9090\",\"job\":\"leaf-prometheus\"},"
            + "\"values\":[[1710000000,\"10\"],[1710000060,\"12\"]]"
            + "},"
            + "{"
            + "\"metric\":{\"instance\":\"prometheus-b:9090\",\"job\":\"leaf-prometheus\"},"
            + "\"values\":[[1710000000,\"20\"],[1710000060,\"22\"]]"
            + "}"
            + "]"
            + "}"
            + "}";

        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );

        NavigableMap<Long, Double> averaged = executor.parsePrometheusResponse(responseBody, null);

        assertEquals(2, averaged.size());
        assertEquals(15.0d, averaged.firstEntry().getValue(), 0.001d);
        assertEquals(17.0d, averaged.lastEntry().getValue(), 0.001d);
    }

    public void testParsePrometheusResponseBySeriesKeepsLabelSetsSeparate() throws Exception {
        String responseBody = "{"
            + "\"status\":\"success\","
            + "\"data\":{"
            + "\"resultType\":\"matrix\","
            + "\"result\":["
            + "{"
            + "\"metric\":{\"instance\":\"prometheus-a:9090\",\"job\":\"leaf-prometheus\"},"
            + "\"values\":[[1710000000,\"10\"],[1710000060,\"12\"]]"
            + "},"
            + "{"
            + "\"metric\":{\"instance\":\"prometheus-b:9090\",\"job\":\"leaf-prometheus\"},"
            + "\"values\":[[1710000000,\"20\"],[1710000060,\"22\"]]"
            + "}"
            + "]"
            + "}"
            + "}";

        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );

        Map<Map<String, String>, NavigableMap<Long, Double>> valuesBySeries = executor.parsePrometheusResponseBySeries(
            responseBody,
            null
        );

        assertEquals(2, valuesBySeries.size());
        assertTrue(valuesBySeries.keySet().stream().anyMatch(labels -> "prometheus-a:9090".equals(labels.get("instance"))));
        assertTrue(valuesBySeries.keySet().stream().anyMatch(labels -> "prometheus-b:9090".equals(labels.get("instance"))));
    }

    public void testResolvePrometheusDataSourcePropertiesSupportsBasicAuth() {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("prometheus.uri", "http://prometheus.example.org:9090");
        properties.put("prometheus.auth.type", "basicauth");
        properties.put("prometheus.auth.username", "demo-user");
        properties.put("prometheus.auth.password", "demo-pass");

        PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource resolved = executor.resolvePrometheusDataSourceProperties(
            properties,
            "prome-auth"
        );

        assertEquals(PrometheusDirectQueryExecutor.PrometheusAuthType.BASICAUTH, resolved.getAuthType());
        assertEquals("http://prometheus.example.org:9090", resolved.getPrometheusBaseUri());
        assertEquals("demo-user", resolved.getUsername());
        assertEquals("demo-pass", resolved.getPassword());
    }

    public void testResolvePrometheusDataSourcePropertiesDecryptsEncryptedBasicAuth() {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            TEST_MASTER_KEY,
            mock(HttpClient.class)
        );
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("prometheus.uri", "http://prometheus.example.org:9090");
        properties.put("prometheus.auth.type", "basicauth");
        properties.put("prometheus.auth.username", encryptCredential("demo-user"));
        properties.put("prometheus.auth.password", encryptCredential("demo-pass"));

        PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource resolved = executor.resolvePrometheusDataSourceProperties(
            properties,
            "prome-auth"
        );

        assertEquals(PrometheusDirectQueryExecutor.PrometheusAuthType.BASICAUTH, resolved.getAuthType());
        assertEquals("demo-user", resolved.getUsername());
        assertEquals("demo-pass", resolved.getPassword());
    }

    public void testBuildRangeQueryRequestAddsBasicAuthHeader() {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );
        PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource resolved = PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource
            .builder()
            .prometheusBaseUri("http://prometheus.example.org:9090")
            .authType(PrometheusDirectQueryExecutor.PrometheusAuthType.BASICAUTH)
            .username("demo-user")
            .password("demo-pass")
            .build();

        HttpRequest request = executor.buildRangeQueryRequest(
            resolved,
            "up",
            1700000000000L,
            1700000060000L,
            60L,
            Instant.parse("2026-04-09T12:34:56Z")
        );

        assertEquals(
            "Basic ZGVtby11c2VyOmRlbW8tcGFzcw==",
            request.headers().firstValue("Authorization").orElse(null)
        );
    }

    public void testBuildRangeQueryRequestAddsAwsSigV4Headers() {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );
        PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource resolved = PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource
            .builder()
            .prometheusBaseUri("https://aps-workspaces.us-west-2.amazonaws.com")
            .authType(PrometheusDirectQueryExecutor.PrometheusAuthType.AWSSIGV4)
            .region("us-west-2")
            .accessKey("AKIDEXAMPLE")
            .secretKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
            .build();

        HttpRequest request = executor.buildRangeQueryRequest(
            resolved,
            "rate(http_requests_total[5m])",
            1700000000000L,
            1700000060000L,
            60L,
            Instant.parse("2026-04-09T12:34:56Z")
        );

        assertEquals("20260409T123456Z", request.headers().firstValue("x-amz-date").orElse(null));
        assertEquals(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            request.headers().firstValue("x-amz-content-sha256").orElse(null)
        );
        assertEquals("aps-workspaces.us-west-2.amazonaws.com", request.uri().getHost());

        String authorization = request.headers().firstValue("Authorization").orElse(null);
        assertNotNull(authorization);
        assertTrue(authorization.startsWith("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260409/us-west-2/aps/aws4_request"));
        assertTrue(authorization.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assertTrue(authorization.matches(".*Signature=[0-9a-f]{64}$"));
    }

    public void testResolvePrometheusDataSourcePropertiesRejectsMissingSigV4Region() {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("prometheus.uri", "https://aps-workspaces.us-west-2.amazonaws.com");
        properties.put("prometheus.auth.type", "awssigv4");
        properties.put("prometheus.auth.access_key", "AKIDEXAMPLE");
        properties.put("prometheus.auth.secret_key", "secret");

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> executor.resolvePrometheusDataSourceProperties(properties, "prome-sigv4")
        );

        assertTrue(error.getMessage().contains("prometheus.auth.region"));
    }

    private String encryptCredential(String plainText) {
        AwsCrypto crypto = AwsCrypto.builder().withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt).build();
        JceMasterKey jceMasterKey = JceMasterKey.getInstance(
            new SecretKeySpec(TEST_MASTER_KEY.getBytes(StandardCharsets.UTF_8), "AES"),
            "Custom",
            "opensearch.config.master.key",
            "AES/GCM/NoPadding"
        );
        CryptoResult<byte[], JceMasterKey> encryptResult = crypto.encryptData(jceMasterKey, plainText.getBytes(StandardCharsets.UTF_8));
        return java.util.Base64.getEncoder().encodeToString(encryptResult.getResult());
    }
}
