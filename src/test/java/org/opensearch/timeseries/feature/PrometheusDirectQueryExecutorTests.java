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

import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.spec.SecretKeySpec;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.PrometheusSource;
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

        executor
            .executeRangeQuery(
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

        NavigableMap<Long, Double> filtered = executor.parsePrometheusResponse(responseBody, Map.of("instance", "prometheus-b:9090"));

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

        Map<Map<String, String>, NavigableMap<Long, Double>> valuesBySeries = executor.parsePrometheusResponseBySeries(responseBody, null);

        assertEquals(2, valuesBySeries.size());
        assertTrue(valuesBySeries.keySet().stream().anyMatch(labels -> "prometheus-a:9090".equals(labels.get("instance"))));
        assertTrue(valuesBySeries.keySet().stream().anyMatch(labels -> "prometheus-b:9090".equals(labels.get("instance"))));
    }

    public void testParsePrometheusResponseHandlesEmptyErrorSingleValueAndInvalidPoints() throws Exception {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );

        assertTrue(executor.parsePrometheusResponseBySeries("", null).isEmpty());
        assertTrue(executor.parsePrometheusResponseBySeries("{\"status\":\"success\",\"data\":{\"result\":[]}}", null).isEmpty());

        IllegalStateException error = expectThrows(
            IllegalStateException.class,
            () -> executor
                .parsePrometheusResponseBySeries("{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"bad query\"}", null)
        );
        assertTrue(error.getMessage().contains("bad_data"));

        String responseBody = "{"
            + "\"status\":\"success\","
            + "\"data\":{\"result\":["
            + "{\"metric\":[],\"value\":[1710000000.5,\"13\"]},"
            + "{\"metric\":{\"instance\":\"bad\"},\"values\":[null,[null,\"1\"],[1710000000,\"NaN\"],[1710000060,\"+Inf\"],[1710000120,\"-Inf\"]]}"
            + "]}"
            + "}";

        Map<Map<String, String>, NavigableMap<Long, Double>> valuesBySeries = executor.parsePrometheusResponseBySeries(responseBody, null);
        assertEquals(1, valuesBySeries.size());
        NavigableMap<Long, Double> values = valuesBySeries.values().iterator().next();
        assertEquals(1710000000500L, values.firstKey().longValue());
        assertEquals(13.0d, values.firstEntry().getValue(), 0.001d);
    }

    public void testExecuteRangeQueryResolvesDatasourceUsesHttpClientAndCachesDatasource() throws Exception {
        Client client = mock(Client.class);
        mockThreadContext(client);
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSourceAsMap()).thenReturn(prometheusDatasource("http://prometheus.example.org:9090", "noauth"));
        mockDatasourceLookup(client, getResponse);

        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body())
            .thenReturn(
                "{"
                    + "\"status\":\"success\","
                    + "\"data\":{\"result\":[{\"metric\":{\"instance\":\"a\"},\"values\":[[1710000000,\"10\"]]}]}"
                    + "}"
            );
        when(httpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(CompletableFuture.completedFuture(httpResponse));

        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            client,
            mock(SecurityClientUtil.class),
            "master-key",
            httpClient
        );
        org.opensearch.ad.model.AnomalyDetector detector = org.opensearch.ad.model.AnomalyDetector
            .parse(TestHelpers.parser(prometheusDetectorString("prome")));

        NavigableMap<Long, Double> firstResult = executeRangeQuery(executor, detector);
        NavigableMap<Long, Double> secondResult = executeRangeQuery(executor, detector);

        assertEquals(10.0d, firstResult.firstEntry().getValue(), 0.001d);
        assertEquals(10.0d, secondResult.firstEntry().getValue(), 0.001d);
        verify(client, times(1)).get(any(GetRequest.class), any(ActionListener.class));
    }

    public void testExecuteRangeQueryConvertsHttpFailureToListenerFailure() throws Exception {
        Client client = mock(Client.class);
        mockThreadContext(client);
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSourceAsMap()).thenReturn(prometheusDatasource("http://prometheus.example.org:9090", "noauth"));
        mockDatasourceLookup(client, getResponse);

        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        when(httpResponse.statusCode()).thenReturn(500);
        when(httpResponse.body()).thenReturn("x".repeat(600));
        when(httpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(CompletableFuture.completedFuture(httpResponse));

        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            client,
            mock(SecurityClientUtil.class),
            "master-key",
            httpClient
        );
        org.opensearch.ad.model.AnomalyDetector detector = org.opensearch.ad.model.AnomalyDetector
            .parse(TestHelpers.parser(prometheusDetectorString("prome")));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();

        executor
            .executeRangeQuery(
                detector,
                1710000000000L,
                1710000060000L,
                60L,
                AnalysisType.AD,
                ActionListener.wrap(result -> fail("Expected HTTP failure"), e -> {
                    failure.set(e);
                    latch.countDown();
                })
            );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(failure.get().getMessage().contains("HTTP 500"));
        assertTrue(failure.get().getMessage().contains("..."));
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

        PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource resolved = executor
            .resolvePrometheusDataSourceProperties(properties, "prome-auth");

        assertEquals(PrometheusDirectQueryExecutor.PrometheusAuthType.BASICAUTH, resolved.getAuthType());
        assertEquals("http://prometheus.example.org:9090", resolved.getPrometheusBaseUri());
        assertEquals("demo-user", resolved.getUsername());
        assertEquals("demo-pass", resolved.getPassword());
    }

    public void testResolvePrometheusDataSourcePropertiesSupportsNoAuthAndNormalizesUri() {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("prometheus.uri", "prometheus.example.org:9090/");

        PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource resolved = executor
            .resolvePrometheusDataSourceProperties(properties, "prome-noauth");

        assertEquals(PrometheusDirectQueryExecutor.PrometheusAuthType.NOAUTH, resolved.getAuthType());
        assertEquals("http://prometheus.example.org:9090", resolved.getPrometheusBaseUri());
    }

    public void testResolvePrometheusDataSourcePropertiesSupportsAwsSigV4Alias() {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("prometheus.uri", "https://aps-workspaces.us-west-2.amazonaws.com");
        properties.put("prometheus.auth.type", "awssigv4auth");
        properties.put("prometheus.auth.region", "us-west-2");
        properties.put("prometheus.auth.access_key", "AKIDEXAMPLE");
        properties.put("prometheus.auth.secret_key", "secret");

        PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource resolved = executor
            .resolvePrometheusDataSourceProperties(properties, "prome-sigv4");

        assertEquals(PrometheusDirectQueryExecutor.PrometheusAuthType.AWSSIGV4, resolved.getAuthType());
        assertEquals("us-west-2", resolved.getRegion());
        assertEquals("AKIDEXAMPLE", resolved.getAccessKey());
        assertEquals("secret", resolved.getSecretKey());
    }

    public void testResolvePrometheusDataSourcePropertiesRejectsBadUriAndUnsupportedAuth() {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );
        Map<String, Object> badUriProperties = new LinkedHashMap<>();
        badUriProperties.put("prometheus.uri", "http://");
        IllegalArgumentException badUri = expectThrows(
            IllegalArgumentException.class,
            () -> executor.resolvePrometheusDataSourceProperties(badUriProperties, "bad-uri")
        );
        assertTrue(badUri.getMessage().contains("invalid Prometheus URI"));

        Map<String, Object> unsupportedAuthProperties = new LinkedHashMap<>();
        unsupportedAuthProperties.put("prometheus.uri", "http://prometheus.example.org:9090");
        unsupportedAuthProperties.put("prometheus.auth.type", "bearer");
        IllegalArgumentException unsupportedAuth = expectThrows(
            IllegalArgumentException.class,
            () -> executor.resolvePrometheusDataSourceProperties(unsupportedAuthProperties, "bad-auth")
        );
        assertTrue(unsupportedAuth.getMessage().contains("unsupported Prometheus auth type"));
    }

    public void testResolvePrometheusDataSourcePropertiesDecryptsEncryptedBasicAuth() {
        assumeTrue("AWS Encryption SDK is not available on the test classpath", isAwsEncryptionSdkAvailable());

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

        PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource resolved = executor
            .resolvePrometheusDataSourceProperties(properties, "prome-auth");

        assertEquals(PrometheusDirectQueryExecutor.PrometheusAuthType.BASICAUTH, resolved.getAuthType());
        assertEquals("demo-user", resolved.getUsername());
        assertEquals("demo-pass", resolved.getPassword());
    }

    public void testResolvePrometheusDataSourcePropertiesKeepsEncryptedCredentialWhenEncryptionSdkUnavailable() {
        assumeTrue("AWS Encryption SDK is available on the test classpath", !isAwsEncryptionSdkAvailable());

        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            TEST_MASTER_KEY,
            mock(HttpClient.class)
        );
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("prometheus.uri", "http://prometheus.example.org:9090");
        properties.put("prometheus.auth.type", "basicauth");
        properties.put("prometheus.auth.username", "encrypted-user");
        properties.put("prometheus.auth.password", "encrypted-pass");

        PrometheusDirectQueryExecutor.ResolvedPrometheusDataSource resolved = executor
            .resolvePrometheusDataSourceProperties(properties, "prome-auth");

        assertEquals(PrometheusDirectQueryExecutor.PrometheusAuthType.BASICAUTH, resolved.getAuthType());
        assertEquals("encrypted-user", resolved.getUsername());
        assertEquals("encrypted-pass", resolved.getPassword());
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

        HttpRequest request = executor
            .buildRangeQueryRequest(resolved, "up", 1700000000000L, 1700000060000L, 60L, Instant.parse("2026-04-09T12:34:56Z"));

        assertEquals("Basic ZGVtby11c2VyOmRlbW8tcGFzcw==", request.headers().firstValue("Authorization").orElse(null));
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

        HttpRequest request = executor
            .buildRangeQueryRequest(
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

    public void testExecuteRangeQueryValidationFailures() throws Exception {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            mock(HttpClient.class)
        );
        Config missingQueryDetector = mock(Config.class);
        when(missingQueryDetector.getPrometheusSource()).thenReturn(new PrometheusSource("PROMQL", "", "prome"));

        Exception missingQuery = executeRangeQueryExpectingFailure(executor, missingQueryDetector, 1_000L, 2_000L, 60L);
        assertTrue(missingQuery.getMessage().contains("prometheus_source.query must be set"));

        org.opensearch.ad.model.AnomalyDetector validDetector = org.opensearch.ad.model.AnomalyDetector
            .parse(TestHelpers.parser(prometheusDetectorString("prome")));
        Exception invalidRange = executeRangeQueryExpectingFailure(executor, validDetector, 2_000L, 1_000L, 60L);
        assertTrue(invalidRange.getMessage().contains("endTimeMs must be larger"));
    }

    public void testOwnedHttpClientIsCreatedLazilyAndClosed() throws Exception {
        PrometheusDirectQueryExecutor executor = new PrometheusDirectQueryExecutor(
            mock(Client.class),
            mock(SecurityClientUtil.class),
            Settings.EMPTY
        );
        Method getHttpClient = PrometheusDirectQueryExecutor.class.getDeclaredMethod("getHttpClient");
        getHttpClient.setAccessible(true);

        assertNotNull(getHttpClient.invoke(executor));
        executor.close();
    }

    @SuppressWarnings("unchecked")
    private String encryptCredential(String plainText) {
        try {
            Class<?> commitmentPolicyClass = Class.forName("com.amazonaws.encryptionsdk.CommitmentPolicy");
            Object commitmentPolicy = Enum
                .valueOf((Class<? extends Enum>) commitmentPolicyClass.asSubclass(Enum.class), "RequireEncryptRequireDecrypt");

            Class<?> awsCryptoClass = Class.forName("com.amazonaws.encryptionsdk.AwsCrypto");
            Object builder = awsCryptoClass.getMethod("builder").invoke(null);
            builder.getClass().getMethod("withCommitmentPolicy", commitmentPolicyClass).invoke(builder, commitmentPolicy);
            Object crypto = builder.getClass().getMethod("build").invoke(builder);

            Class<?> jceMasterKeyClass = Class.forName("com.amazonaws.encryptionsdk.jce.JceMasterKey");
            Object jceMasterKey = jceMasterKeyClass
                .getMethod("getInstance", java.security.Key.class, String.class, String.class, String.class)
                .invoke(
                    null,
                    new SecretKeySpec(TEST_MASTER_KEY.getBytes(StandardCharsets.UTF_8), "AES"),
                    "Custom",
                    "opensearch.config.master.key",
                    "AES/GCM/NoPadding"
                );

            Method encryptDataMethod = null;
            for (Method method : crypto.getClass().getMethods()) {
                if ("encryptData".equals(method.getName()) && method.getParameterCount() == 2) {
                    encryptDataMethod = method;
                    break;
                }
            }
            if (encryptDataMethod == null) {
                throw new NoSuchMethodException("encryptData");
            }

            Object encryptResult = encryptDataMethod.invoke(crypto, jceMasterKey, plainText.getBytes(StandardCharsets.UTF_8));
            byte[] encryptedBytes = (byte[]) encryptResult.getClass().getMethod("getResult").invoke(encryptResult);
            return java.util.Base64.getEncoder().encodeToString(encryptedBytes);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to encrypt Prometheus datasource credential", e);
        }
    }

    private boolean isAwsEncryptionSdkAvailable() {
        try {
            Class.forName("com.amazonaws.encryptionsdk.CommitmentPolicy");
            Class.forName("com.amazonaws.encryptionsdk.AwsCrypto");
            Class.forName("com.amazonaws.encryptionsdk.jce.JceMasterKey");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private NavigableMap<Long, Double> executeRangeQuery(
        PrometheusDirectQueryExecutor executor,
        org.opensearch.ad.model.AnomalyDetector detector
    ) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<NavigableMap<Long, Double>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        executor.executeRangeQuery(detector, 1710000000000L, 1710000060000L, 60L, AnalysisType.AD, ActionListener.wrap(value -> {
            result.set(value);
            latch.countDown();
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        return result.get();
    }

    private Exception executeRangeQueryExpectingFailure(
        PrometheusDirectQueryExecutor executor,
        Config detector,
        long startTimeMs,
        long endTimeMs,
        long stepSeconds
    ) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        executor
            .executeRangeQuery(
                detector,
                startTimeMs,
                endTimeMs,
                stepSeconds,
                AnalysisType.AD,
                ActionListener.wrap(value -> fail("Expected validation failure"), e -> {
                    failure.set(e);
                    latch.countDown();
                })
            );
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        return failure.get();
    }

    private void mockThreadContext(Client client) {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    @SuppressWarnings("unchecked")
    private void mockDatasourceLookup(Client client, GetResponse getResponse) {
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));
    }

    private Map<String, Object> prometheusDatasource(String uri, String authType) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("prometheus.uri", uri);
        properties.put("prometheus.auth.type", authType);

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("connector", "PROMETHEUS");
        source.put("properties", properties);
        return source;
    }

    private String prometheusDetectorString(String dataConnectionId) {
        return prometheusDetectorStringWithQuery(dataConnectionId, "up");
    }

    private String prometheusDetectorStringWithQuery(String dataConnectionId, String query) {
        return "{"
            + "\"name\":\"prom-detector\","
            + "\"source_type\":\"PROMETHEUS\","
            + "\"prometheus_source\":{"
            + "\"query_language\":\"PROMQL\","
            + "\"query\":\""
            + query
            + "\","
            + "\"data_connection_id\":\""
            + dataConnectionId
            + "\""
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
    }
}
