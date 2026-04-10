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
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.PrometheusSource;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Executes Prometheus range queries for Prometheus source detectors.
 *
 * Instead of depending on SQL direct-query transport classes (which are not visible across
 * plugin classloaders), this executor resolves datasource metadata from SQL datasource storage
 * and sends Prometheus query_range HTTP requests directly.
 */
public class PrometheusDirectQueryExecutor {
    static final String DATASOURCE_ENCRYPTION_MASTER_KEY = "plugins.query.datasources.encryption.masterkey";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String DATASOURCE_INDEX = ".ql-datasources";
    private static final String PROMETHEUS_CONNECTOR = "PROMETHEUS";
    private static final String PROMETHEUS_URI_PROPERTY = "prometheus.uri";
    private static final String PROMETHEUS_AUTH_TYPE_PROPERTY = "prometheus.auth.type";
    private static final String PROMETHEUS_AUTH_USERNAME_PROPERTY = "prometheus.auth.username";
    private static final String PROMETHEUS_AUTH_PASSWORD_PROPERTY = "prometheus.auth.password";
    private static final String PROMETHEUS_AUTH_REGION_PROPERTY = "prometheus.auth.region";
    private static final String PROMETHEUS_AUTH_ACCESS_KEY_PROPERTY = "prometheus.auth.access_key";
    private static final String PROMETHEUS_AUTH_SECRET_KEY_PROPERTY = "prometheus.auth.secret_key";
    private static final String RANGE_QUERY_PATH = "api/v1/query_range";
    private static final String AWS_SIGV4_ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String AWS_SIGV4_SERVICE = "aps";
    private static final String AWS4_REQUEST = "aws4_request";
    private static final String EMPTY_PAYLOAD_SHA256 = sha256Hex("");
    private static final DateTimeFormatter AWS_AMZ_DATE_FORMATTER = DateTimeFormatter
        .ofPattern("yyyyMMdd'T'HHmmss'Z'")
        .withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter AWS_DATE_STAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int ERROR_BODY_LIMIT = 512;

    private final Client client;
    private final HttpClient httpClient;
    private final ConcurrentHashMap<String, ResolvedPrometheusDataSource> dataSourceConfigCache;
    private final String dataSourceEncryptionMasterKey;

    public PrometheusDirectQueryExecutor(Client client, SecurityClientUtil clientUtil, Settings settings) {
        this(
            client,
            clientUtil,
            settings == null ? null : settings.get(DATASOURCE_ENCRYPTION_MASTER_KEY),
            HttpClient
                .newBuilder()
                .connectTimeout(Duration.ofSeconds(DEFAULT_TIMEOUT_SECONDS))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build()
        );
    }

    // Visible for tests.
    PrometheusDirectQueryExecutor(Client client, SecurityClientUtil clientUtil, HttpClient httpClient) {
        this(client, clientUtil, null, httpClient);
    }

    PrometheusDirectQueryExecutor(Client client, SecurityClientUtil clientUtil, String dataSourceEncryptionMasterKey) {
        this(
            client,
            clientUtil,
            dataSourceEncryptionMasterKey,
            HttpClient
                .newBuilder()
                .connectTimeout(Duration.ofSeconds(DEFAULT_TIMEOUT_SECONDS))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build()
        );
    }

    // Visible for tests.
    PrometheusDirectQueryExecutor(Client client, SecurityClientUtil clientUtil, String dataSourceEncryptionMasterKey, HttpClient httpClient) {
        this.client = client;
        this.httpClient = httpClient;
        this.dataSourceConfigCache = new ConcurrentHashMap<>();
        this.dataSourceEncryptionMasterKey = dataSourceEncryptionMasterKey;
    }

    /**
     * Executes a PROMQL range query and returns a single series (timestamp -> value).
     * If {@code prometheus_source.series_filter} is set, the matching result series is used.
     * Otherwise, multiple returned series are averaged per timestamp for backward compatibility.
     */
    public void executeRangeQuery(
        Config config,
        long startTimeMs,
        long endTimeMs,
        long stepSeconds,
        AnalysisType context,
        ActionListener<NavigableMap<Long, Double>> listener
    ) {
        executeRangeQuery(config, startTimeMs, endTimeMs, stepSeconds, null, context, listener);
    }

    public void executeRangeQuery(
        Config config,
        long startTimeMs,
        long endTimeMs,
        long stepSeconds,
        Map<String, String> seriesFilterOverride,
        AnalysisType context,
        ActionListener<NavigableMap<Long, Double>> listener
    ) {
        PrometheusSource source = config.getPrometheusSource();
        Map<String, String> effectiveSeriesFilter = mergeSeriesFilters(source == null ? null : source.getSeriesFilter(), seriesFilterOverride);
        executeRangeQuery(
            source,
            startTimeMs,
            endTimeMs,
            stepSeconds,
            responseBody -> parsePrometheusResponse(responseBody, effectiveSeriesFilter),
            listener
        );
    }

    public void executeRangeQueryBySeries(
        Config config,
        long startTimeMs,
        long endTimeMs,
        long stepSeconds,
        AnalysisType context,
        ActionListener<Map<Map<String, String>, NavigableMap<Long, Double>>> listener
    ) {
        PrometheusSource source = config.getPrometheusSource();
        executeRangeQuery(
            source,
            startTimeMs,
            endTimeMs,
            stepSeconds,
            responseBody -> parsePrometheusResponseBySeries(responseBody, source == null ? null : source.getSeriesFilter()),
            listener
        );
    }

    private <T> void executeRangeQuery(
        PrometheusSource source,
        long startTimeMs,
        long endTimeMs,
        long stepSeconds,
        ResponseParser<T> responseParser,
        ActionListener<T> listener
    ) {
        Exception validationException = validateRangeQuery(source, startTimeMs, endTimeMs, stepSeconds);
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }

        resolvePrometheusDataSource(source, ActionListener.wrap(prometheusDataSource -> {
            HttpRequest request;
            try {
                request = buildRangeQueryRequest(prometheusDataSource, source.getQuery(), startTimeMs, endTimeMs, stepSeconds);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }

            httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete((response, throwable) -> {
                    if (throwable != null) {
                        listener
                            .onFailure(
                                new IllegalStateException(
                                    "Failed to query Prometheus datasource [" + source.getDataConnectionId() + "].",
                                    throwable
                                )
                            );
                        return;
                    }

                    if (response.statusCode() < 200 || response.statusCode() >= 300) {
                        listener
                            .onFailure(
                                new IllegalStateException(
                                    "Prometheus query_range failed with HTTP "
                                        + response.statusCode()
                                        + ". Body: "
                                        + truncate(response.body())
                                )
                            );
                        return;
                    }

                    try {
                        listener.onResponse(responseParser.parse(response.body()));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                });
        }, listener::onFailure));
    }

    private Exception validateRangeQuery(PrometheusSource source, long startTimeMs, long endTimeMs, long stepSeconds) {
        if (source == null) {
            return new IllegalArgumentException("prometheus_source must be set when source_type is PROMETHEUS.");
        }
        if (stepSeconds <= 0) {
            return new IllegalArgumentException("stepSeconds must be positive.");
        }
        if (source.getQuery() == null || source.getQuery().trim().isEmpty()) {
            return new IllegalArgumentException("prometheus_source.query must be set when source_type is PROMETHEUS.");
        }
        if (endTimeMs <= startTimeMs) {
            return new IllegalArgumentException("endTimeMs must be larger than startTimeMs.");
        }
        return null;
    }

    private void resolvePrometheusDataSource(PrometheusSource source, ActionListener<ResolvedPrometheusDataSource> listener) {
        String dataConnectionId = source.getDataConnectionId();
        if (dataConnectionId == null || dataConnectionId.trim().isEmpty()) {
            listener.onFailure(new IllegalArgumentException("prometheus_source.data_connection_id must be set."));
            return;
        }

        ResolvedPrometheusDataSource cached = dataSourceConfigCache.get(dataConnectionId);
        if (cached != null) {
            listener.onResponse(cached);
            return;
        }

        GetRequest getRequest = new GetRequest(DATASOURCE_INDEX, dataConnectionId);
        GetResponse response;
        try (ThreadContext.StoredContext ignored = client.threadPool().getThreadContext().stashContext()) {
            response = client.get(getRequest).actionGet();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        try {
            if (response == null || !response.isExists()) {
                listener.onFailure(new IllegalArgumentException("Prometheus datasource [" + dataConnectionId + "] does not exist."));
                return;
            }

            Map<String, Object> sourceMap = response.getSourceAsMap();
            if (sourceMap == null || sourceMap.isEmpty()) {
                listener.onFailure(new IllegalStateException("Datasource [" + dataConnectionId + "] metadata is empty."));
                return;
            }

            String connector = sourceMap.get("connector") == null ? null : sourceMap.get("connector").toString();
            if (connector == null || !PROMETHEUS_CONNECTOR.equalsIgnoreCase(connector)) {
                listener.onFailure(new IllegalArgumentException("Datasource [" + dataConnectionId + "] is not a PROMETHEUS datasource."));
                return;
            }

            Object propertiesObject = sourceMap.get("properties");
            if (!(propertiesObject instanceof Map)) {
                listener.onFailure(new IllegalStateException("Datasource [" + dataConnectionId + "] properties are invalid."));
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>) propertiesObject;
            ResolvedPrometheusDataSource resolved = resolvePrometheusDataSourceProperties(properties, dataConnectionId);
            dataSourceConfigCache.put(dataConnectionId, resolved);
            listener.onResponse(resolved);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private HttpRequest buildRangeQueryRequest(
        ResolvedPrometheusDataSource prometheusDataSource,
        String query,
        long startTimeMs,
        long endTimeMs,
        long stepSeconds
    ) {
        return buildRangeQueryRequest(prometheusDataSource, query, startTimeMs, endTimeMs, stepSeconds, Instant.now());
    }

    HttpRequest buildRangeQueryRequest(
        ResolvedPrometheusDataSource prometheusDataSource,
        String query,
        long startTimeMs,
        long endTimeMs,
        long stepSeconds,
        Instant requestTime
    ) {
        RangeQueryRequestContext requestContext = buildRangeQueryRequestContext(
            prometheusDataSource.getPrometheusBaseUri(),
            query,
            startTimeMs,
            endTimeMs,
            stepSeconds
        );

        HttpRequest.Builder builder = HttpRequest
            .newBuilder(requestContext.getRequestUri())
            .GET()
            .timeout(Duration.ofSeconds(DEFAULT_TIMEOUT_SECONDS));

        switch (prometheusDataSource.getAuthType()) {
            case BASICAUTH:
                builder.header(
                    "Authorization",
                    buildBasicAuthorizationHeader(prometheusDataSource.getUsername(), prometheusDataSource.getPassword())
                );
                break;
            case AWSSIGV4:
                applyAwsSigV4Headers(builder, requestContext, prometheusDataSource, requestTime);
                break;
            case NOAUTH:
            default:
                break;
        }

        return builder.build();
    }

    private RangeQueryRequestContext buildRangeQueryRequestContext(
        String prometheusBaseUri,
        String query,
        long startTimeMs,
        long endTimeMs,
        long stepSeconds
    ) {
        String base = prometheusBaseUri;
        if (!base.endsWith("/")) {
            base += "/";
        }

        long startSeconds = Math.max(0L, startTimeMs / 1000L);
        long endSeconds = Math.max(0L, endTimeMs / 1000L);

        Map<String, String> queryParameters = new LinkedHashMap<>();
        queryParameters.put("query", query);
        queryParameters.put("start", Long.toString(startSeconds));
        queryParameters.put("end", Long.toString(endSeconds));
        queryParameters.put("step", Long.toString(Math.max(1L, stepSeconds)));

        String canonicalQueryString = buildCanonicalQueryString(queryParameters);
        String url = base + RANGE_QUERY_PATH + "?" + canonicalQueryString;
        return new RangeQueryRequestContext(URI.create(url), canonicalQueryString);
    }

    ResolvedPrometheusDataSource resolvePrometheusDataSourceProperties(Map<String, Object> properties, String dataConnectionId) {
        String prometheusUri = getStringProperty(properties, PROMETHEUS_URI_PROPERTY);
        String authType = normalizeAuthType(getStringProperty(properties, PROMETHEUS_AUTH_TYPE_PROPERTY));

        ResolvedPrometheusDataSource.Builder builder = ResolvedPrometheusDataSource
            .builder()
            .prometheusBaseUri(normalizePrometheusUri(prometheusUri, dataConnectionId));

        if (authType == null || authType.isEmpty() || PrometheusAuthType.NOAUTH.getWireName().equals(authType)) {
            return builder.authType(PrometheusAuthType.NOAUTH).build();
        }

        if (PrometheusAuthType.BASICAUTH.getWireName().equals(authType)) {
            return builder
                .authType(PrometheusAuthType.BASICAUTH)
                .username(requireCredentialProperty(properties, PROMETHEUS_AUTH_USERNAME_PROPERTY, dataConnectionId))
                .password(requireCredentialProperty(properties, PROMETHEUS_AUTH_PASSWORD_PROPERTY, dataConnectionId))
                .build();
        }

        if (PrometheusAuthType.AWSSIGV4.getWireName().equals(authType)) {
            return builder
                .authType(PrometheusAuthType.AWSSIGV4)
                .region(requireProperty(properties, PROMETHEUS_AUTH_REGION_PROPERTY, dataConnectionId))
                .accessKey(requireCredentialProperty(properties, PROMETHEUS_AUTH_ACCESS_KEY_PROPERTY, dataConnectionId))
                .secretKey(requireCredentialProperty(properties, PROMETHEUS_AUTH_SECRET_KEY_PROPERTY, dataConnectionId))
                .build();
        }

        throw new IllegalArgumentException(
            "Datasource [" + dataConnectionId + "] has unsupported Prometheus auth type: " + authType
        );
    }

    private String normalizePrometheusUri(String rawUri, String dataConnectionId) {
        if (rawUri == null || rawUri.trim().isEmpty()) {
            throw new IllegalArgumentException("Datasource [" + dataConnectionId + "] does not contain properties.prometheus.uri.");
        }

        String candidate = rawUri.trim();
        if (!candidate.startsWith("http://") && !candidate.startsWith("https://")) {
            candidate = "http://" + candidate;
        }
        if (candidate.endsWith("/")) {
            candidate = candidate.substring(0, candidate.length() - 1);
        }

        try {
            URI uri = URI.create(candidate);
            if (uri.getHost() == null || uri.getHost().isEmpty()) {
                throw new IllegalArgumentException(
                    "Datasource [" + dataConnectionId + "] has invalid Prometheus URI: " + rawUri
                );
            }
            return candidate;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Datasource [" + dataConnectionId + "] has invalid Prometheus URI: " + rawUri, e);
        }
    }

    private String getStringProperty(Map<String, Object> properties, String propertyName) {
        Object rawValue = properties.get(propertyName);
        if (rawValue == null) {
            return null;
        }
        return rawValue.toString();
    }

    private String requireProperty(Map<String, Object> properties, String propertyName, String dataConnectionId) {
        String value = getStringProperty(properties, propertyName);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Datasource [" + dataConnectionId + "] does not contain " + propertyName + ".");
        }
        return value;
    }

    private String requireCredentialProperty(Map<String, Object> properties, String propertyName, String dataConnectionId) {
        String value = requireProperty(properties, propertyName, dataConnectionId);
        return maybeDecryptCredential(value);
    }

    String maybeDecryptCredential(String credentialValue) {
        if (credentialValue == null || credentialValue.trim().isEmpty() || dataSourceEncryptionMasterKey == null || dataSourceEncryptionMasterKey.isEmpty()) {
            return credentialValue;
        }

        try {
            return decryptCredential(credentialValue);
        } catch (Exception e) {
            return credentialValue;
        }
    }

    private String decryptCredential(String encryptedText) {
        AwsCrypto crypto = AwsCrypto.builder().withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt).build();
        JceMasterKey jceMasterKey = JceMasterKey.getInstance(
            new SecretKeySpec(dataSourceEncryptionMasterKey.getBytes(StandardCharsets.UTF_8), "AES"),
            "Custom",
            "opensearch.config.master.key",
            "AES/GCM/NoPadding"
        );
        CryptoResult<byte[], JceMasterKey> decryptedResult = crypto.decryptData(jceMasterKey, Base64.getDecoder().decode(encryptedText));
        return new String(decryptedResult.getResult(), StandardCharsets.UTF_8);
    }

    private String normalizeAuthType(String rawAuthType) {
        if (rawAuthType == null) {
            return null;
        }
        String normalized = rawAuthType.trim().toLowerCase(Locale.ROOT);
        if ("awssigv4auth".equals(normalized)) {
            return PrometheusAuthType.AWSSIGV4.getWireName();
        }
        return normalized;
    }

    private String buildBasicAuthorizationHeader(String username, String password) {
        String credentials = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    private void applyAwsSigV4Headers(
        HttpRequest.Builder builder,
        RangeQueryRequestContext requestContext,
        ResolvedPrometheusDataSource prometheusDataSource,
        Instant requestTime
    ) {
        String amzDate = AWS_AMZ_DATE_FORMATTER.format(requestTime);
        String dateStamp = AWS_DATE_STAMP_FORMATTER.format(requestTime);
        String hostHeader = requestContext.getRequestUri().getHost();
        String canonicalUri = requestContext.getRequestUri().getRawPath() == null || requestContext.getRequestUri().getRawPath().isEmpty()
            ? "/"
            : requestContext.getRequestUri().getRawPath();
        String canonicalHeaders = "host:" + hostHeader + "\n" + "x-amz-content-sha256:" + EMPTY_PAYLOAD_SHA256 + "\n" + "x-amz-date:" + amzDate
            + "\n";
        String signedHeaders = "host;x-amz-content-sha256;x-amz-date";
        String canonicalRequest = "GET"
            + "\n"
            + canonicalUri
            + "\n"
            + requestContext.getCanonicalQueryString()
            + "\n"
            + canonicalHeaders
            + "\n"
            + signedHeaders
            + "\n"
            + EMPTY_PAYLOAD_SHA256;
        String credentialScope = dateStamp
            + "/"
            + prometheusDataSource.getRegion()
            + "/"
            + AWS_SIGV4_SERVICE
            + "/"
            + AWS4_REQUEST;
        String stringToSign = AWS_SIGV4_ALGORITHM
            + "\n"
            + amzDate
            + "\n"
            + credentialScope
            + "\n"
            + sha256Hex(canonicalRequest);
        String signature = hmacSha256Hex(getAwsSigV4SigningKey(prometheusDataSource.getSecretKey(), dateStamp, prometheusDataSource.getRegion()), stringToSign);
        String authorization = AWS_SIGV4_ALGORITHM
            + " Credential="
            + prometheusDataSource.getAccessKey()
            + "/"
            + credentialScope
            + ", SignedHeaders="
            + signedHeaders
            + ", Signature="
            + signature;

        builder.header("x-amz-date", amzDate);
        builder.header("x-amz-content-sha256", EMPTY_PAYLOAD_SHA256);
        builder.header("Authorization", authorization);
    }

    private String buildCanonicalQueryString(Map<String, String> queryParameters) {
        TreeSet<String> orderedKeys = new TreeSet<>(queryParameters.keySet());
        List<String> encodedParameters = new ArrayList<>(orderedKeys.size());
        for (String key : orderedKeys) {
            String value = queryParameters.get(key);
            encodedParameters.add(encodeQueryComponent(key) + "=" + encodeQueryComponent(value == null ? "" : value));
        }
        return String.join("&", encodedParameters);
    }

    private String encodeQueryComponent(String value) {
        StringBuilder builder = new StringBuilder();
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        for (byte currentByte : bytes) {
            int current = currentByte & 0xFF;
            if (
                (current >= 'A' && current <= 'Z')
                    || (current >= 'a' && current <= 'z')
                    || (current >= '0' && current <= '9')
                    || current == '-'
                    || current == '_'
                    || current == '.'
                    || current == '~'
            ) {
                builder.append((char) current);
            } else {
                builder.append('%');
                char upper = Character.toUpperCase(Character.forDigit((current >> 4) & 0xF, 16));
                char lower = Character.toUpperCase(Character.forDigit(current & 0xF, 16));
                builder.append(upper).append(lower);
            }
        }
        return builder.toString();
    }

    private byte[] getAwsSigV4SigningKey(String secretKey, String dateStamp, String region) {
        byte[] kDate = hmacSha256(("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8), dateStamp);
        byte[] kRegion = hmacSha256(kDate, region);
        byte[] kService = hmacSha256(kRegion, AWS_SIGV4_SERVICE);
        return hmacSha256(kService, AWS4_REQUEST);
    }

    private byte[] hmacSha256(byte[] key, String value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to compute HMAC-SHA256 signature.", e);
        }
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to compute SHA-256 hash.", e);
        }
    }

    private String hmacSha256Hex(byte[] key, String value) {
        return bytesToHex(hmacSha256(key, value));
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte currentByte : bytes) {
            builder.append(Character.forDigit((currentByte >> 4) & 0xF, 16));
            builder.append(Character.forDigit(currentByte & 0xF, 16));
        }
        return builder.toString();
    }

    NavigableMap<Long, Double> parsePrometheusResponse(String body, Map<String, String> seriesFilter) throws IOException {
        Map<Map<String, String>, NavigableMap<Long, Double>> valuesBySeries = parsePrometheusResponseBySeries(body, seriesFilter);
        if (seriesFilter != null && !seriesFilter.isEmpty()) {
            return valuesBySeries.values().stream().findFirst().orElseGet(TreeMap::new);
        }

        Map<Long, double[]> timestampAccumulator = new TreeMap<>();
        valuesBySeries.values().forEach(seriesValues -> {
            seriesValues.forEach((timestamp, value) -> {
                double[] sumAndCount = timestampAccumulator.computeIfAbsent(timestamp, key -> new double[] { 0d, 0d });
                sumAndCount[0] += value;
                sumAndCount[1] += 1d;
            });
        });

        NavigableMap<Long, Double> averaged = new TreeMap<>();
        for (Map.Entry<Long, double[]> entry : timestampAccumulator.entrySet()) {
            double[] sumAndCount = entry.getValue();
            if (sumAndCount[1] > 0d) {
                averaged.put(entry.getKey(), sumAndCount[0] / sumAndCount[1]);
            }
        }
        return averaged;
    }

    Map<Map<String, String>, NavigableMap<Long, Double>> parsePrometheusResponseBySeries(
        String body,
        Map<String, String> seriesFilter
    ) throws IOException {
        if (body == null || body.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        JsonNode root = OBJECT_MAPPER.readTree(body);
        String status = root.path("status").asText("");
        if (!"success".equalsIgnoreCase(status)) {
            throw new IllegalStateException(
                "Prometheus query_range failed. errorType="
                    + root.path("errorType").asText("")
                    + ", error="
                    + root.path("error").asText("")
            );
        }

        JsonNode data = root.path("data");
        JsonNode result = data.path("result");
        if (!result.isArray() || result.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Map<String, String>, NavigableMap<Long, Double>> valuesBySeries = new LinkedHashMap<>();
        for (JsonNode series : result) {
            Map<String, String> seriesLabels = parseSeriesLabels(series.path("metric"));
            if (!matchesSeriesFilter(seriesLabels, seriesFilter)) {
                continue;
            }

            NavigableMap<Long, Double> seriesValues = parseSingleSeries(series);
            if (!seriesValues.isEmpty()) {
                valuesBySeries.put(Collections.unmodifiableMap(seriesLabels), seriesValues);
            }
        }
        return valuesBySeries;
    }

    private Map<String, String> parseSeriesLabels(JsonNode metric) {
        Map<String, String> labels = new LinkedHashMap<>();
        if (!metric.isObject()) {
            return labels;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = metric.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode value = field.getValue();
            if (value != null && !value.isNull()) {
                labels.put(field.getKey(), value.asText());
            }
        }
        return labels;
    }

    private Map<String, String> mergeSeriesFilters(Map<String, String> storedFilter, Map<String, String> overrideFilter) {
        if ((storedFilter == null || storedFilter.isEmpty()) && (overrideFilter == null || overrideFilter.isEmpty())) {
            return null;
        }

        Map<String, String> merged = new LinkedHashMap<>();
        if (storedFilter != null) {
            merged.putAll(storedFilter);
        }
        if (overrideFilter != null) {
            merged.putAll(overrideFilter);
        }
        return merged;
    }

    private boolean matchesSeriesFilter(Map<String, String> labels, Map<String, String> seriesFilter) {
        if (seriesFilter == null || seriesFilter.isEmpty()) {
            return true;
        }

        for (Map.Entry<String, String> filterEntry : seriesFilter.entrySet()) {
            String actualValue = labels.get(filterEntry.getKey());
            if (actualValue == null || !actualValue.equals(filterEntry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private NavigableMap<Long, Double> parseSingleSeries(JsonNode series) {
        NavigableMap<Long, Double> valuesByTimestamp = new TreeMap<>();
        JsonNode values = series.get("values");
        if (values != null && values.isArray()) {
            for (JsonNode point : values) {
                addSingleSeriesPoint(valuesByTimestamp, point);
            }
            return valuesByTimestamp;
        }

        JsonNode value = series.get("value");
        if (value != null && value.isArray()) {
            addSingleSeriesPoint(valuesByTimestamp, value);
        }
        return valuesByTimestamp;
    }

    private void addSingleSeriesPoint(Map<Long, Double> valuesByTimestamp, JsonNode point) {
        if (point == null || !point.isArray() || point.size() < 2) {
            return;
        }

        long timestamp = parseEpochMillis(point.get(0));
        if (timestamp < 0) {
            return;
        }

        double value = parseDouble(point.get(1));
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return;
        }

        valuesByTimestamp.put(timestamp, value);
    }

    private long parseEpochMillis(JsonNode rawTimestamp) {
        if (rawTimestamp == null || rawTimestamp.isNull()) {
            return -1L;
        }

        String text = rawTimestamp.isNumber() ? rawTimestamp.numberValue().toString() : rawTimestamp.asText("");
        if (text.isEmpty()) {
            return -1L;
        }

        double numeric = Double.parseDouble(text);
        long asLong = (long) numeric;
        if (asLong > 1_000_000_000_000L) {
            return asLong;
        }
        return (long) Math.floor(numeric * 1000d);
    }

    private double parseDouble(JsonNode rawValue) {
        if (rawValue == null || rawValue.isNull()) {
            return Double.NaN;
        }

        String text = rawValue.isNumber() ? rawValue.numberValue().toString() : rawValue.asText("");
        String normalized = text == null ? "" : text.trim();

        if (normalized.isEmpty() || "nan".equalsIgnoreCase(normalized)) {
            return Double.NaN;
        }
        if ("+inf".equalsIgnoreCase(normalized) || "inf".equalsIgnoreCase(normalized) || "infinity".equalsIgnoreCase(normalized)) {
            return Double.POSITIVE_INFINITY;
        }
        if ("-inf".equalsIgnoreCase(normalized) || "-infinity".equalsIgnoreCase(normalized)) {
            return Double.NEGATIVE_INFINITY;
        }

        return Double.parseDouble(normalized);
    }

    private String truncate(String text) {
        if (text == null) {
            return "";
        }
        if (text.length() <= ERROR_BODY_LIMIT) {
            return text;
        }
        return text.substring(0, ERROR_BODY_LIMIT) + "...";
    }

    enum PrometheusAuthType {
        NOAUTH("noauth"),
        BASICAUTH("basicauth"),
        AWSSIGV4("awssigv4");

        private final String wireName;

        PrometheusAuthType(String wireName) {
            this.wireName = wireName;
        }

        String getWireName() {
            return wireName;
        }
    }

    static final class ResolvedPrometheusDataSource {
        private final String prometheusBaseUri;
        private final PrometheusAuthType authType;
        private final String username;
        private final String password;
        private final String region;
        private final String accessKey;
        private final String secretKey;

        private ResolvedPrometheusDataSource(Builder builder) {
            this.prometheusBaseUri = builder.prometheusBaseUri;
            this.authType = builder.authType;
            this.username = builder.username;
            this.password = builder.password;
            this.region = builder.region;
            this.accessKey = builder.accessKey;
            this.secretKey = builder.secretKey;
        }

        static Builder builder() {
            return new Builder();
        }

        String getPrometheusBaseUri() {
            return prometheusBaseUri;
        }

        PrometheusAuthType getAuthType() {
            return authType;
        }

        String getUsername() {
            return username;
        }

        String getPassword() {
            return password;
        }

        String getRegion() {
            return region;
        }

        String getAccessKey() {
            return accessKey;
        }

        String getSecretKey() {
            return secretKey;
        }

        static final class Builder {
            private String prometheusBaseUri;
            private PrometheusAuthType authType = PrometheusAuthType.NOAUTH;
            private String username;
            private String password;
            private String region;
            private String accessKey;
            private String secretKey;

            Builder prometheusBaseUri(String prometheusBaseUri) {
                this.prometheusBaseUri = prometheusBaseUri;
                return this;
            }

            Builder authType(PrometheusAuthType authType) {
                this.authType = authType;
                return this;
            }

            Builder username(String username) {
                this.username = username;
                return this;
            }

            Builder password(String password) {
                this.password = password;
                return this;
            }

            Builder region(String region) {
                this.region = region;
                return this;
            }

            Builder accessKey(String accessKey) {
                this.accessKey = accessKey;
                return this;
            }

            Builder secretKey(String secretKey) {
                this.secretKey = secretKey;
                return this;
            }

            ResolvedPrometheusDataSource build() {
                return new ResolvedPrometheusDataSource(this);
            }
        }
    }

    static final class RangeQueryRequestContext {
        private final URI requestUri;
        private final String canonicalQueryString;

        RangeQueryRequestContext(URI requestUri, String canonicalQueryString) {
            this.requestUri = requestUri;
            this.canonicalQueryString = canonicalQueryString;
        }

        URI getRequestUri() {
            return requestUri;
        }

        String getCanonicalQueryString() {
            return canonicalQueryString;
        }
    }

    @FunctionalInterface
    private interface ResponseParser<T> {
        T parse(String body) throws IOException;
    }
}
