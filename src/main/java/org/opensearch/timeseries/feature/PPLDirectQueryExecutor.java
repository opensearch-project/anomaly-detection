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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.PPLSource;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Executes restricted single-stream PPL detector queries at runtime through the
 * SQL plugin's internal PPL transport action.
 */
public class PPLDirectQueryExecutor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter PPL_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd HH:mm:ss")
        .optionalStart()
        .appendFraction(ChronoField.MILLI_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);
    private static final ActionType<ActionResponse> PPL_QUERY_ACTION = new ActionType<>(
        "cluster:admin/opensearch/ppl",
        PPLTransportResponse::new
    );
    private static final String PPL_QUERY_PATH = "/_plugins/_ppl";
    private static final String PPL_RESPONSE_FORMAT = "jdbc";

    private final Client client;
    private final SecurityClientUtil clientUtil;

    public PPLDirectQueryExecutor(Client client, SecurityClientUtil clientUtil) {
        this.client = client;
        this.clientUtil = clientUtil;
    }

    public void executeMetricQuery(
        Config config,
        long startTimeMs,
        long endTimeMs,
        AnalysisType context,
        ActionListener<Optional<double[]>> listener
    ) {
        final PPLSource.CompiledPPLQuery compiledQuery;
        try {
            compiledQuery = config.getPPLSource().compile();
        } catch (IllegalArgumentException e) {
            listener.onFailure(e);
            return;
        }

        executeQuery(
            null,
            config,
            compiled -> compiledQuery.buildMetricQueryForRange(startTimeMs, endTimeMs),
            context,
            responseBody -> parseMetricValues(responseBody, compiledQuery.getMetricCount()),
            listener
        );
    }

    public void executeLatestDataTimeQuery(User user, Config config, AnalysisType context, ActionListener<Optional<Long>> listener) {
        executeQuery(
            user,
            config,
            PPLSource.CompiledPPLQuery::buildLatestTimeQuery,
            context,
            PPLDirectQueryExecutor::parseSingleTimestamp,
            listener
        );
    }

    public void executeMinDataTimeQuery(Config config, AnalysisType context, ActionListener<Optional<Long>> listener) {
        executeQuery(
            null,
            config,
            PPLSource.CompiledPPLQuery::buildMinTimeQuery,
            context,
            PPLDirectQueryExecutor::parseSingleTimestamp,
            listener
        );
    }

    public void executeDateRangeQuery(User user, Config config, AnalysisType context, ActionListener<Pair<Long, Long>> listener) {
        executeQuery(
            user,
            config,
            PPLSource.CompiledPPLQuery::buildDateRangeQuery,
            context,
            PPLDirectQueryExecutor::parseDateRange,
            listener
        );
    }

    private <T> void executeQuery(
        User user,
        Config config,
        Function<PPLSource.CompiledPPLQuery, String> queryBuilder,
        AnalysisType context,
        ResponseParser<T> responseParser,
        ActionListener<T> listener
    ) {
        if (config == null || config.getPPLSource() == null) {
            listener.onFailure(new IllegalArgumentException("ppl_source must be set when source_type is PPL."));
            return;
        }

        final PPLSource.CompiledPPLQuery compiledQuery;
        try {
            compiledQuery = config.getPPLSource().compile();
        } catch (IllegalArgumentException e) {
            listener.onFailure(e);
            return;
        }

        PPLTransportRequest request = new PPLTransportRequest(queryBuilder.apply(compiledQuery), PPL_RESPONSE_FORMAT, PPL_QUERY_PATH);
        ActionListener<ActionResponse> queryResponseListener = ActionListener.wrap(response -> {
            try {
                listener.onResponse(responseParser.parse(PPLTransportResponse.fromActionResponse(response).getResult()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);

        if (user != null) {
            clientUtil
                .asyncRequestWithInjectedSecurity(
                    request,
                    (pplRequest, actionListener) -> client.execute(PPL_QUERY_ACTION, pplRequest, actionListener),
                    user,
                    client,
                    context,
                    queryResponseListener
                );
        } else {
            clientUtil
                .asyncRequestWithInjectedSecurity(
                    request,
                    (pplRequest, actionListener) -> client.execute(PPL_QUERY_ACTION, pplRequest, actionListener),
                    config.getId(),
                    client,
                    context,
                    queryResponseListener
                );
        }
    }

    private static Optional<double[]> parseMetricValues(String responseBody, int metricCount) throws IOException {
        JsonNode datarows = OBJECT_MAPPER.readTree(responseBody).path("datarows");
        if (!datarows.isArray() || datarows.isEmpty()) {
            return Optional.empty();
        }

        JsonNode firstRow = datarows.get(0);
        if (!firstRow.isArray() || firstRow.size() < metricCount) {
            return Optional.empty();
        }

        double[] values = new double[metricCount];
        for (int i = 0; i < metricCount; i++) {
            JsonNode valueNode = firstRow.get(i);
            if (valueNode == null || valueNode.isNull()) {
                return Optional.empty();
            }
            if (valueNode.isNumber()) {
                values[i] = valueNode.doubleValue();
                continue;
            }
            if (valueNode.isTextual() && valueNode.textValue() != null && !valueNode.textValue().isBlank()) {
                values[i] = Double.parseDouble(valueNode.textValue());
                continue;
            }
            return Optional.empty();
        }
        return Optional.of(values);
    }

    private static Optional<Long> parseSingleTimestamp(String responseBody) throws IOException {
        JsonNode valueNode = getFirstRowValue(responseBody, 0);
        return parseTimestampValue(valueNode);
    }

    private static Pair<Long, Long> parseDateRange(String responseBody) throws IOException {
        Optional<Long> minTime = parseTimestampValue(getFirstRowValue(responseBody, 0));
        Optional<Long> maxTime = parseTimestampValue(getFirstRowValue(responseBody, 1));
        if (minTime.isEmpty() || maxTime.isEmpty()) {
            throw new IllegalStateException("PPL date range query did not return both min and max timestamps.");
        }
        return Pair.of(minTime.get(), maxTime.get());
    }

    private static JsonNode getFirstRowValue(String responseBody, int columnIndex) throws IOException {
        JsonNode datarows = OBJECT_MAPPER.readTree(responseBody).path("datarows");
        if (!datarows.isArray() || datarows.isEmpty()) {
            return null;
        }
        JsonNode firstRow = datarows.get(0);
        if (!firstRow.isArray() || firstRow.size() <= columnIndex) {
            return null;
        }
        return firstRow.get(columnIndex);
    }

    private static Optional<Long> parseTimestampValue(JsonNode valueNode) {
        if (valueNode == null || valueNode.isNull()) {
            return Optional.empty();
        }

        if (valueNode.isNumber()) {
            return Optional.of(valueNode.longValue());
        }

        if (valueNode.isTextual() && valueNode.textValue() != null && !valueNode.textValue().isBlank()) {
            LocalDateTime localDateTime = LocalDateTime.parse(valueNode.textValue(), PPL_TIMESTAMP_FORMATTER);
            return Optional.of(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
        }

        return Optional.empty();
    }

    private interface ResponseParser<T> {
        T parse(String responseBody) throws Exception;
    }

    private enum PPLJsonStyle {
        PRETTY,
        COMPACT
    }

    private static final class PPLTransportRequest extends ActionRequest {
        private final String query;
        private final String format;
        private final String explainMode;
        private final String jsonContent;
        private final String path;
        private final boolean sanitize;
        private final boolean profile;
        private final String queryId;

        private PPLTransportRequest(String query, String format, String path) {
            this.query = query;
            this.format = format;
            this.explainMode = null;
            this.jsonContent = null;
            this.path = path;
            this.sanitize = true;
            this.profile = false;
            this.queryId = null;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(query);
            out.writeOptionalString(format);
            out.writeOptionalString(explainMode);
            out.writeOptionalString(jsonContent);
            out.writeOptionalString(path);
            out.writeBoolean(sanitize);
            out.writeEnum(PPLJsonStyle.COMPACT);
            out.writeBoolean(profile);
            out.writeOptionalString(queryId);
        }
    }

    private static final class PPLTransportResponse extends ActionResponse {
        private final String result;
        private final String contentType;

        private PPLTransportResponse(StreamInput in) throws IOException {
            super(in);
            this.result = in.readString();
            this.contentType = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(result);
            out.writeString(contentType);
        }

        private static PPLTransportResponse fromActionResponse(ActionResponse actionResponse) {
            if (actionResponse instanceof PPLTransportResponse) {
                return (PPLTransportResponse) actionResponse;
            }

            try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                OutputStreamStreamOutput output = new OutputStreamStreamOutput(baos)
            ) {
                actionResponse.writeTo(output);
                try (InputStreamStreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                    return new PPLTransportResponse(input);
                }
            } catch (IOException e) {
                throw new IllegalStateException("failed to parse ActionResponse into local PPL transport response", e);
            }
        }

        private String getResult() {
            return result;
        }
    }
}
