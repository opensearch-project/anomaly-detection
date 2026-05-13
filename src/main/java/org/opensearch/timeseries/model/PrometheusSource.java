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

package org.opensearch.timeseries.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.annotation.Generated;

import com.google.common.base.Objects;

/**
 * Prometheus source configuration for external metrics ingestion.
 */
public class PrometheusSource implements Writeable, ToXContentObject {

    public static final String QUERY_LANGUAGE_FIELD = "query_language";
    public static final String QUERY_FIELD = "query";
    public static final String DATA_CONNECTION_ID_FIELD = "data_connection_id";
    public static final String SERIES_FILTER_FIELD = "series_filter";

    private final String queryLanguage;
    private final String query;
    private final String dataConnectionId;
    private final Map<String, String> seriesFilter;

    public PrometheusSource(String queryLanguage, String query, String dataConnectionId) {
        this(queryLanguage, query, dataConnectionId, null);
    }

    public PrometheusSource(String queryLanguage, String query, String dataConnectionId, Map<String, String> seriesFilter) {
        this.queryLanguage = queryLanguage;
        this.query = query;
        this.dataConnectionId = dataConnectionId;
        this.seriesFilter = seriesFilter == null || seriesFilter.isEmpty()
            ? null
            : Collections.unmodifiableMap(new LinkedHashMap<>(seriesFilter));
    }

    public PrometheusSource(StreamInput in) throws IOException {
        this.queryLanguage = in.readOptionalString();
        this.query = in.readOptionalString();
        this.dataConnectionId = in.readOptionalString();
        this.seriesFilter = in.readBoolean()
            ? Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString))
            : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(queryLanguage);
        out.writeOptionalString(query);
        out.writeOptionalString(dataConnectionId);
        out.writeBoolean(seriesFilter != null);
        if (seriesFilter != null) {
            out.writeMap(seriesFilter, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (queryLanguage != null) {
            builder.field(QUERY_LANGUAGE_FIELD, queryLanguage);
        }
        if (query != null) {
            builder.field(QUERY_FIELD, query);
        }
        if (dataConnectionId != null) {
            builder.field(DATA_CONNECTION_ID_FIELD, dataConnectionId);
        }
        if (seriesFilter != null && !seriesFilter.isEmpty()) {
            builder.field(SERIES_FILTER_FIELD, seriesFilter);
        }
        return builder.endObject();
    }

    public static PrometheusSource parse(XContentParser parser) throws IOException {
        String queryLanguage = null;
        String query = null;
        String dataConnectionId = null;
        Map<String, String> seriesFilter = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case QUERY_LANGUAGE_FIELD:
                    queryLanguage = parser.textOrNull();
                    break;
                case QUERY_FIELD:
                    query = parser.textOrNull();
                    break;
                case DATA_CONNECTION_ID_FIELD:
                    dataConnectionId = parser.textOrNull();
                    break;
                case SERIES_FILTER_FIELD:
                    if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                        seriesFilter = new LinkedHashMap<>();
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            String labelName = parser.currentName();
                            parser.nextToken();
                            seriesFilter.put(labelName, parser.textOrNull());
                        }
                    } else {
                        parser.skipChildren();
                    }
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new PrometheusSource(queryLanguage, query, dataConnectionId, seriesFilter);
    }

    public String getQueryLanguage() {
        return queryLanguage;
    }

    public String getQuery() {
        return query;
    }

    public String getDataConnectionId() {
        return dataConnectionId;
    }

    public Map<String, String> getSeriesFilter() {
        return seriesFilter;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrometheusSource that = (PrometheusSource) o;
        return Objects.equal(queryLanguage, that.queryLanguage)
            && Objects.equal(query, that.query)
            && Objects.equal(dataConnectionId, that.dataConnectionId)
            && Objects.equal(seriesFilter, that.seriesFilter);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(queryLanguage, query, dataConnectionId, seriesFilter);
    }
}
