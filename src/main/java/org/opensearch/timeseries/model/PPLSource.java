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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.logging.log4j.util.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.annotation.Generated;

import com.google.common.base.Objects;

/**
 * Stores the original PPL query for detectors created from a detector-friendly
 * single-stream PPL form. The query is compiled into detector metadata at
 * creation time, then executed through the PPL engine at runtime.
 */
public class PPLSource implements Writeable, ToXContentObject {

    public static final String QUERY_LANGUAGE_FIELD = "query_language";
    public static final String QUERY_FIELD = "query";
    public static final String PPL_QUERY_LANGUAGE = "PPL";

    private static final String STATS_ERROR_MESSAGE =
        "Unsupported PPL stats clause. Expected a final stage shaped like: stats <agg>(...) [as <feature>][, ...] by span(<time_field>, <interval>), where interval uses s or m.";
    private static final DateTimeFormatter QUERY_TIMESTAMP_FORMATTER = DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss.SSS", Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    private final String queryLanguage;
    private final String query;

    public PPLSource(String queryLanguage, String query) {
        this.queryLanguage = queryLanguage;
        this.query = query;
    }

    public PPLSource(StreamInput in) throws IOException {
        this.queryLanguage = in.readOptionalString();
        this.query = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(queryLanguage);
        out.writeOptionalString(query);
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
        return builder.endObject();
    }

    public static PPLSource parse(XContentParser parser) throws IOException {
        String queryLanguage = null;
        String query = null;

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
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new PPLSource(queryLanguage, query);
    }

    public CompiledPPLQuery compile() {
        return compile(query);
    }

    public static CompiledPPLQuery compile(String query) {
        if (Strings.isBlank(query)) {
            throw new IllegalArgumentException("ppl_source.query must be set when source_type is PPL.");
        }

        List<String> stages = splitTopLevel(query.trim(), '|');
        if (stages.size() < 2) {
            throw new IllegalArgumentException(
                "Unsupported ppl_source.query. Expected a PPL pipeline that starts with source = <index> and ends with stats ... by span(...)."
            );
        }

        SourceStage sourceStage = parseSourceStage(stages.get(0));
        int statsStageIndex = findFinalStatsStageIndex(stages);
        if (statsStageIndex <= 0) {
            throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
        }

        for (int i = statsStageIndex + 1; i < stages.size(); i++) {
            String stage = stages.get(i);
            if (!isSortStage(stage)) {
                throw new IllegalArgumentException(
                    "Unsupported PPL pipeline stage [" + stage + "] after the final stats stage. Only trailing sort stages are supported."
                );
            }
        }

        List<String> preStatsStages = new ArrayList<>(Math.max(0, statsStageIndex - 1));
        for (int i = 1; i < statsStageIndex; i++) {
            String stage = stages.get(i).trim();
            if (stage.isEmpty()) {
                continue;
            }
            if (isSourceStage(stage)) {
                throw new IllegalArgumentException("PPL detector queries must contain exactly one source stage.");
            }
            if (isStatsStage(stage)) {
                throw new IllegalArgumentException(
                    "Only one final stats stage is supported in ppl_source.query for detector creation."
                );
            }
            preStatsStages.add(stage);
        }

        StatsStage statsStage = parseStatsStage(stages.get(statsStageIndex));
        return new CompiledPPLQuery(
            sourceStage.getIndex(),
            Collections.unmodifiableList(preStatsStages),
            statsStage.getTimeField(),
            statsStage.getMetrics(),
            statsStage.getInterval()
        );
    }

    private static SourceStage parseSourceStage(String stage) {
        String trimmed = stage.trim();
        int equalsIndex = trimmed.indexOf('=');
        if (!startsWithKeyword(trimmed, "source") || equalsIndex < 0) {
            throw new IllegalArgumentException("Unsupported PPL source clause. Expected: source = <index>.");
        }

        String index = stripBackticks(trimmed.substring(equalsIndex + 1));
        if (Strings.isBlank(index) || index.contains(",")) {
            throw new IllegalArgumentException("PPL source must reference exactly one index or index pattern.");
        }
        return new SourceStage(index);
    }

    private static int findFinalStatsStageIndex(List<String> stages) {
        int lastStatsStageIndex = -1;
        for (int i = stages.size() - 1; i >= 1; i--) {
            String stage = stages.get(i).trim();
            if (stage.isEmpty()) {
                continue;
            }
            if (isStatsStage(stage)) {
                lastStatsStageIndex = i;
                break;
            }
        }
        return lastStatsStageIndex;
    }

    private static StatsStage parseStatsStage(String stage) {
        String trimmed = stage.trim();
        if (!isStatsStage(trimmed)) {
            throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
        }

        String statsBody = trimmed.substring("stats".length()).trim();
        int bySpanIndex = findTopLevelBySpanIndex(statsBody);
        if (bySpanIndex < 0) {
            throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
        }

        String metricsClause = statsBody.substring(0, bySpanIndex).trim();
        String spanClause = statsBody.substring(bySpanIndex).trim();

        if (!startsWithKeyword(spanClause, "by")) {
            throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
        }
        spanClause = spanClause.substring(2).trim();
        if (!startsWithKeyword(spanClause, "span")) {
            throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
        }

        int openParenIndex = spanClause.indexOf('(');
        int closeParenIndex = findMatchingParen(spanClause, openParenIndex);
        if (openParenIndex < 0 || closeParenIndex < 0) {
            throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
        }

        String spanArgs = spanClause.substring(openParenIndex + 1, closeParenIndex).trim();
        List<String> spanParts = splitTopLevel(spanArgs, ',');
        if (spanParts.size() != 2) {
            throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
        }

        String trailingSpanClause = spanClause.substring(closeParenIndex + 1).trim();
        if (!trailingSpanClause.isEmpty()) {
            int aliasSeparator = findTopLevelAsSeparator(trailingSpanClause);
            if (aliasSeparator != 0 || trailingSpanClause.substring(aliasSeparator + 2).trim().isEmpty()) {
                // `as bucket` is allowed, but we do not consume the alias.
                throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
            }
        }

        String timeField = stripBackticks(spanParts.get(0));
        if (Strings.isBlank(timeField) || timeField.contains(" ")) {
            throw new IllegalArgumentException("PPL span time field must be a single field reference.");
        }

        List<MetricSpec> metrics = parseMetrics(metricsClause);
        IntervalTimeConfiguration intervalConfiguration = parseInterval(spanParts.get(1).trim());
        return new StatsStage(metrics, timeField, intervalConfiguration);
    }

    private static List<MetricSpec> parseMetrics(String metricsClause) {
        List<String> metricExpressions = splitTopLevel(metricsClause, ',');
        if (metricExpressions.isEmpty()) {
            throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
        }

        List<MetricSpec> metrics = new ArrayList<>(metricExpressions.size());
        Set<String> seenFeatureNames = new HashSet<>();
        for (String metricExpression : metricExpressions) {
            MetricSpec metric = parseMetric(metricExpression.trim());
            if (!seenFeatureNames.add(metric.getFeatureName())) {
                throw new IllegalArgumentException(
                    "Duplicate metric alias [" + metric.getFeatureName() + "] is not supported in ppl_source.query."
                );
            }
            metrics.add(metric);
        }
        return Collections.unmodifiableList(metrics);
    }

    private static MetricSpec parseMetric(String metricExpression) {
        int aliasSeparator = findTopLevelAsSeparator(metricExpression);
        String metricCore = aliasSeparator >= 0 ? metricExpression.substring(0, aliasSeparator).trim() : metricExpression.trim();
        String alias = aliasSeparator >= 0 ? stripBackticks(metricExpression.substring(aliasSeparator + 2).trim()) : null;

        int openParenIndex = metricCore.indexOf('(');
        int closeParenIndex = findMatchingParen(metricCore, openParenIndex);
        if (openParenIndex <= 0 || closeParenIndex != metricCore.length() - 1) {
            throw new IllegalArgumentException(STATS_ERROR_MESSAGE);
        }

        String aggregationType = metricCore.substring(0, openParenIndex).trim().toLowerCase(Locale.ROOT);
        if (!isSupportedAggregationType(aggregationType)) {
            throw new IllegalArgumentException(
                "Unsupported aggregation [" + aggregationType + "] in ppl_source.query. Supported aggregations are count, sum, avg, min, and max."
            );
        }

        String argumentExpression = metricCore.substring(openParenIndex + 1, closeParenIndex).trim();
        boolean countAll = "count".equals(aggregationType) && (argumentExpression.isEmpty() || "*".equals(argumentExpression));
        if (!countAll && Strings.isBlank(argumentExpression)) {
            throw new IllegalArgumentException("PPL aggregation arguments cannot be empty unless using count().");
        }

        String normalizedArgument = countAll ? null : argumentExpression;
        String resolvedFeatureName = Strings.isBlank(alias)
            ? defaultFeatureName(aggregationType, normalizedArgument, countAll)
            : alias;

        return new MetricSpec(aggregationType, normalizedArgument, resolvedFeatureName, countAll);
    }

    private static boolean isSupportedAggregationType(String aggregationType) {
        return "count".equals(aggregationType)
            || "sum".equals(aggregationType)
            || "avg".equals(aggregationType)
            || "min".equals(aggregationType)
            || "max".equals(aggregationType);
    }

    private static String defaultFeatureName(String aggregationType, String argumentExpression, boolean countAll) {
        if (countAll) {
            return "count_all";
        }
        String sanitized = sanitizeIdentifier(stripBackticks(argumentExpression));
        return aggregationType + "_" + (sanitized.isEmpty() ? "value" : sanitized);
    }

    private static String sanitizeIdentifier(String value) {
        return (value == null ? "" : value).replaceAll("[^A-Za-z0-9_]+", "_").replaceAll("^_+|_+$", "");
    }

    private static List<String> splitTopLevel(String value, char delimiter) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBackticks = false;

        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            char previous = i > 0 ? value.charAt(i - 1) : '\0';

            if (ch == '\'' && !inDoubleQuote && !inBackticks && previous != '\\') {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '"' && !inSingleQuote && !inBackticks && previous != '\\') {
                inDoubleQuote = !inDoubleQuote;
            } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
                inBackticks = !inBackticks;
            } else if (!inSingleQuote && !inDoubleQuote && !inBackticks) {
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth = Math.max(0, depth - 1);
                }
            }

            if (ch == delimiter && depth == 0 && !inSingleQuote && !inDoubleQuote && !inBackticks) {
                String part = current.toString().trim();
                if (!part.isEmpty()) {
                    parts.add(part);
                }
                current.setLength(0);
                continue;
            }

            current.append(ch);
        }

        String part = current.toString().trim();
        if (!part.isEmpty()) {
            parts.add(part);
        }
        return parts;
    }

    private static int findTopLevelBySpanIndex(String statsBody) {
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBackticks = false;

        for (int i = 0; i < statsBody.length(); i++) {
            char ch = statsBody.charAt(i);
            char previous = i > 0 ? statsBody.charAt(i - 1) : '\0';

            if (ch == '\'' && !inDoubleQuote && !inBackticks && previous != '\\') {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '"' && !inSingleQuote && !inBackticks && previous != '\\') {
                inDoubleQuote = !inDoubleQuote;
            } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
                inBackticks = !inBackticks;
            } else if (!inSingleQuote && !inDoubleQuote && !inBackticks) {
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth = Math.max(0, depth - 1);
                }
            }

            if (depth == 0 && !inSingleQuote && !inDoubleQuote && !inBackticks && matchesKeywordWithLeadingWhitespace(statsBody, i, "by")) {
                String tail = statsBody.substring(i).trim();
                if (tail.length() >= 2 && startsWithKeyword(tail, "by")) {
                    String afterBy = tail.substring(2).trim();
                    if (startsWithKeyword(afterBy, "span")) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private static int findTopLevelAsSeparator(String value) {
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBackticks = false;

        for (int i = 0; i <= value.length() - 2; i++) {
            char ch = value.charAt(i);
            char previous = i > 0 ? value.charAt(i - 1) : '\0';

            if (ch == '\'' && !inDoubleQuote && !inBackticks && previous != '\\') {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '"' && !inSingleQuote && !inBackticks && previous != '\\') {
                inDoubleQuote = !inDoubleQuote;
            } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
                inBackticks = !inBackticks;
            } else if (!inSingleQuote && !inDoubleQuote && !inBackticks) {
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth = Math.max(0, depth - 1);
                }
            }

            if (depth == 0 && !inSingleQuote && !inDoubleQuote && !inBackticks && matchesKeywordWithWhitespace(value, i, "as")) {
                return i;
            }
        }
        return -1;
    }

    private static int findMatchingParen(String value, int openParenIndex) {
        if (openParenIndex < 0 || openParenIndex >= value.length() || value.charAt(openParenIndex) != '(') {
            return -1;
        }

        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBackticks = false;

        for (int i = openParenIndex; i < value.length(); i++) {
            char ch = value.charAt(i);
            char previous = i > 0 ? value.charAt(i - 1) : '\0';

            if (ch == '\'' && !inDoubleQuote && !inBackticks && previous != '\\') {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '"' && !inSingleQuote && !inBackticks && previous != '\\') {
                inDoubleQuote = !inDoubleQuote;
            } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
                inBackticks = !inBackticks;
            } else if (!inSingleQuote && !inDoubleQuote && !inBackticks) {
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth--;
                    if (depth == 0) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private static boolean matchesKeywordWithWhitespace(String value, int startIndex, String keyword) {
        int endIndex = startIndex + keyword.length();
        if (endIndex > value.length()) {
            return false;
        }
        if (!value.regionMatches(true, startIndex, keyword, 0, keyword.length())) {
            return false;
        }

        boolean validLeft = startIndex == 0 || Character.isWhitespace(value.charAt(startIndex - 1));
        boolean validRight = endIndex == value.length() || Character.isWhitespace(value.charAt(endIndex));
        return validLeft && validRight;
    }

    private static boolean matchesKeywordWithLeadingWhitespace(String value, int startIndex, String keyword) {
        if (!matchesKeywordWithWhitespace(value, startIndex, keyword)) {
            return false;
        }
        return startIndex == 0 || Character.isWhitespace(value.charAt(startIndex - 1));
    }

    private static IntervalTimeConfiguration parseInterval(String interval) {
        String trimmed = interval == null ? "" : interval.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Unsupported PPL span interval [" + interval + "].");
        }

        int numericEnd = 0;
        while (numericEnd < trimmed.length() && Character.isDigit(trimmed.charAt(numericEnd))) {
            numericEnd++;
        }
        if (numericEnd == 0) {
            throw new IllegalArgumentException("Unsupported PPL span interval [" + interval + "].");
        }

        String numericText = trimmed.substring(0, numericEnd);
        String unitText = trimmed.substring(numericEnd).trim().toLowerCase(Locale.ROOT);
        if (unitText.length() != 1) {
            throw new IllegalArgumentException("Unsupported PPL span interval [" + interval + "].");
        }

        int value = Integer.parseInt(numericText);
        if (value <= 0) {
            throw new IllegalArgumentException("PPL span interval must be positive.");
        }

        switch (unitText.charAt(0)) {
            case 's':
                return new IntervalTimeConfiguration(value, ChronoUnit.SECONDS);
            case 'm':
                return new IntervalTimeConfiguration(value, ChronoUnit.MINUTES);
            default:
                throw new IllegalArgumentException(
                    "Unsupported PPL span interval unit [" + unitText + "]. Only seconds (s) and minutes (m) are supported."
                );
        }
    }

    private static boolean startsWithKeyword(String value, String keyword) {
        String trimmed = value == null ? "" : value.trim();
        if (trimmed.length() < keyword.length()) {
            return false;
        }
        if (!trimmed.regionMatches(true, 0, keyword, 0, keyword.length())) {
            return false;
        }
        return trimmed.length() == keyword.length()
            || Character.isWhitespace(trimmed.charAt(keyword.length()))
            || trimmed.charAt(keyword.length()) == '='
            || trimmed.charAt(keyword.length()) == '(';
    }

    private static boolean isSourceStage(String stage) {
        return startsWithKeyword(stage, "source");
    }

    private static boolean isWhereStage(String stage) {
        return startsWithKeyword(stage, "where");
    }

    private static boolean isStatsStage(String stage) {
        return startsWithKeyword(stage, "stats");
    }

    private static boolean isSortStage(String stage) {
        return startsWithKeyword(stage, "sort");
    }

    private static String stripBackticks(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.startsWith("`") && trimmed.endsWith("`") && trimmed.length() >= 2) {
            return trimmed.substring(1, trimmed.length() - 1).trim();
        }
        return trimmed;
    }

    public String getQueryLanguage() {
        return queryLanguage;
    }

    public String getQuery() {
        return query;
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
        PPLSource that = (PPLSource) o;
        return Objects.equal(queryLanguage, that.queryLanguage) && Objects.equal(query, that.query);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(queryLanguage, query);
    }

    public static class CompiledPPLQuery {
        private final String index;
        private final List<String> preStatsStages;
        private final String timeField;
        private final List<MetricSpec> metrics;
        private final IntervalTimeConfiguration interval;

        CompiledPPLQuery(
            String index,
            List<String> preStatsStages,
            String timeField,
            List<MetricSpec> metrics,
            IntervalTimeConfiguration interval
        ) {
            this.index = index;
            this.preStatsStages = preStatsStages;
            this.timeField = timeField;
            this.metrics = metrics;
            this.interval = interval;
        }

        public String getIndex() {
            return index;
        }

        public List<String> getPreStatsStages() {
            return preStatsStages;
        }

        public String getTimeField() {
            return timeField;
        }

        public List<MetricSpec> getMetrics() {
            return metrics;
        }

        public int getMetricCount() {
            return metrics.size();
        }

        public List<String> getFeatureNames() {
            List<String> featureNames = new ArrayList<>(metrics.size());
            for (MetricSpec metric : metrics) {
                featureNames.add(metric.getFeatureName());
            }
            return Collections.unmodifiableList(featureNames);
        }

        public IntervalTimeConfiguration getInterval() {
            return interval;
        }

        public List<Feature> toPlaceholderFeatures() {
            List<Feature> features = new ArrayList<>(metrics.size());
            for (MetricSpec metric : metrics) {
                features.add(Feature.createDirectQueryPlaceholder(UUIDs.base64UUID(), metric.getFeatureName(), true));
            }
            return features;
        }

        public String buildMetricQueryForRange(long startTimeMs, long endTimeMs) {
            List<String> metricsExpressions = new ArrayList<>(metrics.size());
            for (MetricSpec metric : metrics) {
                metricsExpressions.add(metric.toStatsExpression());
            }
            return buildStatsQuery(
                String.join(", ", metricsExpressions),
                String.format(
                    Locale.ROOT,
                    "%s >= \"%s\" and %s < \"%s\"",
                    maybeQuote(timeField),
                    formatQueryTimestamp(startTimeMs),
                    maybeQuote(timeField),
                    formatQueryTimestamp(endTimeMs)
                )
            );
        }

        public String buildLatestTimeQuery() {
            return buildStatsQuery("max(" + maybeQuote(timeField) + ") as latest_time", null);
        }

        public String buildMinTimeQuery() {
            return buildStatsQuery("min(" + maybeQuote(timeField) + ") as min_time", null);
        }

        public String buildDateRangeQuery() {
            return buildStatsQuery(
                "min(" + maybeQuote(timeField) + ") as min_time, max(" + maybeQuote(timeField) + ") as max_time",
                null
            );
        }

        private String buildStatsQuery(String statsExpression, String appendedWhereClause) {
            StringBuilder builder = new StringBuilder();
            builder.append("source = ").append(maybeQuote(index));
            for (String stage : preStatsStages) {
                builder.append(" | ").append(stage);
            }
            if (!Strings.isBlank(appendedWhereClause)) {
                builder.append(" | where ").append(appendedWhereClause);
            }
            builder.append(" | stats ").append(statsExpression);
            return builder.toString();
        }

        private static String formatQueryTimestamp(long epochMillis) {
            return QUERY_TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(epochMillis));
        }

        private static String maybeQuote(String identifier) {
            if (identifier == null) {
                return null;
            }
            String trimmed = identifier.trim();
            return trimmed.matches("[A-Za-z0-9_.*:-]+") ? trimmed : "`" + trimmed.replace("`", "\\`") + "`";
        }
    }

    public static class MetricSpec {
        private final String aggregationType;
        private final String aggregationField;
        private final String featureName;
        private final boolean countAll;

        MetricSpec(String aggregationType, String aggregationField, String featureName, boolean countAll) {
            this.aggregationType = aggregationType;
            this.aggregationField = aggregationField;
            this.featureName = featureName;
            this.countAll = countAll;
        }

        public String getAggregationType() {
            return aggregationType;
        }

        public String getAggregationField() {
            return aggregationField;
        }

        public String getFeatureName() {
            return featureName;
        }

        public boolean isCountAll() {
            return countAll;
        }

        private String toStatsExpression() {
            if (countAll) {
                return "count() as " + CompiledPPLQuery.maybeQuote(featureName);
            }
            return aggregationType + "(" + aggregationField + ") as " + CompiledPPLQuery.maybeQuote(featureName);
        }
    }

    private static class SourceStage {
        private final String index;

        SourceStage(String index) {
            this.index = index;
        }

        public String getIndex() {
            return index;
        }
    }

    private static class StatsStage {
        private final List<MetricSpec> metrics;
        private final String timeField;
        private final IntervalTimeConfiguration interval;

        StatsStage(List<MetricSpec> metrics, String timeField, IntervalTimeConfiguration interval) {
            this.metrics = metrics;
            this.timeField = timeField;
            this.interval = interval;
        }

        public List<MetricSpec> getMetrics() {
            return metrics;
        }

        public String getTimeField() {
            return timeField;
        }

        public IntervalTimeConfiguration getInterval() {
            return interval;
        }
    }
}
