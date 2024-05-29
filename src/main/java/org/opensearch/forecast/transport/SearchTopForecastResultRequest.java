/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.model.FilterBy;
import org.opensearch.forecast.model.Subaggregation;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.util.ParseUtils;

/**
 * Request for getting the top forecast results for HC forecasters.
 * <p>
 * forecasterId, filterBy, and forecastFrom are required.
 * One or two of buildInQuery, entity, threshold, filterQuery, subaggregations will be set to
 * appropriate value depending on filterBy.
 * Other parameters will be set to default values if left blank.
 */
public class SearchTopForecastResultRequest extends ActionRequest implements ToXContentObject {

    private static final String TASK_ID_FIELD = "task_id";
    private static final String SIZE_FIELD = "size";
    private static final String SPLIT_BY_FIELD = "split_by";
    private static final String FILTER_BY_FIELD = "filter_by";
    private static final String BUILD_IN_QUERY_FIELD = "build_in_query";
    private static final String THRESHOLD_FIELD = "threshold";
    private static final String RELATION_TO_THRESHOLD_FIELD = "relation_to_threshold";
    private static final String FILTER_QUERY_FIELD = "filter_query";
    public static final String SUBAGGREGATIONS_FIELD = "subaggregations";
    // forecast from looks for data end time
    private static final String FORECAST_FROM_FIELD = "forecast_from";
    private static final String RUN_ONCE_FIELD = "run_once";

    private String forecasterId;
    private String taskId;
    private boolean runOnce;
    private Integer size;
    private List<String> splitBy;
    private FilterBy filterBy;
    private BuildInQuery buildInQuery;
    private Float threshold;
    private RelationalOperation relationToThreshold;
    private QueryBuilder filterQuery;
    private List<Subaggregation> subaggregations;
    private Instant forecastFrom;

    public SearchTopForecastResultRequest(StreamInput in) throws IOException {
        super(in);
        forecasterId = in.readOptionalString();
        taskId = in.readOptionalString();
        runOnce = in.readBoolean();
        size = in.readOptionalInt();
        splitBy = in.readOptionalStringList();
        if (in.readBoolean()) {
            filterBy = in.readEnum(FilterBy.class);
        } else {
            filterBy = null;
        }
        if (in.readBoolean()) {
            buildInQuery = in.readEnum(BuildInQuery.class);
        } else {
            buildInQuery = null;
        }
        threshold = in.readOptionalFloat();
        if (in.readBoolean()) {
            relationToThreshold = in.readEnum(RelationalOperation.class);
        } else {
            relationToThreshold = null;
        }
        if (in.readBoolean()) {
            filterQuery = in.readNamedWriteable(QueryBuilder.class);
        } else {
            filterQuery = null;
        }
        if (in.readBoolean()) {
            subaggregations = in.readList(Subaggregation::new);
        } else {
            subaggregations = null;
        }
        forecastFrom = in.readOptionalInstant();
    }

    public SearchTopForecastResultRequest(
        String forecasterId,
        String taskId,
        boolean runOnce,
        Integer size,
        List<String> splitBy,
        FilterBy filterBy,
        BuildInQuery buildInQuery,
        Float threshold,
        RelationalOperation relationToThreshold,
        QueryBuilder filterQuery,
        List<Subaggregation> subaggregations,
        Instant forecastFrom
    ) {
        super();
        this.forecasterId = forecasterId;
        this.taskId = taskId;
        this.runOnce = runOnce;
        this.size = size;
        this.splitBy = splitBy;
        this.filterBy = filterBy;
        this.buildInQuery = buildInQuery;
        this.threshold = threshold;
        this.relationToThreshold = relationToThreshold;
        this.filterQuery = filterQuery;
        this.subaggregations = subaggregations;
        this.forecastFrom = forecastFrom;
    }

    public String getTaskId() {
        return taskId;
    }

    public boolean isRunOnce() {
        return runOnce;
    }

    public Integer getSize() {
        return size;
    }

    public String getForecasterId() {
        return forecasterId;
    }

    public List<String> getSplitBy() {
        return splitBy;
    }

    public FilterBy getFilterBy() {
        return filterBy;
    }

    public BuildInQuery getBuildInQuery() {
        return buildInQuery;
    }

    public Float getThreshold() {
        return threshold;
    }

    public QueryBuilder getFilterQuery() {
        return filterQuery;
    }

    public List<Subaggregation> getSubaggregations() {
        return subaggregations;
    }

    public Instant getForecastFrom() {
        return forecastFrom;
    }

    public RelationalOperation getRelationToThreshold() {
        return relationToThreshold;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public void setForecasterId(String forecasterId) {
        this.forecasterId = forecasterId;
    }

    public void setRunOnce(boolean runOnce) {
        this.runOnce = runOnce;
    }

    public void setSplitBy(List<String> splitBy) {
        this.splitBy = splitBy;
    }

    public void setFilterBy(FilterBy filterBy) {
        this.filterBy = filterBy;
    }

    public void setBuildInQuery(BuildInQuery buildInQuery) {
        this.buildInQuery = buildInQuery;
    }

    public void setThreshold(Float threshold) {
        this.threshold = threshold;
    }

    public void setFilterQuery(QueryBuilder filterQuery) {
        this.filterQuery = filterQuery;
    }

    public void setSubaggregations(List<Subaggregation> subaggregations) {
        this.subaggregations = subaggregations;
    }

    public void setForecastFrom(Instant forecastFrom) {
        this.forecastFrom = forecastFrom;
    }

    public void setRelationToThreshold(RelationalOperation relationToThreshold) {
        this.relationToThreshold = relationToThreshold;
    }

    public static SearchTopForecastResultRequest parse(XContentParser parser, String forecasterId) throws IOException {
        String taskId = null;
        Integer size = null;
        List<String> splitBy = null;
        FilterBy filterBy = null;
        BuildInQuery buildInQuery = null;
        Float threshold = null;
        RelationalOperation relationToThreshold = null;
        QueryBuilder filterQuery = null;
        List<Subaggregation> subaggregations = new ArrayList<>();
        Instant forecastFrom = null;
        boolean runOnce = false;

        // "forecasterId" and "historical" params come from the original API path, not in the request body
        // and therefore don't need to be parsed
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case SIZE_FIELD:
                    size = parser.intValue();
                    break;
                case SPLIT_BY_FIELD:
                    splitBy = Arrays.asList(parser.text().split(","));
                    break;
                case FILTER_BY_FIELD:
                    filterBy = FilterBy.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    break;
                case BUILD_IN_QUERY_FIELD:
                    buildInQuery = BuildInQuery.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    break;
                case THRESHOLD_FIELD:
                    threshold = parser.floatValue();
                    break;
                case RELATION_TO_THRESHOLD_FIELD:
                    relationToThreshold = RelationalOperation.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    break;
                case FILTER_QUERY_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    try {
                        filterQuery = parseInnerQueryBuilder(parser);
                    } catch (ParsingException | XContentParseException e) {
                        throw new ValidationException(
                            "Custom query error in data filter: " + e.getMessage(),
                            ValidationIssueType.FILTER_QUERY,
                            ValidationAspect.FORECASTER
                        );
                    } catch (IllegalArgumentException e) {
                        if (!e.getMessage().contains("empty clause")) {
                            throw e;
                        }
                    }
                    break;
                case SUBAGGREGATIONS_FIELD:
                    try {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            subaggregations.add(Subaggregation.parse(parser));
                        }
                    } catch (Exception e) {
                        if (e instanceof ParsingException || e instanceof XContentParseException) {
                            throw new ValidationException(
                                "Custom query error: " + e.getMessage(),
                                ValidationIssueType.SUBAGGREGATION,
                                ValidationAspect.FORECASTER
                            );
                        }
                        throw e;
                    }
                    break;
                case FORECAST_FROM_FIELD:
                    forecastFrom = ParseUtils.toInstant(parser);
                    break;
                case RUN_ONCE_FIELD:
                    runOnce = parser.booleanValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new SearchTopForecastResultRequest(
            forecasterId,
            taskId,
            runOnce,
            size,
            splitBy,
            filterBy,
            buildInQuery,
            threshold,
            relationToThreshold,
            filterQuery,
            subaggregations,
            forecastFrom
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // "forecasterId" and "historical" params come from the original API path, not in the request body
        // and therefore don't need to be in the generated json
        builder
            .field(TASK_ID_FIELD, taskId)
            .field(SPLIT_BY_FIELD, String.join(",", splitBy))
            .field(FILTER_BY_FIELD, filterBy.name())
            .field(RUN_ONCE_FIELD, runOnce);

        if (size != null) {
            builder.field(SIZE_FIELD, size);
        }
        if (buildInQuery != null) {
            builder.field(BUILD_IN_QUERY_FIELD, buildInQuery);
        }
        if (threshold != null) {
            builder.field(THRESHOLD_FIELD, threshold);
        }
        if (relationToThreshold != null) {
            builder.field(RELATION_TO_THRESHOLD_FIELD, relationToThreshold);
        }
        if (filterQuery != null) {
            builder.field(FILTER_QUERY_FIELD, filterQuery);
        }
        if (subaggregations != null) {
            builder.field(SUBAGGREGATIONS_FIELD, subaggregations.toArray());
        }
        if (forecastFrom != null) {
            builder.field(FORECAST_FROM_FIELD, forecastFrom.toString());
        }

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(forecasterId);
        out.writeOptionalString(taskId);
        out.writeBoolean(runOnce);
        out.writeOptionalInt(size);
        out.writeOptionalStringCollection(splitBy);
        if (filterBy == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeEnum(filterBy);
        }
        if (buildInQuery == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeEnum(buildInQuery);
        }
        out.writeOptionalFloat(threshold);
        if (relationToThreshold == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeEnum(relationToThreshold);
        }
        if (filterQuery == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeNamedWriteable(filterQuery);
        }
        if (subaggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeList(subaggregations);
        }
        out.writeOptionalInstant(forecastFrom);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (forecasterId == null) {
            return addValidationError("Cannot find forecasterId", null);
        }
        if (filterBy == null) {
            return addValidationError("Must set filter_by", null);
        }
        if (forecastFrom == null) {
            return addValidationError("Must set forecast_from with epoch of milliseconds", null);
        }
        if (!((filterBy == FilterBy.BUILD_IN_QUERY) == (buildInQuery != null))) {
            throw new IllegalArgumentException(
                "If 'filter_by' is set to BUILD_IN_QUERY, a 'build_in_query' type must be provided. Otherwise, 'build_in_query' should not be given."
            );
        }

        if (filterBy == FilterBy.BUILD_IN_QUERY
            && buildInQuery == BuildInQuery.DISTANCE_TO_THRESHOLD_VALUE
            && (threshold == null || relationToThreshold == null)) {
            return addValidationError(
                String
                    .format(Locale.ROOT, "Must set threshold and relation_to_threshold, but get %s and %s", threshold, relationToThreshold),
                null
            );
        }
        if (filterBy == FilterBy.CUSTOM_QUERY && (subaggregations == null || subaggregations.isEmpty())) {
            return addValidationError("Must set subaggregations", null);
        }
        if (!runOnce && !Strings.isNullOrEmpty(taskId)) {
            return addValidationError("task id must not be set when run_once is false", null);
        }
        return null;
    }
}
