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

package org.opensearch.timeseries.util;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.Nullable;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.indices.InvalidIndexNameException;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Feature;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

/**
 * Utility functions for REST handlers.
 */
public final class RestHandlerUtils {
    private static final Logger logger = LogManager.getLogger(RestHandlerUtils.class);
    public static final String _ID = "_id";
    public static final String _VERSION = "_version";
    public static final String _SEQ_NO = "_seq_no";
    public static final String IF_SEQ_NO = "if_seq_no";
    public static final String _PRIMARY_TERM = "_primary_term";
    public static final String IF_PRIMARY_TERM = "if_primary_term";
    public static final String REFRESH = "refresh";
    public static final String RESULT_INDEX = "resultIndex";
    public static final String REALTIME_TASK = "realtime_task";
    public static final String HISTORICAL_ANALYSIS_TASK = "historical_analysis_task";
    public static final String RUN = "_run";
    public static final String PREVIEW = "_preview";
    public static final String START_JOB = "_start";
    public static final String STOP_JOB = "_stop";
    public static final String INSIGHTS_START = "_start";
    public static final String INSIGHTS_STOP = "_stop";
    public static final String INSIGHTS_RESULTS = "_results";
    public static final String INSIGHTS_STATUS = "_status";
    public static final String PROFILE = "_profile";
    public static final String TYPE = "type";
    public static final String ENTITY = "entity";
    public static final String COUNT = "count";
    public static final String MATCH = "match";
    public static final String RESULTS = "results";
    public static final String VALIDATE = "_validate";
    public static final String SEARCH = "_search";
    public static final ToXContent.MapParams XCONTENT_WITH_TYPE = new ToXContent.MapParams(ImmutableMap.of("with_type", "true"));
    public static final String REST_STATUS = "rest_status";
    public static final String RUN_ONCE = "_run_once";
    public static final String SUGGEST = "_suggest";
    public static final String RUN_ONCE_TASK = "run_once_task";

    public static final String OPENSEARCH_DASHBOARDS_USER_AGENT = "OpenSearch Dashboards";
    public static final String[] UI_METADATA_EXCLUDE = new String[] { Config.UI_METADATA_FIELD };
    public static final String NODE_ID = "nodeId";
    public static final String STATS = "stats";
    public static final String STAT = "stat";

    // AD constants
    public static final String DETECTOR_ID = "detectorID";
    public static final String ANOMALY_DETECTOR = "anomaly_detector";
    public static final String ANOMALY_DETECTOR_JOB = "anomaly_detector_job";
    public static final String TOP_ANOMALIES = "_topAnomalies";
    public static final String INDEX = "index";
    public static final String FREQUENCY = "frequency";
    public static final String FROM = "from";
    public static final String SIZE = "size";

    // forecast constants
    public static final String FORECASTER_ID = "forecasterID";
    public static final String FORECASTER = "forecaster";
    public static final String FORECASTER_JOB = "forecaster_job";
    public static final String TOP_FORECASTS = "_topForecasts";

    private RestHandlerUtils() {}

    /**
     * Checks to see if the request came from OpenSearch-Dashboards, if so we want to return the UI Metadata from the document.
     * If the request came from the client then we exclude the UI Metadata from the search result.
     * We also take into account the given `_source` field and respect the correct fields to be returned.
     * @param request rest request
     * @param searchSourceBuilder an instance of the searchSourceBuilder to fetch _source field
     * @return instance of {@link org.opensearch.search.fetch.subphase.FetchSourceContext}
     */
    public static FetchSourceContext getSourceContext(RestRequest request, SearchSourceBuilder searchSourceBuilder) {
        String userAgent = coalesceToEmpty(request.header("User-Agent"));

        // If there is a _source given in request than we either add UI_Metadata to exclude or not depending on if request
        // is from OpenSearch-Dashboards, if no _source field then we either exclude UI_metadata or return nothing at all.
        if (searchSourceBuilder.fetchSource() != null) {
            if (userAgent.contains(OPENSEARCH_DASHBOARDS_USER_AGENT)) {
                return new FetchSourceContext(
                    true,
                    searchSourceBuilder.fetchSource().includes(),
                    searchSourceBuilder.fetchSource().excludes()
                );
            } else {
                String[] newArray = (String[]) ArrayUtils.addAll(searchSourceBuilder.fetchSource().excludes(), UI_METADATA_EXCLUDE);
                return new FetchSourceContext(true, searchSourceBuilder.fetchSource().includes(), newArray);
            }
        } else if (!userAgent.contains(OPENSEARCH_DASHBOARDS_USER_AGENT)) {
            return new FetchSourceContext(true, Strings.EMPTY_ARRAY, UI_METADATA_EXCLUDE);
        } else {
            return null;
        }
    }

    public static XContentParser createXContentParser(RestChannel channel, BytesReference bytesReference) throws IOException {
        return XContentHelper
            .createParser(channel.request().getXContentRegistry(), LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON);
    }

    public static XContentParser createXContentParserFromRegistry(NamedXContentRegistry xContentRegistry, BytesReference bytesReference)
        throws IOException {
        return XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON);
    }

    /**
     * Check if there is configuration/syntax error in feature definition of config
     * @param config config to check
     * @param maxFeatures max allowed feature number
     * @return error message if error exists; otherwise, null is returned
     */
    public static String checkFeaturesSyntax(Config config, int maxFeatures) {
        List<Feature> features = config.getFeatureAttributes();
        if (features != null) {
            if (features.size() > maxFeatures) {
                return "Can't create more than " + maxFeatures + " feature(s)";
            }
            return validateFeaturesConfig(config.getFeatureAttributes());
        }
        return null;
    }

    private static String validateFeaturesConfig(List<Feature> features) {
        final Set<String> duplicateFeatureNames = new HashSet<>();
        final Set<String> featureNames = new HashSet<>();
        final Set<String> duplicateFeatureAggNames = new HashSet<>();
        final Set<String> featureAggNames = new HashSet<>();

        features.forEach(feature -> {
            if (!featureNames.add(feature.getName())) {
                duplicateFeatureNames.add(feature.getName());
            }
            if (!featureAggNames.add(feature.getAggregation().getName())) {
                duplicateFeatureAggNames.add(feature.getAggregation().getName());
            }
        });

        StringBuilder errorMsgBuilder = new StringBuilder();
        if (duplicateFeatureNames.size() > 0) {
            errorMsgBuilder.append("There are duplicate feature names: ");
            errorMsgBuilder.append(String.join(", ", duplicateFeatureNames));
        }
        if (errorMsgBuilder.length() != 0 && duplicateFeatureAggNames.size() > 0) {
            errorMsgBuilder.append(". ");
        }
        if (duplicateFeatureAggNames.size() > 0) {
            errorMsgBuilder.append(CommonMessages.DUPLICATE_FEATURE_AGGREGATION_NAMES);
            errorMsgBuilder.append(String.join(", ", duplicateFeatureAggNames));
        }
        return errorMsgBuilder.toString();
    }

    public static boolean isExceptionCausedByInvalidQuery(Exception ex) {
        if (!(ex instanceof SearchPhaseExecutionException)) {
            return false;
        }
        SearchPhaseExecutionException exception = (SearchPhaseExecutionException) ex;
        // If any shards return bad request and failure cause is IllegalArgumentException, we
        // consider the feature query is invalid and will not count the error in failure stats.
        for (ShardSearchFailure failure : exception.shardFailures()) {
            if (RestStatus.BAD_REQUEST != failure.status() || !(failure.getCause() instanceof IllegalArgumentException)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Wrap action listener to avoid return verbose error message and wrong 500 error to user.
     * Suggestion for exception handling in timeseries analysis (e.g., AD and Forecast):
     * 1. If the error is caused by wrong input, throw IllegalArgumentException exception.
     * 2. For other errors, please use TimeSeriesException or its subclass, or use
     *    OpenSearchStatusException.
     *
     * TODO: tune this function for wrapped exception, return root exception error message
     *
     * @param actionListener action listener
     * @param generalErrorMessage general error message
     * @param <T> action listener response type
     * @return wrapped action listener
     */
    public static <T> ActionListener wrapRestActionListener(ActionListener<T> actionListener, String generalErrorMessage) {
        return ActionListener.<T>wrap(r -> actionListener.onResponse(r), e -> {
            logger.error("Wrap exception before sending back to user", e);
            Throwable cause = Throwables.getRootCause(e);
            if (isProperExceptionToReturn(e)) {
                actionListener.onFailure(e);
            } else if (isProperExceptionToReturn(cause)) {
                actionListener.onFailure((Exception) cause);
            } else {
                RestStatus status = isBadRequest(e) ? BAD_REQUEST : INTERNAL_SERVER_ERROR;
                String errorMessage = generalErrorMessage;
                if (isBadRequest(e) || e instanceof TimeSeriesException) {
                    errorMessage = e.getMessage();
                } else if (cause != null && (isBadRequest(cause) || cause instanceof TimeSeriesException)) {
                    errorMessage = cause.getMessage();
                }
                actionListener.onFailure(new OpenSearchStatusException(errorMessage, status));
            }
        });
    }

    public static boolean isBadRequest(Throwable e) {
        if (e == null) {
            return false;
        }
        return e instanceof IllegalArgumentException || e instanceof ResourceNotFoundException;
    }

    public static boolean isProperExceptionToReturn(Throwable e) {
        if (e == null) {
            return false;
        }
        return e instanceof OpenSearchStatusException || e instanceof IndexNotFoundException || e instanceof InvalidIndexNameException;
    }

    private static String coalesceToEmpty(@Nullable String s) {
        return s == null ? "" : s;
    }

    public static Entity buildEntity(RestRequest request, String detectorId) throws IOException {
        if (org.opensearch.core.common.Strings.isEmpty(detectorId)) {
            throw new IllegalStateException(CommonMessages.CONFIG_ID_MISSING_MSG);
        }

        String entityName = request.param(CommonName.CATEGORICAL_FIELD);
        String entityValue = request.param(CommonName.ENTITY_KEY);

        if (entityName != null && entityValue != null) {
            // single-stream profile request:
            // GET
            // _plugins/_anomaly_detection/detectors/<detectorId>/_profile/init_progress?category_field=<field-name>&entity=<value>
            return Entity.createSingleAttributeEntity(entityName, entityValue);
        } else if (request.hasContent()) {
            /*
             * HCAD profile request: GET
             * _plugins/_anomaly_detection/detectors/<detectorId>/_profile/init_progress {
             * "entity": [{ "name": "clientip", "value": "13.24.0.0" }] }
             */
            Optional<Entity> entity = Entity.fromJsonObject(request.contentParser());
            if (entity.isPresent()) {
                return entity.get();
            }
        }
        // not a valid profile request with correct entity information
        return null;
    }
}
