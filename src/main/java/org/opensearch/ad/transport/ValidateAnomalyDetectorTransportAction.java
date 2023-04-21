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

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.util.ParseUtils.checkFilterByBackendRoles;
import static org.opensearch.ad.util.ParseUtils.getNullUser;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.ad.common.exception.ADValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssue;
import org.opensearch.ad.model.DetectorValidationIssueType;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.rest.handler.ValidateAnomalyDetectorActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

public class ValidateAnomalyDetectorTransportAction extends
    TransportAction<ValidateAnomalyDetectorRequest, ValidateAnomalyDetectorResponse> {
    private static final Logger logger = LogManager.getLogger(ValidateAnomalyDetectorTransportAction.class);

    private final SDKRestClient client;
    private final SDKClusterService clusterService;
    private final SDKNamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final SearchFeatureDao searchFeatureDao;
    private volatile Boolean filterByEnabled;
    private Clock clock;
    private final Settings settings;

    @Inject
    public ValidateAnomalyDetectorTransportAction(
        ExtensionsRunner extensionsRunner,
        SDKRestClient client,
        SDKClusterService clusterService,
        TaskManager taskManager,
        SDKNamedXContentRegistry namedXContentRegistry,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ActionFilters actionFilters,
        SearchFeatureDao searchFeatureDao
    ) {
        super(ValidateAnomalyDetectorAction.NAME, actionFilters, taskManager);
        this.client = client;
        this.clusterService = clusterService;
        this.xContentRegistry = namedXContentRegistry;
        this.settings = extensionsRunner.getEnvironmentSettings();
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.filterByEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
        this.searchFeatureDao = searchFeatureDao;
        this.clock = Clock.systemUTC();
    }

    @Override
    public void doExecute(Task task, ValidateAnomalyDetectorRequest request, ActionListener<ValidateAnomalyDetectorResponse> listener) {
        // Temporary null user for AD extension without security. Will always execute detector.
        UserIdentity user = getNullUser();
        AnomalyDetector anomalyDetector = request.getDetector();
        try {
            resolveUserAndExecute(user, listener, () -> validateExecute(request, user, listener));
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void resolveUserAndExecute(
        UserIdentity requestedUser,
        ActionListener<ValidateAnomalyDetectorResponse> listener,
        AnomalyDetectorFunction function
    ) {
        try {
            // Check if user has backend roles
            // When filter by is enabled, block users validating detectors who do not have backend roles.
            if (filterByEnabled && !checkFilterByBackendRoles(requestedUser, listener)) {
                return;
            }
            // Validate Detector
            function.execute();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void validateExecute(
        ValidateAnomalyDetectorRequest request,
        UserIdentity user,
        ActionListener<ValidateAnomalyDetectorResponse> listener
    ) {
        AnomalyDetector detector = request.getDetector();
        ActionListener<ValidateAnomalyDetectorResponse> validateListener = ActionListener.wrap(response -> {
            logger.debug("Result of validation process " + response);
            // forcing response to be empty
            listener.onResponse(new ValidateAnomalyDetectorResponse((DetectorValidationIssue) null));
        }, exception -> {
            if (exception instanceof ADValidationException) {
                // ADValidationException is converted as validation issues returned as response to user
                DetectorValidationIssue issue = parseADValidationException((ADValidationException) exception);
                listener.onResponse(new ValidateAnomalyDetectorResponse(issue));
                return;
            }
            logger.error(exception);
            listener.onFailure(exception);
        });
        checkIndicesAndExecute(detector.getIndices(), () -> {
            ValidateAnomalyDetectorActionHandler handler = new ValidateAnomalyDetectorActionHandler(
                clusterService,
                client,
                validateListener,
                anomalyDetectionIndices,
                detector,
                request.getRequestTimeout(),
                request.getMaxSingleEntityAnomalyDetectors(),
                request.getMaxMultiEntityAnomalyDetectors(),
                request.getMaxAnomalyFeatures(),
                RestRequest.Method.POST,
                xContentRegistry,
                user,
                searchFeatureDao,
                request.getValidationType(),
                clock
            );
            try {
                handler.start();
            } catch (Exception exception) {
                String errorMessage = String
                    .format(Locale.ROOT, "Unknown exception caught while validating detector %s", request.getDetector());
                logger.error(errorMessage, exception);
                listener.onFailure(exception);
            }
        }, listener);
    }

    protected DetectorValidationIssue parseADValidationException(ADValidationException exception) {
        String originalErrorMessage = exception.getMessage();
        String errorMessage = "";
        Map<String, String> subIssues = null;
        IntervalTimeConfiguration intervalSuggestion = exception.getIntervalSuggestion();
        switch (exception.getType()) {
            case FEATURE_ATTRIBUTES:
                int firstLeftBracketIndex = originalErrorMessage.indexOf("[");
                int lastRightBracketIndex = originalErrorMessage.lastIndexOf("]");
                if (firstLeftBracketIndex != -1) {
                    // if feature issue messages are between square brackets like
                    // [Feature has issue: A, Feature has issue: B]
                    errorMessage = originalErrorMessage.substring(firstLeftBracketIndex + 1, lastRightBracketIndex);
                    subIssues = getFeatureSubIssuesFromErrorMessage(errorMessage);
                } else {
                    // features having issue like over max feature limit, duplicate feature name, etc.
                    errorMessage = originalErrorMessage;
                }
                break;
            case NAME:
            case CATEGORY:
            case DETECTION_INTERVAL:
            case FILTER_QUERY:
            case TIMEFIELD_FIELD:
            case SHINGLE_SIZE_FIELD:
            case WINDOW_DELAY:
            case RESULT_INDEX:
            case GENERAL_SETTINGS:
            case AGGREGATION:
            case TIMEOUT:
            case INDICES:
                errorMessage = originalErrorMessage;
                break;
        }
        return new DetectorValidationIssue(exception.getAspect(), exception.getType(), errorMessage, subIssues, intervalSuggestion);
    }

    // Example of method output:
    // String input:Feature has invalid query returning empty aggregated data: average_total_rev, Feature has invalid query causing runtime
    // exception: average_total_rev-2
    // output: "sub_issues": {
    // "average_total_rev": "Feature has invalid query returning empty aggregated data",
    // "average_total_rev-2": "Feature has invalid query causing runtime exception"
    // }
    private Map<String, String> getFeatureSubIssuesFromErrorMessage(String errorMessage) {
        Map<String, String> result = new HashMap<>();
        String[] subIssueMessagesSuffix = errorMessage.split(", ");
        for (int i = 0; i < subIssueMessagesSuffix.length; i++) {
            result.put(subIssueMessagesSuffix[i].split(": ")[1], subIssueMessagesSuffix[i].split(": ")[0]);
        }
        return result;
    }

    private void checkIndicesAndExecute(
        List<String> indices,
        AnomalyDetectorFunction function,
        ActionListener<ValidateAnomalyDetectorResponse> listener
    ) {
        SearchRequest searchRequest = new SearchRequest()
            .indices(indices.toArray(new String[0]))
            .source(new SearchSourceBuilder().size(1).query(QueryBuilders.matchAllQuery()));
        client.search(searchRequest, ActionListener.wrap(r -> function.execute(), e -> {
            if (e instanceof IndexNotFoundException) {
                // IndexNotFoundException is converted to a ADValidationException that gets
                // parsed to a DetectorValidationIssue that is returned to
                // the user as a response indicating index doesn't exist
                DetectorValidationIssue issue = parseADValidationException(
                    new ADValidationException(
                        CommonErrorMessages.INDEX_NOT_FOUND,
                        DetectorValidationIssueType.INDICES,
                        ValidationAspect.DETECTOR
                    )
                );
                listener.onResponse(new ValidateAnomalyDetectorResponse(issue));
                return;
            }
            logger.error(e);
            listener.onFailure(e);
        }));
    }
}
