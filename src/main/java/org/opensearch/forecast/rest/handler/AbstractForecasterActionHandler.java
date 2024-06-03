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

package org.opensearch.forecast.rest.handler;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Locale;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.IndexForecasterResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.rest.handler.AbstractTimeSeriesActionHandler;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

import com.google.common.collect.Sets;

/**
 * AbstractForecasterActionHandler extends the AbstractTimeSeriesActionHandler to provide a specialized
 * base for handling forecasting-related actions within OpenSearch. This abstract class encapsulates common
 * logic and utilities specifically tailored for managing forecasting tasks, including the validation, creation,
 * and updating of forecaster configurations.
 *
 * Key functionalities include:
 * - Processing REST requests related to forecasting tasks, ensuring they meet the required standards and formats.
 * - Interacting with ForecastIndex and ForecastIndexManagement for forecast-specific index operations.
 * - Validating forecasting configurations against various constraints, such as maximum allowed single-stream
 *   and high cardinality (HC) forecasters, ensuring the configurations adhere to defined limits.
 * - Managing user permissions and security for forecasting operations, leveraging the SecurityClientUtil.
 * - Extending support for forecasting-specific fields and settings, such as forecast horizon, imputation options,
 *   and emphasis on recent data.
 *
 * Usage:
 * This class is designed to be extended by concrete handlers that implement forecasting-specific logic for actions
 * such as creating a new forecaster, updating existing configurations, or validating forecasting models. It provides
 * a structured framework that includes essential services like client communication, security utilities, and task management,
 * allowing implementers to focus on the unique aspects of their forecasting tasks.
 *
 * Extending classes are required to implement abstract methods defined in both this class and its parent, providing
 * functionality for parsing forecasting configurations, handling validation exceptions, and constructing response
 * objects for REST calls.
 *
 * Example Extension:
 * A concrete implementation might include a IndexForecasterActionHandler that leverages this class to handle the
 * creation of new forecaster configurations, including validation against predefined limits and index management.
 */
public abstract class AbstractForecasterActionHandler<T extends ActionResponse> extends
    AbstractTimeSeriesActionHandler<T, ForecastIndex, ForecastIndexManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager> {
    protected final Logger logger = LogManager.getLogger(AbstractForecasterActionHandler.class);

    /**
     *  Message template indicating the limit on the number of high cardinality (HC) forecasters has been reached.
     */
    public static final String EXCEEDED_MAX_HC_FORECASTERS_PREFIX_MSG = "Can't create more than %d HC forecasters.";

    /**
     *  Message template indicating the limit on the number of single-stream forecasters has been reached.
     */
    public static final String EXCEEDED_MAX_SINGLE_STREAM_FORECASTERS_PREFIX_MSG = "Can't create more than %d single-stream forecasters.";

    /**
     *  Message indicating failure to create forecasters due to lack of documents in the user-specified indices.
     */
    public static final String NO_DOCS_IN_USER_INDEX_MSG = "Can't create forecasters as no document is found in the indices: ";

    /**
     *  Message template for the error that occurs when attempting to create a forecaster with a name that is already in use.
     */
    public static final String DUPLICATE_FORECASTER_MSG =
        "Cannot create forecasters with name [%s] as it's already used by another forecaster";

    /**
     *  Message template indicating that validation of forecaster features has failed, mentioning the forecaster's name.
     */
    public static final String VALIDATION_FEATURE_FAILURE = "Validation failed for feature(s) of forecaster %s";

    /**
     * Constructor function.
     *
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param clientUtil              Forecast security client
     * @param transportService        ES transport service
     * @param forecastIndices         forecast index manager
     * @param forecasterId            forecaster identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param refreshPolicy           refresh policy
     * @param forecaster              forecaster instance
     * @param requestTimeout          request time out configuration
     * @param maxSingleStreamForecasters     max single-stream forecasters allowed
     * @param maxHCForecasters        max HC forecasters allowed
     * @param maxFeatures             max features allowed per forecaster
     * @param maxCategoricalFields    max categorical fields allowed
     * @param method                  Rest Method type
     * @param xContentRegistry        Registry which is used for XContentParser
     * @param user                    User context
     * @param forecastTaskManager     Forecast task manager
     * @param searchFeatureDao        Search utility
     * @param validationType          validation type in validate API. Can be null (no validation).
     * @param isDryRun                Whether handler is dryrun or not
     * @param clock                   clock object to know when to timeout
     * @param settings                Node settings
     */
    public AbstractForecasterActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        TransportService transportService,
        ForecastIndexManagement forecastIndices,
        String forecasterId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        Config forecaster,
        TimeValue requestTimeout,
        Integer maxSingleStreamForecasters,
        Integer maxHCForecasters,
        Integer maxFeatures,
        Integer maxCategoricalFields,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        User user,
        ForecastTaskManager forecastTaskManager,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        boolean isDryRun,
        Clock clock,
        Settings settings
    ) {
        super(
            forecaster,
            forecastIndices,
            isDryRun,
            client,
            forecasterId,
            clientUtil,
            user,
            method,
            clusterService,
            xContentRegistry,
            transportService,
            requestTimeout,
            refreshPolicy,
            seqNo,
            primaryTerm,
            validationType,
            searchFeatureDao,
            maxFeatures,
            maxCategoricalFields,
            AnalysisType.FORECAST,
            forecastTaskManager,
            ForecastTaskType.RUN_ONCE_TASK_TYPES,
            true,
            maxSingleStreamForecasters,
            maxHCForecasters,
            clock,
            settings,
            ValidationAspect.FORECASTER
        );
    }

    @Override
    protected TimeSeriesException createValidationException(String msg, ValidationIssueType type) {
        return new ValidationException(msg, type, ValidationAspect.FORECASTER);
    }

    @Override
    protected Forecaster parse(XContentParser parser, GetResponse response) throws IOException {
        return Forecaster.parse(parser, response.getId(), response.getVersion());
    }

    @Override
    protected String getExceedMaxSingleStreamConfigsErrorMsg(int maxSingleStreamConfigs) {
        return String.format(Locale.ROOT, EXCEEDED_MAX_SINGLE_STREAM_FORECASTERS_PREFIX_MSG, getMaxSingleStreamConfigs());
    }

    @Override
    protected String getExceedMaxHCConfigsErrorMsg(int maxHCConfigs) {
        return String.format(Locale.ROOT, EXCEEDED_MAX_HC_FORECASTERS_PREFIX_MSG, getMaxHCConfigs());
    }

    @Override
    protected String getNoDocsInUserIndexErrorMsg(String suppliedIndices) {
        return String.format(Locale.ROOT, NO_DOCS_IN_USER_INDEX_MSG, suppliedIndices);
    }

    @Override
    protected String getDuplicateConfigErrorMsg(String name) {
        return String.format(Locale.ROOT, DUPLICATE_FORECASTER_MSG, name);
    }

    @Override
    protected Config copyConfig(User user, Config config) {
        return new Forecaster(
            config.getId(),
            config.getVersion(),
            config.getName(),
            config.getDescription(),
            config.getTimeField(),
            config.getIndices(),
            config.getFeatureAttributes(),
            config.getFilterQuery(),
            config.getInterval(),
            config.getWindowDelay(),
            config.getShingleSize(),
            config.getUiMetadata(),
            config.getSchemaVersion(),
            Instant.now(),
            config.getCategoryFields(),
            user,
            config.getCustomResultIndex(),
            ((Forecaster) config).getHorizon(),
            config.getImputationOption(),
            config.getRecencyEmphasis(),
            config.getSeasonIntervals(),
            config.getHistoryIntervals(),
            config.getCustomResultIndexMinSize(),
            config.getCustomResultIndexMinAge(),
            config.getCustomResultIndexTTL()
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T createIndexConfigResponse(IndexResponse indexResponse, Config config) {
        return (T) new IndexForecasterResponse(
            indexResponse.getId(),
            indexResponse.getVersion(),
            indexResponse.getSeqNo(),
            indexResponse.getPrimaryTerm(),
            (Forecaster) config,
            RestStatus.CREATED
        );
    }

    @Override
    protected Set<ValidationAspect> getDefaultValidationType() {
        return Sets.newHashSet(ValidationAspect.FORECASTER);
    }

    @Override
    protected String getFeatureErrorMsg(String name) {
        return String.format(Locale.ROOT, VALIDATION_FEATURE_FAILURE, name);
    }

    @Override
    protected void validateModel(ActionListener<T> listener) {
        ForecastModelValidationActionHandler modelValidationActionHandler = new ForecastModelValidationActionHandler(
            clusterService,
            client,
            clientUtil,
            (ActionListener<ValidateConfigResponse>) listener,
            (Forecaster) config,
            requestTimeout,
            xContentRegistry,
            searchFeatureDao,
            validationType,
            clock,
            settings,
            user
        );
        modelValidationActionHandler.start();
    }
}
