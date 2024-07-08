/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.constant.CommonMessages.CATEGORICAL_FIELD_TYPE_ERR_MSG;
import static org.opensearch.timeseries.util.ParseUtils.parseAggregators;
import static org.opensearch.timeseries.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.opensearch.timeseries.util.RestHandlerUtils.isExceptionCausedByInvalidQuery;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.MergeableList;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

import com.google.common.collect.Sets;

/**
 * AbstractTimeSeriesActionHandler serves as the foundational base for handling various time series actions
 * such as creating, updating, and validating configurations related to time series analysis. This class encapsulates
 * common logic and utilities for managing time series indices, processing requests, and interacting with the OpenSearch cluster
 * to execute time series tasks.
 *
 * Responsibilities include:
 * - Validating and processing REST requests for time series configurations, ensuring they comply with predefined
 *   constraints and formats.
 * - Managing interactions with the underlying time series indices, including index creation, document indexing,
 *   and configuration retrieval.
 * - Serving as a base for specialized action handlers that implement specific logic for different types of time series tasks
 *   (e.g., anomaly detection, forecasting).
 * - Handling security and permission validations for time series operations, leveraging OpenSearch's security features
 *   to ensure operations are performed by authorized users.
 *
 * The class is designed to be extended by specific action handlers that implement the abstract methods provided,
 * allowing for flexible and modular enhancement of the time series capabilities within OpenSearch.
 *
 * Usage of this class requires extending it to implement the abstract methods, which include but are not limited to
 * configuration validation, indexing logic, and model validation. Implementers will benefit from the common utilities
 * and framework provided by this class, focusing on the unique logic pertinent to their specific time series task.
 */
public abstract class AbstractTimeSeriesActionHandler<T extends ActionResponse, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>>
    implements
        Processor<T> {

    protected final Logger logger = LogManager.getLogger(AbstractTimeSeriesActionHandler.class);

    public static final String NAME_REGEX = "[a-zA-Z0-9._-]+";
    public static final Integer MAX_NAME_SIZE = 64;
    public static final String CATEGORY_NOT_FOUND_ERR_MSG = "Can't find the categorical field %s";

    public static String INVALID_NAME_SIZE = "Name should be shortened. The maximum limit is "
        + AbstractTimeSeriesActionHandler.MAX_NAME_SIZE
        + " characters.";

    public static final Set<String> ALL_VALIDATION_ASPECTS_STRS = Arrays
        .asList(ValidationAspect.values())
        .stream()
        .map(aspect -> aspect.getName())
        .collect(Collectors.toSet());

    protected final Config config;
    protected final IndexManagement<IndexType> timeSeriesIndices;
    protected final boolean isDryRun;
    protected final Client client;
    protected final String id;
    protected final SecurityClientUtil clientUtil;
    protected final User user;
    protected final RestRequest.Method method;
    protected final ConfigUpdateConfirmer<IndexType, IndexManagementType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType> handler;
    protected final ClusterService clusterService;
    protected final NamedXContentRegistry xContentRegistry;
    protected final TimeValue requestTimeout;
    protected final WriteRequest.RefreshPolicy refreshPolicy;
    protected final Long seqNo;
    protected final Long primaryTerm;
    protected final String validationType;
    protected final SearchFeatureDao searchFeatureDao;
    protected final Integer maxFeatures;
    protected final Integer maxCategoricalFields;
    protected final AnalysisType context;
    protected final List<TaskTypeEnum> batchTasks;
    protected final boolean canUpdateEverything;

    protected final Integer maxSingleStreamConfigs;
    protected final Integer maxHCConfigs;
    protected final Clock clock;
    protected final Settings settings;
    protected final ValidationAspect configValidationAspect;

    public AbstractTimeSeriesActionHandler(
        Config config,
        IndexManagement<IndexType> timeSeriesIndices,
        boolean isDryRun,
        Client client,
        String id,
        SecurityClientUtil clientUtil,
        User user,
        RestRequest.Method method,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        TransportService transportService,
        TimeValue requestTimeout,
        WriteRequest.RefreshPolicy refreshPolicy,
        Long seqNo,
        Long primaryTerm,
        String validationType,
        SearchFeatureDao searchFeatureDao,
        Integer maxFeatures,
        Integer maxCategoricalFields,
        AnalysisType context,
        TaskManagerType taskManager,
        List<TaskTypeEnum> batchTasks,
        boolean canUpdateCategoryField,
        Integer maxSingleStreamConfigs,
        Integer maxHCConfigs,
        Clock clock,
        Settings settings,
        ValidationAspect configValidationAspect
    ) {
        this.config = config;
        this.timeSeriesIndices = timeSeriesIndices;
        this.isDryRun = isDryRun;
        this.client = client;
        this.id = id == null ? "" : id;
        this.clientUtil = clientUtil;
        this.user = user;
        this.method = method;
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
        this.requestTimeout = requestTimeout;
        this.refreshPolicy = refreshPolicy;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.validationType = validationType;
        this.searchFeatureDao = searchFeatureDao;
        this.maxFeatures = maxFeatures;
        this.maxCategoricalFields = maxCategoricalFields;
        this.context = context;
        this.batchTasks = batchTasks;
        this.canUpdateEverything = canUpdateCategoryField;
        this.maxSingleStreamConfigs = maxSingleStreamConfigs;
        this.maxHCConfigs = maxHCConfigs;
        this.clock = clock;
        this.settings = settings;
        this.handler = new ConfigUpdateConfirmer<>(taskManager, transportService);
        this.configValidationAspect = configValidationAspect;
    }

    /**
     * Start function to process create/update/validate config request.
     *
     * If validation type is detector/forecaster then all validation in this class involves validation
     * checks against the configurations.
     * Any issues raised here would block user from creating the config (e.g., anomaly detector).
     * If validation Aspect is of type model then further non-blocker validation will be executed
     * after the blocker validation is executed. Any issues that are raised for model validation
     * are simply warnings for the user in terms of how configuration could be changed to lead to
     * a higher likelihood of model training completing successfully.
     *
     * For custom index validation, if config is not using custom result index, check if config
     * index exist first, if not, will create first. Otherwise, check if custom
     * result index exists or not. If exists, will check if index mapping matches
     * config result index mapping and if user has correct permission to write index.
     * If doesn't exist, will create custom result index with result index
     * mapping.
     */
    @Override
    public void start(ActionListener<T> listener) {
        String resultIndexOrAlias = config.getCustomResultIndexOrAlias();
        // use default detector result index which is system index
        if (resultIndexOrAlias == null) {
            createOrUpdateConfig(listener);
            return;
        }

        if (this.isDryRun) {
            if (timeSeriesIndices.doesIndexExist(resultIndexOrAlias) || timeSeriesIndices.doesAliasExist(resultIndexOrAlias)) {
                timeSeriesIndices
                    .validateResultIndexAndExecute(
                        resultIndexOrAlias,
                        () -> createOrUpdateConfig(listener),
                        false,
                        ActionListener.wrap(r -> createOrUpdateConfig(listener), ex -> {
                            logger.error(ex);
                            listener.onFailure(createValidationException(ex.getMessage(), ValidationIssueType.RESULT_INDEX));
                            return;
                        })
                    );
                return;
            } else {
                createOrUpdateConfig(listener);
                return;
            }
        }
        // use custom result index if not validating and resultIndex not null
        timeSeriesIndices.initCustomResultIndexAndExecute(resultIndexOrAlias, () -> createOrUpdateConfig(listener), listener);
    }

    // if isDryRun is true then this method is being executed through Validation API meaning actual
    // index won't be created, only validation checks will be executed throughout the class
    private void createOrUpdateConfig(ActionListener<T> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            if (!timeSeriesIndices.doesConfigIndexExist() && !this.isDryRun) {
                logger.info("Config Indices do not exist");
                timeSeriesIndices
                    .initConfigIndex(
                        ActionListener
                            .wrap(
                                response -> onCreateMappingsResponse(response, false, listener),
                                exception -> listener.onFailure(exception)
                            )
                    );
            } else {
                logger.info("DryRun variable " + this.isDryRun);
                validateName(this.isDryRun, listener);
            }
        } catch (Exception e) {
            logger.error("Failed to create or update forecaster " + id, e);
            listener.onFailure(e);
        }
    }

    protected void validateName(boolean indexingDryRun, ActionListener<T> listener) {
        if (!config.getName().matches(NAME_REGEX)) {
            listener.onFailure(createValidationException(CommonMessages.INVALID_NAME, ValidationIssueType.NAME));
            return;

        }
        if (config.getName().length() > MAX_NAME_SIZE) {
            listener.onFailure(createValidationException(AbstractTimeSeriesActionHandler.INVALID_NAME_SIZE, ValidationIssueType.NAME));
            return;
        }
        validateTimeField(indexingDryRun, listener);
    }

    protected void validateTimeField(boolean indexingDryRun, ActionListener<T> listener) {
        String givenTimeField = config.getTimeField();
        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(config.getIndices().toArray(new String[0])).fields(givenTimeField);
        getMappingsRequest.indicesOptions(IndicesOptions.strictExpand());

        // comments explaining fieldMappingResponse parsing can be found inside validateCategoricalField(String, boolean)
        ActionListener<GetFieldMappingsResponse> mappingsListener = ActionListener.wrap(getMappingsResponse -> {
            boolean foundField = false;
            Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappingsByIndex = getMappingsResponse.mappings();

            for (Map<String, GetFieldMappingsResponse.FieldMappingMetadata> mappingsByField : mappingsByIndex.values()) {
                for (Map.Entry<String, GetFieldMappingsResponse.FieldMappingMetadata> field2Metadata : mappingsByField.entrySet()) {

                    GetFieldMappingsResponse.FieldMappingMetadata fieldMetadata = field2Metadata.getValue();
                    if (fieldMetadata != null) {
                        // sourceAsMap returns sth like {host2={type=keyword}} with host2 being a nested field
                        Map<String, Object> fieldMap = fieldMetadata.sourceAsMap();
                        if (fieldMap != null) {
                            for (Object type : fieldMap.values()) {
                                if (type instanceof Map) {
                                    foundField = true;
                                    Map<String, Object> metadataMap = (Map<String, Object>) type;
                                    String typeName = (String) metadataMap.get(CommonName.TYPE);
                                    if (!typeName.equals(CommonName.DATE_TYPE) && !typeName.equals(CommonName.DATE_NANOS_TYPE)) {
                                        listener
                                            .onFailure(
                                                new ValidationException(
                                                    String.format(Locale.ROOT, CommonMessages.INVALID_TIMESTAMP, givenTimeField),
                                                    ValidationIssueType.TIMEFIELD_FIELD,
                                                    configValidationAspect
                                                )
                                            );
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (!foundField) {
                listener
                    .onFailure(
                        new ValidationException(
                            String.format(Locale.ROOT, CommonMessages.NON_EXISTENT_TIMESTAMP, givenTimeField),
                            ValidationIssueType.TIMEFIELD_FIELD,
                            configValidationAspect
                        )
                    );
                return;
            }
            prepareConfigIndexing(indexingDryRun, listener);
        }, error -> {
            String message = String.format(Locale.ROOT, "Fail to get the index mapping of %s", config.getIndices());
            logger.error(message, error);
            listener.onFailure(new IllegalArgumentException(message));
        });
        clientUtil
            .executeWithInjectedSecurity(GetFieldMappingsAction.INSTANCE, getMappingsRequest, user, client, context, mappingsListener);
    }

    /**
     * Prepare for indexing a new config.
     * @param indexingDryRun if this is dryrun for indexing; when validation, it is true; when create/update, it is false
     */
    protected void prepareConfigIndexing(boolean indexingDryRun, ActionListener<T> listener) {
        if (method == RestRequest.Method.PUT) {
            handler
                .confirmJobRunning(
                    clusterService,
                    client,
                    id,
                    listener,
                    () -> updateConfig(id, indexingDryRun, listener),
                    xContentRegistry
                );
        } else {
            createConfig(indexingDryRun, listener);
        }
    }

    protected void updateConfig(String id, boolean indexingDryRun, ActionListener<T> listener) {
        GetRequest request = new GetRequest(CommonName.CONFIG_INDEX, id);
        client
            .get(
                request,
                ActionListener
                    .wrap(
                        response -> onGetConfigResponse(response, indexingDryRun, id, listener),
                        exception -> listener.onFailure(exception)
                    )
            );
    }

    private void onGetConfigResponse(GetResponse response, boolean indexingDryRun, String id, ActionListener<T> listener) {
        if (!response.isExists()) {
            listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + id, RestStatus.NOT_FOUND));
            return;
        }
        try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            Config existingConfig = parse(parser, response);
            // If category field changed, frontend may not be able to render AD result for different config types correctly.
            // For example, if an anomaly detector changed from HC to single entity detector, AD result page may show multiple anomaly
            // result points on the same time point if there are multiple entities have anomaly results.
            // If single-category HC changed category field from IP to error type, the AD result page may show both IP and error type
            // in top N entities list. That's confusing.
            // So we decide to block updating detector category field.
            // for forecasting, we will not show results after forecaster configuration change (excluding changes like description)
            // thus it is safe to allow updating everything. In the future, we might change AD to allow such behavior.
            if (!canUpdateEverything) {
                if (!ParseUtils.listEqualsWithoutConsideringOrder(existingConfig.getCategoryFields(), config.getCategoryFields())) {
                    listener.onFailure(new OpenSearchStatusException(CommonMessages.CAN_NOT_CHANGE_CATEGORY_FIELD, RestStatus.BAD_REQUEST));
                    return;
                }
                if (!Objects.equals(existingConfig.getCustomResultIndexOrAlias(), config.getCustomResultIndexOrAlias())) {
                    listener
                        .onFailure(
                            new OpenSearchStatusException(CommonMessages.CAN_NOT_CHANGE_CUSTOM_RESULT_INDEX, RestStatus.BAD_REQUEST)
                        );
                    return;
                }
            }

            ActionListener<Void> confirmBatchRunningListener = ActionListener
                .wrap(
                    r -> searchConfigInputIndices(id, indexingDryRun, listener),
                    // can't update config if there is task running
                    listener::onFailure
                );

            handler.confirmBatchRunning(id, batchTasks, confirmBatchRunningListener);
        } catch (IOException e) {
            String message = "Failed to parse anomaly detector " + id;
            logger.error(message, e);
            listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
        }

    }

    protected void validateAgainstExistingHCConfig(String detectorId, boolean indexingDryRun, ActionListener<T> listener) {
        if (timeSeriesIndices.doesConfigIndexExist()) {
            QueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(Config.CATEGORY_FIELD));

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);

            SearchRequest searchRequest = new SearchRequest(CommonName.CONFIG_INDEX).source(searchSourceBuilder);
            client
                .search(
                    searchRequest,
                    ActionListener
                        .wrap(
                            response -> onSearchHCConfigResponse(response, detectorId, indexingDryRun, listener),
                            exception -> listener.onFailure(exception)
                        )
                );
        } else {
            validateCategoricalField(detectorId, indexingDryRun, listener);
        }

    }

    protected void createConfig(boolean indexingDryRun, ActionListener<T> listener) {
        try {
            List<String> categoricalFields = config.getCategoryFields();
            if (categoricalFields != null && categoricalFields.size() > 0) {
                validateAgainstExistingHCConfig(null, indexingDryRun, listener);
            } else {
                if (timeSeriesIndices.doesConfigIndexExist()) {
                    QueryBuilder query = QueryBuilders.matchAllQuery();
                    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);

                    SearchRequest searchRequest = new SearchRequest(CommonName.CONFIG_INDEX).source(searchSourceBuilder);

                    client
                        .search(
                            searchRequest,
                            ActionListener
                                .wrap(
                                    response -> onSearchSingleStreamConfigResponse(response, indexingDryRun, listener),
                                    exception -> listener.onFailure(exception)
                                )
                        );
                } else {
                    searchConfigInputIndices(null, indexingDryRun, listener);
                }

            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected void onSearchSingleStreamConfigResponse(SearchResponse response, boolean indexingDryRun, ActionListener<T> listener)
        throws IOException {
        if (response.getHits().getTotalHits().value >= getMaxSingleStreamConfigs()) {
            String errorMsgSingleEntity = getExceedMaxSingleStreamConfigsErrorMsg(getMaxSingleStreamConfigs());
            logger.error(errorMsgSingleEntity);
            if (indexingDryRun) {
                listener.onFailure(createValidationException(errorMsgSingleEntity, ValidationIssueType.GENERAL_SETTINGS));
                return;
            }
            listener.onFailure(new IllegalArgumentException(errorMsgSingleEntity));
        } else {
            searchConfigInputIndices(null, indexingDryRun, listener);
        }
    }

    protected void onSearchHCConfigResponse(SearchResponse response, String detectorId, boolean indexingDryRun, ActionListener<T> listener)
        throws IOException {
        if (response.getHits().getTotalHits().value >= getMaxHCConfigs()) {
            String errorMsg = getExceedMaxHCConfigsErrorMsg(getMaxHCConfigs());
            logger.error(errorMsg);
            if (indexingDryRun) {
                listener.onFailure(createValidationException(errorMsg, ValidationIssueType.GENERAL_SETTINGS));
                return;
            }
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            validateCategoricalField(detectorId, indexingDryRun, listener);
        }
    }

    @SuppressWarnings("unchecked")
    protected void validateCategoricalField(String detectorId, boolean indexingDryRun, ActionListener<T> listener) {
        List<String> categoryField = config.getCategoryFields();

        if (categoryField == null) {
            searchConfigInputIndices(detectorId, indexingDryRun, listener);
            return;
        }

        // we only support a certain number of categorical field
        // If there is more fields than required, Config's constructor
        // throws validation exception before reaching this line
        int maxCategoryFields = maxCategoricalFields;
        if (categoryField.size() > maxCategoryFields) {
            listener
                .onFailure(
                    createValidationException(CommonMessages.getTooManyCategoricalFieldErr(maxCategoryFields), ValidationIssueType.CATEGORY)
                );
            return;
        }

        String categoryField0 = categoryField.get(0);

        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(config.getIndices().toArray(new String[0])).fields(categoryField.toArray(new String[0]));
        getMappingsRequest.indicesOptions(IndicesOptions.strictExpand());

        ActionListener<GetFieldMappingsResponse> mappingsListener = ActionListener.wrap(getMappingsResponse -> {
            // example getMappingsResponse:
            // GetFieldMappingsResponse{mappings={server-metrics={_doc={service=FieldMappingMetadata{fullName='service',
            // source=org.opensearch.core.common.bytes.BytesArray@7ba87dbd}}}}}
            // for nested field, it would be
            // GetFieldMappingsResponse{mappings={server-metrics={_doc={host_nest.host2=FieldMappingMetadata{fullName='host_nest.host2',
            // source=org.opensearch.core.common.bytes.BytesArray@8fb4de08}}}}}
            boolean foundField = false;

            // Review why the change from FieldMappingMetadata to GetFieldMappingsResponse.FieldMappingMetadata
            Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappingsByIndex = getMappingsResponse.mappings();

            for (Map<String, GetFieldMappingsResponse.FieldMappingMetadata> mappingsByField : mappingsByIndex.values()) {
                for (Map.Entry<String, GetFieldMappingsResponse.FieldMappingMetadata> field2Metadata : mappingsByField.entrySet()) {
                    // example output:
                    // host_nest.host2=FieldMappingMetadata{fullName='host_nest.host2',
                    // source=org.opensearch.core.common.bytes.BytesArray@8fb4de08}

                    // Review why the change from FieldMappingMetadata to GetFieldMappingsResponse.FieldMappingMetadata

                    GetFieldMappingsResponse.FieldMappingMetadata fieldMetadata = field2Metadata.getValue();

                    if (fieldMetadata != null) {
                        // sourceAsMap returns sth like {host2={type=keyword}} with host2 being a nested field
                        Map<String, Object> fieldMap = fieldMetadata.sourceAsMap();
                        if (fieldMap != null) {
                            for (Object type : fieldMap.values()) {
                                if (type != null && type instanceof Map) {
                                    foundField = true;
                                    Map<String, Object> metadataMap = (Map<String, Object>) type;
                                    String typeName = (String) metadataMap.get(CommonName.TYPE);
                                    if (!typeName.equals(CommonName.KEYWORD_TYPE) && !typeName.equals(CommonName.IP_TYPE)) {
                                        listener
                                            .onFailure(
                                                createValidationException(CATEGORICAL_FIELD_TYPE_ERR_MSG, ValidationIssueType.CATEGORY)
                                            );
                                        return;
                                    }
                                }
                            }
                        }

                    }
                }
            }

            if (foundField == false) {
                listener
                    .onFailure(
                        createValidationException(
                            String.format(Locale.ROOT, CATEGORY_NOT_FOUND_ERR_MSG, categoryField0),
                            ValidationIssueType.CATEGORY
                        )
                    );
                return;
            }

            searchConfigInputIndices(detectorId, indexingDryRun, listener);
        }, error -> {
            String message = String.format(Locale.ROOT, "Fail to get the index mapping of %s", config.getIndices());
            logger.error(message, error);
            listener.onFailure(new IllegalArgumentException(message));
        });

        clientUtil
            .executeWithInjectedSecurity(GetFieldMappingsAction.INSTANCE, getMappingsRequest, user, client, context, mappingsListener);
    }

    protected void searchConfigInputIndices(String detectorId, boolean indexingDryRun, ActionListener<T> listener) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(0)
            .timeout(requestTimeout);

        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);

        ActionListener<SearchResponse> searchResponseListener = ActionListener
            .wrap(
                searchResponse -> onSearchConfigInputIndicesResponse(searchResponse, detectorId, indexingDryRun, listener),
                exception -> listener.onFailure(exception)
            );

        clientUtil.asyncRequestWithInjectedSecurity(searchRequest, client::search, user, client, context, searchResponseListener);
    }

    protected void onSearchConfigInputIndicesResponse(
        SearchResponse response,
        String detectorId,
        boolean indexingDryRun,
        ActionListener<T> listener
    ) throws IOException {
        if (response.getHits().getTotalHits().value == 0) {
            String errorMsg = getNoDocsInUserIndexErrorMsg(Arrays.toString(config.getIndices().toArray(new String[0])));
            logger.error(errorMsg);
            if (indexingDryRun) {
                listener.onFailure(createValidationException(errorMsg, ValidationIssueType.INDICES));
                return;
            }
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            validateConfigFeatures(detectorId, indexingDryRun, listener);
        }
    }

    protected void checkConfigNameExists(String configId, boolean indexingDryRun, ActionListener<T> listener) throws IOException {
        if (timeSeriesIndices.doesConfigIndexExist()) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            // src/main/resources/mappings/config.json#L14
            boolQueryBuilder.must(QueryBuilders.termQuery("name.keyword", config.getName()));
            if (StringUtils.isNotBlank(configId)) {
                boolQueryBuilder.mustNot(QueryBuilders.termQuery(RestHandlerUtils._ID, configId));
            }
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).timeout(requestTimeout);
            SearchRequest searchRequest = new SearchRequest(CommonName.CONFIG_INDEX).source(searchSourceBuilder);
            client
                .search(
                    searchRequest,
                    ActionListener
                        .wrap(
                            searchResponse -> onSearchConfigNameResponse(searchResponse, config.getName(), indexingDryRun, listener),
                            exception -> listener.onFailure(exception)
                        )
                );
        } else {
            tryIndexingConfig(indexingDryRun, listener);
        }

    }

    protected void onSearchConfigNameResponse(SearchResponse response, String name, boolean indexingDryRun, ActionListener<T> listener)
        throws IOException {
        if (response.getHits().getTotalHits().value > 0) {
            String errorMsg = getDuplicateConfigErrorMsg(name);
            logger.warn(errorMsg);
            if (indexingDryRun) {
                listener.onFailure(createValidationException(errorMsg, ValidationIssueType.NAME));
            } else {
                listener.onFailure(new OpenSearchStatusException(errorMsg, RestStatus.CONFLICT));
            }
        } else {
            tryIndexingConfig(indexingDryRun, listener);
        }
    }

    protected void tryIndexingConfig(boolean indexingDryRun, ActionListener<T> listener) throws IOException {
        if (!indexingDryRun) {
            indexConfig(id, listener);
        } else {
            finishConfigValidationOrContinueToModelValidation(listener);
        }
    }

    protected Set<ValidationAspect> getValidationTypes(String validationType) {
        if (StringUtils.isBlank(validationType)) {
            return getDefaultValidationType();
        } else {
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(validationType.split(",")));
            return ValidationAspect
                .getNames(Sets.intersection(AbstractTimeSeriesActionHandler.ALL_VALIDATION_ASPECTS_STRS, typesInRequest));
        }
    }

    protected void finishConfigValidationOrContinueToModelValidation(ActionListener<T> listener) {
        logger.info("Skipping indexing detector. No blocking issue found so far.");
        if (!getValidationTypes(validationType).contains(ValidationAspect.MODEL)) {
            listener.onResponse(null);
        } else {
            validateModel(listener);
        }
    }

    @SuppressWarnings("unchecked")
    protected void indexConfig(String id, ActionListener<T> listener) throws IOException {
        Config copiedConfig = copyConfig(user, config);
        IndexRequest indexRequest = new IndexRequest(CommonName.CONFIG_INDEX)
            .setRefreshPolicy(refreshPolicy)
            .source(copiedConfig.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE))
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm)
            .timeout(requestTimeout);
        if (StringUtils.isNotBlank(id)) {
            indexRequest.id(id);
        }

        client.index(indexRequest, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                String errorMsg = checkShardsFailure(indexResponse);
                if (errorMsg != null) {
                    listener.onFailure(new OpenSearchStatusException(errorMsg, indexResponse.status()));
                    return;
                }
                listener.onResponse(createIndexConfigResponse(indexResponse, copiedConfig));
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to update config", e);
                if (e.getMessage() != null && e.getMessage().contains("version conflict")) {
                    listener.onFailure(new IllegalArgumentException("There was a problem updating the config:[" + id + "]"));
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    protected void onCreateMappingsResponse(CreateIndexResponse response, boolean indexingDryRun, ActionListener<T> listener) {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", CommonName.CONFIG_INDEX);
            prepareConfigIndexing(indexingDryRun, listener);
        } else {
            logger.warn("Created {} with mappings call not acknowledged.", CommonName.CONFIG_INDEX);
            listener
                .onFailure(
                    new OpenSearchStatusException(
                        "Created " + CommonName.CONFIG_INDEX + "with mappings call not acknowledged.",
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
        }
    }

    protected String checkShardsFailure(IndexResponse response) {
        StringBuilder failureReasons = new StringBuilder();
        if (response.getShardInfo().getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : response.getShardInfo().getFailures()) {
                failureReasons.append(failure);
            }
            return failureReasons.toString();
        }
        return null;
    }

    /**
     * Validate config/syntax, and runtime error of config features
     * @param id config id
     * @param indexingDryRun if false, then will eventually index detector; true, skip indexing detector
     * @throws IOException when fail to parse feature aggregation
     */
    // TODO: move this method to util class so that it can be re-usable for more use cases
    // https://github.com/opensearch-project/anomaly-detection/issues/39
    protected void validateConfigFeatures(String id, boolean indexingDryRun, ActionListener<T> listener) throws IOException {
        if (config != null && (config.getFeatureAttributes() == null || config.getFeatureAttributes().isEmpty())) {
            checkConfigNameExists(id, indexingDryRun, listener);
            return;
        }
        // checking configuration/syntax error of detector features
        String error = RestHandlerUtils.checkFeaturesSyntax(config, maxFeatures);
        if (StringUtils.isNotBlank(error)) {
            if (indexingDryRun) {
                listener.onFailure(createValidationException(error, ValidationIssueType.FEATURE_ATTRIBUTES));
                return;
            }
            listener.onFailure(new OpenSearchStatusException(error, RestStatus.BAD_REQUEST));
            return;
        }
        // checking runtime error from feature query
        ActionListener<MergeableList<Optional<double[]>>> validateFeatureQueriesListener = ActionListener.wrap(response -> {
            checkConfigNameExists(id, indexingDryRun, listener);
        }, exception -> { listener.onFailure(createValidationException(exception.getMessage(), ValidationIssueType.FEATURE_ATTRIBUTES)); });
        MultiResponsesDelegateActionListener<MergeableList<Optional<double[]>>> multiFeatureQueriesResponseListener =
            new MultiResponsesDelegateActionListener<MergeableList<Optional<double[]>>>(
                validateFeatureQueriesListener,
                config.getFeatureAttributes().size(),
                getFeatureErrorMsg(config.getName()),
                false
            );

        for (Feature feature : config.getFeatureAttributes()) {
            SearchSourceBuilder ssb = new SearchSourceBuilder().size(1).query(QueryBuilders.matchAllQuery());
            AggregatorFactories.Builder internalAgg = parseAggregators(
                feature.getAggregation().toString(),
                xContentRegistry,
                feature.getId()
            );
            ssb.aggregation(internalAgg.getAggregatorFactories().iterator().next());
            SearchRequest searchRequest = new SearchRequest().indices(config.getIndices().toArray(new String[0])).source(ssb);
            ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(response -> {
                Optional<double[]> aggFeatureResult = searchFeatureDao.parseResponse(response, Arrays.asList(feature.getId()));
                if (aggFeatureResult.isPresent()) {
                    multiFeatureQueriesResponseListener
                        .onResponse(
                            new MergeableList<Optional<double[]>>(new ArrayList<Optional<double[]>>(Arrays.asList(aggFeatureResult)))
                        );
                } else {
                    String errorMessage = CommonMessages.FEATURE_WITH_EMPTY_DATA_MSG + feature.getName();
                    logger.error(errorMessage);
                    multiFeatureQueriesResponseListener.onFailure(new OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST));
                }
            }, e -> {
                String errorMessage;
                if (isExceptionCausedByInvalidQuery(e)) {
                    errorMessage = CommonMessages.FEATURE_WITH_INVALID_QUERY_MSG + feature.getName();
                } else {
                    errorMessage = CommonMessages.UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG + feature.getName();
                }
                logger.error(errorMessage, e);
                multiFeatureQueriesResponseListener.onFailure(new OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST, e));
            });
            clientUtil.asyncRequestWithInjectedSecurity(searchRequest, client::search, user, client, context, searchResponseListener);
        }
    }

    /**
     * Retrieves the maximum number of single stream configurations allowed.
     *
     * @return The maximum number of single stream configurations.
     */
    protected Integer getMaxSingleStreamConfigs() {
        return maxSingleStreamConfigs;
    }

    /**
     * Retrieves the maximum number of high cardinality configurations allowed.
     *
     * @return The maximum number of high cardinality configurations.
     */
    protected Integer getMaxHCConfigs() {
        return maxHCConfigs;
    }

    /**
     * Creates a validation exception with a specific message and issue type.
     *
     * @param msg The detailed message for the exception.
     * @param type The type of validation issue that triggered the exception.
     * @return A new instance of {@link TimeSeriesException} specialized for validation issues.
     */
    protected abstract TimeSeriesException createValidationException(String msg, ValidationIssueType type);

    /**
     * Parses the configuration from the provided content parser and get response.
     *
     * @param parser The parser for reading the configuration data.
     * @param response The get response containing the configuration to parse.
     * @return A new {@code Config} instance parsed from the input.
     * @throws IOException If an I/O error occurs while parsing.
     */
    protected abstract Config parse(XContentParser parser, GetResponse response) throws IOException;

    /**
     * Generates an error message indicating that the maximum number of single stream configurations has been exceeded.
     *
     * @param maxSingleStreamConfigs The maximum number of single stream configurations allowed.
     * @return An error message string.
     */
    protected abstract String getExceedMaxSingleStreamConfigsErrorMsg(int maxSingleStreamConfigs);

    /**
     * Generates an error message indicating that the maximum number of high cardinality configurations has been exceeded.
     *
     * @param maxHCConfigs The maximum number of high cardinality configurations allowed.
     * @return An error message string.
     */
    protected abstract String getExceedMaxHCConfigsErrorMsg(int maxHCConfigs);

    /**
     * Generates an error message indicating that there are no documents in the user-specified index or indices.
     *
     * @param suppliedIndices The indices supplied by the user.
     * @return An error message string.
     */
    protected abstract String getNoDocsInUserIndexErrorMsg(String suppliedIndices);

    /**
     * Generates an error message indicating that a configuration with the specified name already exists.
     *
     * @param name The name of the configuration.
     * @return An error message string.
     */
    protected abstract String getDuplicateConfigErrorMsg(String name);

    /**
     * Generates an error message related to a feature identified by its ID.
     *
     * @param id The ID of the feature that triggered the error.
     * @return An error message string.
     */
    protected abstract String getFeatureErrorMsg(String id);

    /**
     * Creates a copy of the given configuration for the specified user.
     *
     * @param user The user for whom the configuration is being copied.
     * @param config The original configuration to copy.
     * @return A new {@code Config} instance that is a copy of the original for the specified user.
     */
    protected abstract Config copyConfig(User user, Config config);

    /**
     * Creates a response object for an index configuration operation.
     *
     * @param indexResponse The response from the index operation.
     * @param config The configuration that was indexed.
     * @return A new response object containing details about the indexing operation and the configuration.
     */
    protected abstract T createIndexConfigResponse(IndexResponse indexResponse, Config config);

    /**
     * Retrieves the default set of validation aspects to be applied.
     *
     * @return A set of {@link ValidationAspect} indicating the default validation rules.
     */
    protected abstract Set<ValidationAspect> getDefaultValidationType();

    /**
     * Validate model
     * @param listener listener to return response
     */
    protected abstract void validateModel(ActionListener<T> listener);
}
