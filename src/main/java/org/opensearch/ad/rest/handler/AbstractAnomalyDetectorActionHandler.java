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

package org.opensearch.ad.rest.handler;

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG;
import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.util.ParseUtils.listEqualsWithoutConsideringOrder;
import static org.opensearch.ad.util.ParseUtils.parseAggregators;
import static org.opensearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.opensearch.ad.util.RestHandlerUtils.isExceptionCausedByInvalidQuery;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
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
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionResponse;
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
import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.ad.common.exception.ADValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssueType;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.MergeableList;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.rest.RestValidateAnomalyDetectorAction;
import org.opensearch.ad.settings.NumericSetting;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.ad.transport.ValidateAnomalyDetectorResponse;
import org.opensearch.ad.util.MultiResponsesDelegateActionListener;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.TransportService;

import com.google.common.collect.Sets;

/**
 * Abstract Anomaly detector REST action handler to process POST/PUT request.
 * POST request is for either validating or creating anomaly detector.
 * PUT request is for updating anomaly detector.
 *
 * <p>Create, Update and Validate APIs all share similar validation process, the differences in logic
 * between the three usages of this class are outlined below.</p>
 * <ul>
 * <li><code>Create/Update:</code><p>This class is extended by <code>IndexAnomalyDetectorActionHandler</code> which handles
 *  either create AD or update AD REST Actions. When this class is constructed from these
 *  actions then the <code>isDryRun</code> parameter will be instantiated as <b>false</b>.</p>
 *  <p>This means that if the AD index doesn't exist at the time request is received it will be created.
 *  Furthermore, this handler will actually create or update the AD and also handle a few exceptions as
 *  they are thrown instead of converting some of them to ADValidationExceptions.</p>
 * <li><code>Validate:</code><p>This class is also extended by <code>ValidateAnomalyDetectorActionHandler</code> which handles
 *  the validate AD REST Actions. When this class is constructed from these
 *  actions then the <code>isDryRun</code> parameter will be instantiated as <b>true</b>.</p>
 *  <p>This means that if the AD index doesn't exist at the time request is received it wont be created.
 *  Furthermore, this means that the AD won't actually be created and all exceptions will be wrapped into
 *  DetectorValidationResponses hence the user will be notified which validation checks didn't pass.</p>
 *  <p>After completing all the first round of validation which is identical to the checks that are done for the
 *  create/update APIs, this code will check if the validation type is 'model' and if true it will
 *  instantiate the <code>ModelValidationActionHandler</code> class and run the non-blocker validation logic</p>
 *  </ul>
 */
public abstract class AbstractAnomalyDetectorActionHandler<T extends ActionResponse> {
    public static final String EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG = "Can't create more than %d multi-entity anomaly detectors.";
    public static final String EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG =
        "Can't create more than %d single-entity anomaly detectors.";
    public static final String NO_DOCS_IN_USER_INDEX_MSG = "Can't create anomaly detector as no document is found in the indices: ";
    public static final String ONLY_ONE_CATEGORICAL_FIELD_ERR_MSG = "We can have only one categorical field.";
    public static final String CATEGORICAL_FIELD_TYPE_ERR_MSG = "A categorical field must be of type keyword or ip.";
    public static final String CATEGORY_NOT_FOUND_ERR_MSG = "Can't find the categorical field %s";
    public static final String DUPLICATE_DETECTOR_MSG = "Cannot create anomaly detector with name [%s] as it's already used by detector %s";
    public static final String NAME_REGEX = "[a-zA-Z0-9._-]+";
    public static final Integer MAX_DETECTOR_NAME_SIZE = 64;
    private static final Set<ValidationAspect> DEFAULT_VALIDATION_ASPECTS = Sets.newHashSet(ValidationAspect.DETECTOR);

    protected final AnomalyDetectionIndices anomalyDetectionIndices;
    protected final String detectorId;
    protected final Long seqNo;
    protected final Long primaryTerm;
    protected final WriteRequest.RefreshPolicy refreshPolicy;
    protected final AnomalyDetector anomalyDetector;
    protected final ClusterService clusterService;

    protected final Logger logger = LogManager.getLogger(AbstractAnomalyDetectorActionHandler.class);
    protected final TimeValue requestTimeout;
    protected final Integer maxSingleEntityAnomalyDetectors;
    protected final Integer maxMultiEntityAnomalyDetectors;
    protected final Integer maxAnomalyFeatures;
    protected final AnomalyDetectorActionHandler handler = new AnomalyDetectorActionHandler();
    protected final RestRequest.Method method;
    protected final Client client;
    protected final TransportService transportService;
    protected final NamedXContentRegistry xContentRegistry;
    protected final ActionListener<T> listener;
    protected final UserIdentity user;
    protected final ADTaskManager adTaskManager;
    protected final SearchFeatureDao searchFeatureDao;
    protected final boolean isDryRun;
    protected final Clock clock;
    protected final String validationType;

    /**
     * Constructor function.
     *
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param transportService        ES transport service
     * @param listener                ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param refreshPolicy           refresh policy
     * @param anomalyDetector         anomaly detector instance
     * @param requestTimeout          request time out configuration
     * @param maxSingleEntityAnomalyDetectors     max single-entity anomaly detectors allowed
     * @param maxMultiEntityAnomalyDetectors      max multi-entity detectors allowed
     * @param maxAnomalyFeatures      max features allowed per detector
     * @param method                  Rest Method type
     * @param xContentRegistry        Registry which is used for XContentParser
     * @param user                    User context
     * @param adTaskManager           AD Task manager
     * @param searchFeatureDao        Search feature dao
     * @param isDryRun                Whether handler is dryrun or not
     * @param validationType          Whether validation is for detector or model
     * @param clock                   clock object to know when to timeout
     */
    public AbstractAnomalyDetectorActionHandler(
        ClusterService clusterService,
        Client client,
        TransportService transportService,
        ActionListener<T> listener,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector anomalyDetector,
        TimeValue requestTimeout,
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        UserIdentity user,
        ADTaskManager adTaskManager,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        boolean isDryRun,
        Clock clock
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.transportService = transportService;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.listener = listener;
        this.detectorId = detectorId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.anomalyDetector = anomalyDetector;
        this.requestTimeout = requestTimeout;
        this.maxSingleEntityAnomalyDetectors = maxSingleEntityAnomalyDetectors;
        this.maxMultiEntityAnomalyDetectors = maxMultiEntityAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.method = method;
        this.xContentRegistry = xContentRegistry;
        this.user = user;
        this.adTaskManager = adTaskManager;
        this.searchFeatureDao = searchFeatureDao;
        this.validationType = validationType;
        this.isDryRun = isDryRun;
        this.clock = clock;
    }

    /**
     * Start function to process create/update/validate anomaly detector request.
     * If detector is not using custom result index, check if anomaly detector
     * index exist first, if not, will create first. Otherwise, check if custom
     * result index exists or not. If exists, will check if index mapping matches
     * AD result index mapping and if user has correct permission to write index.
     * If doesn't exist, will create custom result index with AD result index
     * mapping.
     */
    public void start() {
        String resultIndex = anomalyDetector.getResultIndex();
        // use default detector result index which is system index
        if (resultIndex == null) {
            createOrUpdateDetector();
            return;
        }

        if (this.isDryRun) {
            if (anomalyDetectionIndices.doesIndexExist(resultIndex)) {
                anomalyDetectionIndices
                    .validateCustomResultIndexAndExecute(
                        resultIndex,
                        () -> createOrUpdateDetector(),
                        ActionListener.wrap(r -> createOrUpdateDetector(), ex -> {
                            logger.error(ex);
                            listener
                                .onFailure(
                                    new ADValidationException(
                                        ex.getMessage(),
                                        DetectorValidationIssueType.RESULT_INDEX,
                                        ValidationAspect.DETECTOR
                                    )
                                );
                            return;
                        })
                    );
                return;
            } else {
                createOrUpdateDetector();
                return;
            }
        }
        // use custom result index if not validating and resultIndex not null
        anomalyDetectionIndices.initCustomResultIndexAndExecute(resultIndex, () -> createOrUpdateDetector(), listener);
    }

    // if isDryRun is true then this method is being executed through Validation API meaning actual
    // index won't be created, only validation checks will be executed throughout the class
    private void createOrUpdateDetector() {
        try {
            if (!anomalyDetectionIndices.doesAnomalyDetectorIndexExist() && !this.isDryRun) {
                logger.info("AnomalyDetector Indices do not exist");
                anomalyDetectionIndices
                    .initAnomalyDetectorIndex(
                        ActionListener
                            .wrap(response -> onCreateMappingsResponse(response, false), exception -> listener.onFailure(exception))
                    );
            } else {
                logger.info("AnomalyDetector Indices do exist, calling prepareAnomalyDetectorIndexing");
                logger.info("DryRun variable " + this.isDryRun);
                validateDetectorName(this.isDryRun);
            }
        } catch (Exception e) {
            logger.error("Failed to create or update detector " + detectorId, e);
            listener.onFailure(e);
        }
    }

    // These validation checks are executed here and not in AnomalyDetector.parse()
    // in order to not break any past detectors that were made with invalid names
    // because it was never check on the backend in the past
    protected void validateDetectorName(boolean indexingDryRun) {
        if (!anomalyDetector.getName().matches(NAME_REGEX)) {
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.INVALID_DETECTOR_NAME,
                        DetectorValidationIssueType.NAME,
                        ValidationAspect.DETECTOR
                    )
                );
            return;

        }
        if (anomalyDetector.getName().length() > MAX_DETECTOR_NAME_SIZE) {
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.INVALID_DETECTOR_NAME_SIZE,
                        DetectorValidationIssueType.NAME,
                        ValidationAspect.DETECTOR
                    )
                );
            return;
        }
        validateTimeField(indexingDryRun);
    }

    protected void validateTimeField(boolean indexingDryRun) {
        String givenTimeField = anomalyDetector.getTimeField();
        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(anomalyDetector.getIndices().toArray(new String[0])).fields(givenTimeField);
        getMappingsRequest.indicesOptions(IndicesOptions.strictExpand());

        // comments explaining fieldMappingResponse parsing can be found inside following method:
        // AbstractAnomalyDetectorActionHandler.validateCategoricalField(String, boolean)
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
                                    if (!typeName.equals(CommonName.DATE_TYPE)) {
                                        listener
                                            .onFailure(
                                                new ADValidationException(
                                                    String.format(Locale.ROOT, CommonErrorMessages.INVALID_TIMESTAMP, givenTimeField),
                                                    DetectorValidationIssueType.TIMEFIELD_FIELD,
                                                    ValidationAspect.DETECTOR
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
                        new ADValidationException(
                            String.format(Locale.ROOT, CommonErrorMessages.NON_EXISTENT_TIMESTAMP, givenTimeField),
                            DetectorValidationIssueType.TIMEFIELD_FIELD,
                            ValidationAspect.DETECTOR
                        )
                    );
                return;
            }
            prepareAnomalyDetectorIndexing(indexingDryRun);
        }, error -> {
            String message = String.format(Locale.ROOT, "Fail to get the index mapping of %s", anomalyDetector.getIndices());
            logger.error(message, error);
            listener.onFailure(new IllegalArgumentException(message));
        });
        client.execute(GetFieldMappingsAction.INSTANCE, getMappingsRequest, mappingsListener);
    }

    /**
     * Prepare for indexing a new anomaly detector.
     * @param indexingDryRun if this is dryrun for indexing; when validation, it is true; when create/update, it is false
     */
    protected void prepareAnomalyDetectorIndexing(boolean indexingDryRun) {
        if (method == RestRequest.Method.PUT) {
            // handler
            // .getDetectorJob(
            // clusterService,
            // client,
            // detectorId,
            // listener,
            // () -> updateAnomalyDetector(detectorId, indexingDryRun),
            // xContentRegistry
            // );
        } else {
            createAnomalyDetector(indexingDryRun);
        }
    }

    protected void updateAnomalyDetector(String detectorId, boolean indexingDryRun) {
        GetRequest request = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client
            .get(
                request,
                ActionListener
                    .wrap(
                        response -> onGetAnomalyDetectorResponse(response, indexingDryRun, detectorId),
                        exception -> listener.onFailure(exception)
                    )
            );
    }

    private void onGetAnomalyDetectorResponse(GetResponse response, boolean indexingDryRun, String detectorId) {
        if (!response.isExists()) {
            listener.onFailure(new OpenSearchStatusException(FAIL_TO_FIND_DETECTOR_MSG + detectorId, RestStatus.NOT_FOUND));
            return;
        }
        try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            AnomalyDetector existingDetector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());
            // If detector category field changed, frontend may not be able to render AD result for different detector types correctly.
            // For example, if detector changed from HC to single entity detector, AD result page may show multiple anomaly
            // result points on the same time point if there are multiple entities have anomaly results.
            // If single-category HC changed category field from IP to error type, the AD result page may show both IP and error type
            // in top N entities list. That's confusing.
            // So we decide to block updating detector category field.
            if (!listEqualsWithoutConsideringOrder(existingDetector.getCategoryField(), anomalyDetector.getCategoryField())) {
                listener
                    .onFailure(new OpenSearchStatusException(CommonErrorMessages.CAN_NOT_CHANGE_CATEGORY_FIELD, RestStatus.BAD_REQUEST));
                return;
            }
            if (!Objects.equals(existingDetector.getResultIndex(), anomalyDetector.getResultIndex())) {
                listener.onFailure(new OpenSearchStatusException(CommonErrorMessages.CAN_NOT_CHANGE_RESULT_INDEX, RestStatus.BAD_REQUEST));
                return;
            }

            adTaskManager.getAndExecuteOnLatestDetectorLevelTask(detectorId, HISTORICAL_DETECTOR_TASK_TYPES, (adTask) -> {
                if (adTask.isPresent() && !adTask.get().isDone()) {
                    // can't update detector if there is AD task running
                    listener.onFailure(new OpenSearchStatusException("Detector is running", RestStatus.INTERNAL_SERVER_ERROR));
                } else {
                    validateExistingDetector(existingDetector, indexingDryRun);
                }
            }, transportService, true, listener);
        } catch (IOException e) {
            String message = "Failed to parse anomaly detector " + detectorId;
            logger.error(message, e);
            listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
        }

    }

    protected void validateExistingDetector(AnomalyDetector existingDetector, boolean indexingDryRun) {
        if (!hasCategoryField(existingDetector) && hasCategoryField(this.anomalyDetector)) {
            validateAgainstExistingMultiEntityAnomalyDetector(detectorId, indexingDryRun);
        } else {
            validateCategoricalField(detectorId, indexingDryRun);
        }
    }

    protected boolean hasCategoryField(AnomalyDetector detector) {
        return detector.getCategoryField() != null && !detector.getCategoryField().isEmpty();
    }

    protected void validateAgainstExistingMultiEntityAnomalyDetector(String detectorId, boolean indexingDryRun) {
        if (anomalyDetectionIndices.doesAnomalyDetectorIndexExist()) {
            QueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(AnomalyDetector.CATEGORY_FIELD));

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);

            SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);
            client
                .search(
                    searchRequest,
                    ActionListener
                        .wrap(
                            response -> onSearchMultiEntityAdResponse(response, detectorId, indexingDryRun),
                            exception -> listener.onFailure(exception)
                        )
                );
        } else {
            validateCategoricalField(detectorId, indexingDryRun);
        }

    }

    protected void createAnomalyDetector(boolean indexingDryRun) {
        try {
            List<String> categoricalFields = anomalyDetector.getCategoryField();
            if (categoricalFields != null && categoricalFields.size() > 0) {
                validateAgainstExistingMultiEntityAnomalyDetector(null, indexingDryRun);
            } else {
                if (anomalyDetectionIndices.doesAnomalyDetectorIndexExist()) {
                    QueryBuilder query = QueryBuilders.matchAllQuery();
                    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);

                    SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);

                    client
                        .search(
                            searchRequest,
                            ActionListener
                                .wrap(
                                    response -> onSearchSingleEntityAdResponse(response, indexingDryRun),
                                    exception -> listener.onFailure(exception)
                                )
                        );
                } else {
                    searchAdInputIndices(null, indexingDryRun);
                }

            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected void onSearchSingleEntityAdResponse(SearchResponse response, boolean indexingDryRun) throws IOException {
        if (response.getHits().getTotalHits().value >= maxSingleEntityAnomalyDetectors) {
            String errorMsgSingleEntity = String
                .format(Locale.ROOT, EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG, maxSingleEntityAnomalyDetectors);
            logger.error(errorMsgSingleEntity);
            if (indexingDryRun) {
                listener
                    .onFailure(
                        new ADValidationException(
                            errorMsgSingleEntity,
                            DetectorValidationIssueType.GENERAL_SETTINGS,
                            ValidationAspect.DETECTOR
                        )
                    );
                return;
            }
            listener.onFailure(new IllegalArgumentException(errorMsgSingleEntity));
        } else {
            searchAdInputIndices(null, indexingDryRun);
        }
    }

    protected void onSearchMultiEntityAdResponse(SearchResponse response, String detectorId, boolean indexingDryRun) throws IOException {
        if (response.getHits().getTotalHits().value >= maxMultiEntityAnomalyDetectors) {
            String errorMsg = String.format(Locale.ROOT, EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG, maxMultiEntityAnomalyDetectors);
            logger.error(errorMsg);
            if (indexingDryRun) {
                listener
                    .onFailure(
                        new ADValidationException(errorMsg, DetectorValidationIssueType.GENERAL_SETTINGS, ValidationAspect.DETECTOR)
                    );
                return;
            }
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            validateCategoricalField(detectorId, indexingDryRun);
        }
    }

    @SuppressWarnings("unchecked")
    protected void validateCategoricalField(String detectorId, boolean indexingDryRun) {
        List<String> categoryField = anomalyDetector.getCategoryField();

        if (categoryField == null) {
            searchAdInputIndices(detectorId, indexingDryRun);
            return;
        }

        // we only support a certain number of categorical field
        // If there is more fields than required, AnomalyDetector's constructor
        // throws ADValidationException before reaching this line
        int maxCategoryFields = NumericSetting.maxCategoricalFields();
        if (categoryField.size() > maxCategoryFields) {
            listener
                .onFailure(
                    new ADValidationException(
                        CommonErrorMessages.getTooManyCategoricalFieldErr(maxCategoryFields),
                        DetectorValidationIssueType.CATEGORY,
                        ValidationAspect.DETECTOR
                    )
                );
            return;
        }

        String categoryField0 = categoryField.get(0);

        GetFieldMappingsRequest getMappingsRequest = new GetFieldMappingsRequest();
        getMappingsRequest.indices(anomalyDetector.getIndices().toArray(new String[0])).fields(categoryField.toArray(new String[0]));
        getMappingsRequest.indicesOptions(IndicesOptions.strictExpand());

        ActionListener<GetFieldMappingsResponse> mappingsListener = ActionListener.wrap(getMappingsResponse -> {
            // example getMappingsResponse:
            // GetFieldMappingsResponse{mappings={server-metrics={_doc={service=FieldMappingMetadata{fullName='service',
            // source=org.opensearch.common.bytes.BytesArray@7ba87dbd}}}}}
            // for nested field, it would be
            // GetFieldMappingsResponse{mappings={server-metrics={_doc={host_nest.host2=FieldMappingMetadata{fullName='host_nest.host2',
            // source=org.opensearch.common.bytes.BytesArray@8fb4de08}}}}}
            boolean foundField = false;

            // Review why the change from FieldMappingMetadata to GetFieldMappingsResponse.FieldMappingMetadata
            Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappingsByIndex = getMappingsResponse.mappings();

            for (Map<String, GetFieldMappingsResponse.FieldMappingMetadata> mappingsByField : mappingsByIndex.values()) {
                for (Map.Entry<String, GetFieldMappingsResponse.FieldMappingMetadata> field2Metadata : mappingsByField.entrySet()) {
                    // example output:
                    // host_nest.host2=FieldMappingMetadata{fullName='host_nest.host2',
                    // source=org.opensearch.common.bytes.BytesArray@8fb4de08}

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
                                                new ADValidationException(
                                                    CATEGORICAL_FIELD_TYPE_ERR_MSG,
                                                    DetectorValidationIssueType.CATEGORY,
                                                    ValidationAspect.DETECTOR
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

            if (foundField == false) {
                listener
                    .onFailure(
                        new ADValidationException(
                            String.format(Locale.ROOT, CATEGORY_NOT_FOUND_ERR_MSG, categoryField0),
                            DetectorValidationIssueType.CATEGORY,
                            ValidationAspect.DETECTOR
                        )
                    );
                return;
            }

            searchAdInputIndices(detectorId, indexingDryRun);
        }, error -> {
            String message = String.format(Locale.ROOT, "Fail to get the index mapping of %s", anomalyDetector.getIndices());
            logger.error(message, error);
            listener.onFailure(new IllegalArgumentException(message));
        });

        client.execute(GetFieldMappingsAction.INSTANCE, getMappingsRequest, mappingsListener);
    }

    protected void searchAdInputIndices(String detectorId, boolean indexingDryRun) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(0)
            .timeout(requestTimeout);

        SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0])).source(searchSourceBuilder);

        client
            .search(
                searchRequest,
                ActionListener
                    .wrap(
                        searchResponse -> onSearchAdInputIndicesResponse(searchResponse, detectorId, indexingDryRun),
                        exception -> listener.onFailure(exception)
                    )
            );
    }

    protected void onSearchAdInputIndicesResponse(SearchResponse response, String detectorId, boolean indexingDryRun) throws IOException {
        if (response.getHits().getTotalHits().value == 0) {
            String errorMsg = NO_DOCS_IN_USER_INDEX_MSG + Arrays.toString(anomalyDetector.getIndices().toArray(new String[0]));
            logger.error(errorMsg);
            if (indexingDryRun) {
                listener.onFailure(new ADValidationException(errorMsg, DetectorValidationIssueType.INDICES, ValidationAspect.DETECTOR));
                return;
            }
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            validateAnomalyDetectorFeatures(detectorId, indexingDryRun);
        }
    }

    protected void checkADNameExists(String detectorId, boolean indexingDryRun) throws IOException {
        if (anomalyDetectionIndices.doesAnomalyDetectorIndexExist()) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            // src/main/resources/mappings/anomaly-detectors.json#L14
            boolQueryBuilder.must(QueryBuilders.termQuery("name.keyword", anomalyDetector.getName()));
            if (StringUtils.isNotBlank(detectorId)) {
                boolQueryBuilder.mustNot(QueryBuilders.termQuery(RestHandlerUtils._ID, detectorId));
            }
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).timeout(requestTimeout);
            SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);

            client
                .search(
                    searchRequest,
                    ActionListener
                        .wrap(
                            searchResponse -> onSearchADNameResponse(searchResponse, detectorId, anomalyDetector.getName(), indexingDryRun),
                            exception -> listener.onFailure(exception)
                        )
                );
        } else {
            tryIndexingAnomalyDetector(indexingDryRun);
        }

    }

    protected void onSearchADNameResponse(SearchResponse response, String detectorId, String name, boolean indexingDryRun)
        throws IOException {
        if (response.getHits().getTotalHits().value > 0) {
            String errorMsg = String
                .format(
                    Locale.ROOT,
                    DUPLICATE_DETECTOR_MSG,
                    name,
                    Arrays.stream(response.getHits().getHits()).map(hit -> hit.getId()).collect(Collectors.toList())
                );
            logger.warn(errorMsg);
            listener.onFailure(new ADValidationException(errorMsg, DetectorValidationIssueType.NAME, ValidationAspect.DETECTOR));
        } else {
            tryIndexingAnomalyDetector(indexingDryRun);
        }
    }

    protected void tryIndexingAnomalyDetector(boolean indexingDryRun) throws IOException {
        if (!indexingDryRun) {
            indexAnomalyDetector(detectorId);
        } else {
            finishDetectorValidationOrContinueToModelValidation();
        }
    }

    protected Set<ValidationAspect> getValidationTypes(String validationType) {
        if (StringUtils.isBlank(validationType)) {
            return DEFAULT_VALIDATION_ASPECTS;
        } else {
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(validationType.split(",")));
            return ValidationAspect
                .getNames(Sets.intersection(RestValidateAnomalyDetectorAction.ALL_VALIDATION_ASPECTS_STRS, typesInRequest));
        }
    }

    protected void finishDetectorValidationOrContinueToModelValidation() {
        logger.info("Skipping indexing detector. No blocking issue found so far.");
        if (!getValidationTypes(validationType).contains(ValidationAspect.MODEL)) {
            listener.onResponse(null);
        } else {
            ModelValidationActionHandler modelValidationActionHandler = new ModelValidationActionHandler(
                clusterService,
                client,
                (ActionListener<ValidateAnomalyDetectorResponse>) listener,
                anomalyDetector,
                requestTimeout,
                xContentRegistry,
                searchFeatureDao,
                validationType,
                clock
            );
            modelValidationActionHandler.checkIfMultiEntityDetector();
        }
    }

    @SuppressWarnings("unchecked")
    protected void indexAnomalyDetector(String detectorId) throws IOException {
        AnomalyDetector detector = new AnomalyDetector(
            anomalyDetector.getDetectorId(),
            anomalyDetector.getVersion(),
            anomalyDetector.getName(),
            anomalyDetector.getDescription(),
            anomalyDetector.getTimeField(),
            anomalyDetector.getIndices(),
            anomalyDetector.getFeatureAttributes(),
            anomalyDetector.getFilterQuery(),
            anomalyDetector.getDetectionInterval(),
            anomalyDetector.getWindowDelay(),
            anomalyDetector.getShingleSize(),
            anomalyDetector.getUiMetadata(),
            anomalyDetector.getSchemaVersion(),
            Instant.now(),
            anomalyDetector.getCategoryField(),
            user,
            anomalyDetector.getResultIndex()
        );
        IndexRequest indexRequest = new IndexRequest(ANOMALY_DETECTORS_INDEX)
            .setRefreshPolicy(refreshPolicy)
            .source(detector.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE))
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm)
            .timeout(requestTimeout);
        if (StringUtils.isNotBlank(detectorId)) {
            indexRequest.id(detectorId);
        }

        client.index(indexRequest, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                String errorMsg = checkShardsFailure(indexResponse);
                if (errorMsg != null) {
                    listener.onFailure(new OpenSearchStatusException(errorMsg, indexResponse.status()));
                    return;
                }
                listener
                    .onResponse(
                        (T) new IndexAnomalyDetectorResponse(
                            indexResponse.getId(),
                            indexResponse.getVersion(),
                            indexResponse.getSeqNo(),
                            indexResponse.getPrimaryTerm(),
                            detector,
                            RestStatus.CREATED
                        )
                    );
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to update detector", e);
                if (e.getMessage() != null && e.getMessage().contains("version conflict")) {
                    listener
                        .onFailure(
                            new IllegalArgumentException("There was a problem updating the historical detector:[" + detectorId + "]")
                        );
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    protected void onCreateMappingsResponse(CreateIndexResponse response, boolean indexingDryRun) throws IOException {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
            prepareAnomalyDetectorIndexing(indexingDryRun);
        } else {
            logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTORS_INDEX);
            listener
                .onFailure(
                    new OpenSearchStatusException(
                        "Created " + ANOMALY_DETECTORS_INDEX + "with mappings call not acknowledged.",
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
     * Validate config/syntax, and runtime error of detector features
     * @param detectorId detector id
     * @param indexingDryRun if false, then will eventually index detector; true, skip indexing detector
     * @throws IOException when fail to parse feature aggregation
     */
    // TODO: move this method to util class so that it can be re-usable for more use cases
    // https://github.com/opensearch-project/anomaly-detection/issues/39
    protected void validateAnomalyDetectorFeatures(String detectorId, boolean indexingDryRun) throws IOException {
        if (anomalyDetector != null
            && (anomalyDetector.getFeatureAttributes() == null || anomalyDetector.getFeatureAttributes().isEmpty())) {
            checkADNameExists(detectorId, indexingDryRun);
            return;
        }
        // checking configuration/syntax error of detector features
        String error = RestHandlerUtils.checkAnomalyDetectorFeaturesSyntax(anomalyDetector, maxAnomalyFeatures);
        if (StringUtils.isNotBlank(error)) {
            if (indexingDryRun) {
                listener
                    .onFailure(new ADValidationException(error, DetectorValidationIssueType.FEATURE_ATTRIBUTES, ValidationAspect.DETECTOR));
                return;
            }
            listener.onFailure(new OpenSearchStatusException(error, RestStatus.BAD_REQUEST));
            return;
        }
        // checking runtime error from feature query
        ActionListener<MergeableList<Optional<double[]>>> validateFeatureQueriesListener = ActionListener
            .wrap(response -> { checkADNameExists(detectorId, indexingDryRun); }, exception -> {
                listener
                    .onFailure(
                        new ADValidationException(
                            exception.getMessage(),
                            DetectorValidationIssueType.FEATURE_ATTRIBUTES,
                            ValidationAspect.DETECTOR
                        )
                    );
            });
        MultiResponsesDelegateActionListener<MergeableList<Optional<double[]>>> multiFeatureQueriesResponseListener =
            new MultiResponsesDelegateActionListener<MergeableList<Optional<double[]>>>(
                validateFeatureQueriesListener,
                anomalyDetector.getFeatureAttributes().size(),
                String.format(Locale.ROOT, CommonErrorMessages.VALIDATION_FEATURE_FAILURE, anomalyDetector.getName()),
                false
            );

        for (Feature feature : anomalyDetector.getFeatureAttributes()) {
            SearchSourceBuilder ssb = new SearchSourceBuilder().size(1).query(QueryBuilders.matchAllQuery());
            AggregatorFactories.Builder internalAgg = parseAggregators(
                feature.getAggregation().toString(),
                xContentRegistry,
                feature.getId()
            );
            ssb.aggregation(internalAgg.getAggregatorFactories().iterator().next());
            SearchRequest searchRequest = new SearchRequest().indices(anomalyDetector.getIndices().toArray(new String[0])).source(ssb);
            client.search(searchRequest, ActionListener.wrap(response -> {
                Optional<double[]> aggFeatureResult = searchFeatureDao.parseResponse(response, Arrays.asList(feature.getId()));
                if (aggFeatureResult.isPresent()) {
                    multiFeatureQueriesResponseListener
                        .onResponse(
                            new MergeableList<Optional<double[]>>(new ArrayList<Optional<double[]>>(Arrays.asList(aggFeatureResult)))
                        );
                } else {
                    String errorMessage = CommonErrorMessages.FEATURE_WITH_EMPTY_DATA_MSG + feature.getName();
                    logger.error(errorMessage);
                    multiFeatureQueriesResponseListener.onFailure(new OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST));
                }
            }, e -> {
                String errorMessage;
                if (isExceptionCausedByInvalidQuery(e)) {
                    errorMessage = CommonErrorMessages.FEATURE_WITH_INVALID_QUERY_MSG + feature.getName();
                } else {
                    errorMessage = CommonErrorMessages.UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG + feature.getName();
                }
                logger.error(errorMessage, e);
                multiFeatureQueriesResponseListener.onFailure(new OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST, e));
            }));
        }
    }
}
