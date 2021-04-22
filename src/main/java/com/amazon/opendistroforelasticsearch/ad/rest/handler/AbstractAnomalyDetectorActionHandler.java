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

package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.parseAggregators;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.isExceptionCausedByInvalidQuery;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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

import com.amazon.opendistroforelasticsearch.ad.common.exception.ADValidationException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.feature.SearchFeatureDao;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorValidationIssueType;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.MergeableList;
import com.amazon.opendistroforelasticsearch.ad.model.ValidationAspect;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.ad.transport.IndexAnomalyDetectorResponse;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

public abstract class AbstractAnomalyDetectorActionHandler<T extends ActionResponse> {
    public static final String EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG = "Can't create multi-entity anomaly detectors more than ";
    public static final String EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG = "Can't create single-entity anomaly detectors more than ";
    public static final String NO_DOCS_IN_USER_INDEX_MSG = "Can't create anomaly detector as no document found in indices: ";
    public static final String ONLY_ONE_CATEGORICAL_FIELD_ERR_MSG = "We can have only one categorical field.";
    public static final String CATEGORICAL_FIELD_TYPE_ERR_MSG = "A categorical field must be of type keyword or ip.";
    public static final String NOT_FOUND_ERR_MSG = "Cannot found the categorical field %s";
    // Modifying message for FEATURE below may break the parseADValidationException method of ValidateAnomalyDetectorTransportAction
    public static final String FEATURE_INVALID_MSG_PREFIX = "Feature has invalid query";
    public static final String FEATURE_WITH_EMPTY_DATA_MSG = FEATURE_INVALID_MSG_PREFIX + " returning empty aggregated data: ";
    public static final String FEATURE_WITH_INVALID_QUERY_MSG = FEATURE_INVALID_MSG_PREFIX + " causing runtime exception: ";
    public static final String UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG = "Unknown exception caught while searching query for feature: ";

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
    protected final User user;
    protected final ADTaskManager adTaskManager;
    protected final SearchFeatureDao searchFeatureDao;
    protected final boolean isDryun;

    /**
     * Constructor function.
     *
     * @param clusterService          ClusterService
     * @param client                  ES node client that executes actions on the local node
     * @param transportService        ES transport service
     * @param listener                 ES channel used to construct bytes / builder based outputs, and send responses
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
     * @param isDryun                 Whether handler is dryrun or not
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
        User user,
        ADTaskManager adTaskManager,
        SearchFeatureDao searchFeatureDao,
        boolean isDryun
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
        this.isDryun = isDryun;
    }

    /**
     * Start function to process create/update/validate anomaly detector request.
     * Check if anomaly detector index exist first, if not, will create first.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#initAnomalyDetectorIndexIfAbsent(ActionListener)}
     */
    public void start() throws IOException {
        if (!anomalyDetectionIndices.doesAnomalyDetectorIndexExist() && !this.isDryun) {
            logger.info("AnomalyDetector Indices do not exist");
            anomalyDetectionIndices
                .initAnomalyDetectorIndex(
                    ActionListener
                        .wrap(response -> onCreateMappingsResponse(response, false), exception -> listener.onFailure(exception))
                );
        } else {
            logger.info("AnomalyDetector Indices do exist, calling prepareAnomalyDetectorIndexing");
            prepareAnomalyDetectorIndexing(this.isDryun);
        }
    }

    /**
     * Prepare for indexing a new anomaly detector.
     * @param indexingDryrun if this is dryrun for indexing; when validation, it is true; when create/update, it is false
     */
    protected void prepareAnomalyDetectorIndexing(boolean indexingDryrun) {
        if (method == RestRequest.Method.PUT) {
            handler
                .getDetectorJob(
                    clusterService,
                    client,
                    detectorId,
                    listener,
                    () -> updateAnomalyDetector(detectorId, indexingDryrun),
                    xContentRegistry
                );
        } else {
            createAnomalyDetector(indexingDryrun);
        }
    }

    protected void updateAnomalyDetector(String detectorId, boolean indexingDryrun) {
        GetRequest request = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
        client
            .get(
                request,
                ActionListener
                    .wrap(response -> onGetAnomalyDetectorResponse(response, indexingDryrun), exception -> listener.onFailure(exception))
            );
    }

    protected void onGetAnomalyDetectorResponse(GetResponse response, boolean indexingDryrun) {
        if (!response.isExists()) {
            listener.onFailure(new OpenSearchStatusException("AnomalyDetector is not found with id: " + detectorId, RestStatus.NOT_FOUND));
            return;
        }
        try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            AnomalyDetector existingDetector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());
            // We have separate flows for realtime and historical detector currently. User
            // can't change detector from realtime to historical, vice versa.
            if (existingDetector.isRealTimeDetector() != anomalyDetector.isRealTimeDetector()) {
                listener
                    .onFailure(
                        new OpenSearchStatusException(
                            "Can't change detector type between realtime and historical detector",
                            RestStatus.BAD_REQUEST
                        )
                    );
                return;
            }

            if (existingDetector.isRealTimeDetector()) {
                validateExistingDetector(existingDetector, indexingDryrun);
            } else {
                adTaskManager.getLatestADTask(detectorId, (adTask) -> {
                    if (adTask.isPresent() && !adTaskManager.isADTaskEnded(adTask.get())) {
                        // can't update detector if there is AD task running
                        listener.onFailure(new OpenSearchStatusException("Detector is running", RestStatus.INTERNAL_SERVER_ERROR));
                    } else {
                        // TODO: change to validateDetector method when we support HC historical detector
                        searchAdInputIndices(detectorId, indexingDryrun);
                    }
                }, transportService, listener);
            }
        } catch (IOException e) {
            String message = "Failed to parse anomaly detector " + detectorId;
            logger.error(message, e);
            listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
        }

    }

    protected void validateExistingDetector(AnomalyDetector existingDetector, boolean indexingDryrun) {
        if (!hasCategoryField(existingDetector) && hasCategoryField(this.anomalyDetector)) {
            validateAgainstExistingMultiEntityAnomalyDetector(detectorId, indexingDryrun);
        } else {
            validateCategoricalField(detectorId, indexingDryrun);
        }
    }

    protected boolean hasCategoryField(AnomalyDetector detector) {
        return detector.getCategoryField() != null && !detector.getCategoryField().isEmpty();
    }

    protected void validateAgainstExistingMultiEntityAnomalyDetector(String detectorId, boolean indexingDryrun) {
        if (anomalyDetectionIndices.doesAnomalyDetectorIndexExist()) {
            QueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(AnomalyDetector.CATEGORY_FIELD));

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeout);

            SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTORS_INDEX).source(searchSourceBuilder);

            client
                .search(
                    searchRequest,
                    ActionListener
                        .wrap(
                            response -> onSearchMultiEntityAdResponse(response, detectorId, indexingDryrun),
                            exception -> listener.onFailure(exception)
                        )
                );
        } else {
            validateCategoricalField(detectorId, indexingDryrun);
        }

    }

    protected void createAnomalyDetector(boolean indexingDryrun) {
        try {
            List<String> categoricalFields = anomalyDetector.getCategoryField();
            if (categoricalFields != null && categoricalFields.size() > 0) {
                validateAgainstExistingMultiEntityAnomalyDetector(null, indexingDryrun);
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
                                                    response -> onSearchSingleEntityAdResponse(response, indexingDryrun),
                                                    exception -> listener.onFailure(exception)
                                            )
                            );
                } else {
                    searchAdInputIndices(null, indexingDryrun);
                }

            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected void onSearchSingleEntityAdResponse(SearchResponse response, boolean indexingDryrun) throws IOException {
        if (response.getHits().getTotalHits().value >= maxSingleEntityAnomalyDetectors && !indexingDryrun) {
            String errorMsg = EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG + maxSingleEntityAnomalyDetectors;
            logger.error(errorMsg);
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            searchAdInputIndices(null, indexingDryrun);
        }
    }

    protected void onSearchMultiEntityAdResponse(SearchResponse response, String detectorId, boolean indexingDryrun) throws IOException {
        if (response.getHits().getTotalHits().value >= maxMultiEntityAnomalyDetectors && !indexingDryrun) {
            String errorMsg = EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG + maxMultiEntityAnomalyDetectors;
            logger.error(errorMsg);
            listener.onFailure(new IllegalArgumentException(errorMsg));
        } else {
            validateCategoricalField(detectorId, indexingDryrun);
        }
    }

    @SuppressWarnings("unchecked")
    protected void validateCategoricalField(String detectorId, boolean indexingDryrun) {
        List<String> categoryField = anomalyDetector.getCategoryField();

        if (categoryField == null) {
            searchAdInputIndices(detectorId, indexingDryrun);
            return;
        }

        // we only support one categorical field
        // If there is more than 1 field or none, AnomalyDetector's constructor
        // throws IllegalArgumentException before reaching this line
        if (categoryField.size() != 1) {
            listener
                .onFailure(
                    new ADValidationException(
                        ONLY_ONE_CATEGORICAL_FIELD_ERR_MSG,
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
            Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>>> mappingsByIndex = getMappingsResponse
                .mappings();

            for (Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappingsByType : mappingsByIndex.values()) {
                for (Map<String, GetFieldMappingsResponse.FieldMappingMetadata> mappingsByField : mappingsByType.values()) {
                    for (Map.Entry<String, GetFieldMappingsResponse.FieldMappingMetadata> field2Metadata : mappingsByField.entrySet()) {
                        // example output:
                        // host_nest.host2=FieldMappingMetadata{fullName='host_nest.host2',
                        // source=org.opensearch.common.bytes.BytesArray@8fb4de08}
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
            }

            if (foundField == false) {
                listener
                    .onFailure(
                        new ADValidationException(
                            String.format(Locale.ROOT, NOT_FOUND_ERR_MSG, categoryField0),
                            DetectorValidationIssueType.CATEGORY,
                            ValidationAspect.DETECTOR
                        )
                    );
                return;
            }

            searchAdInputIndices(detectorId, indexingDryrun);
        }, error -> {
            String message = String.format(Locale.ROOT, "Fail to get the index mapping of %s", anomalyDetector.getIndices());
            logger.error(message, error);
            listener.onFailure(new IllegalArgumentException(message));
        });

        client.execute(GetFieldMappingsAction.INSTANCE, getMappingsRequest, mappingsListener);
    }

    protected void searchAdInputIndices(String detectorId, boolean indexingDryrun) {
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
                        searchResponse -> onSearchAdInputIndicesResponse(searchResponse, detectorId, indexingDryrun),
                        exception -> listener.onFailure(exception)
                    )
            );
    }

    protected void onSearchAdInputIndicesResponse(SearchResponse response, String detectorId, boolean indexingDryrun) throws IOException {
        if (response.getHits().getTotalHits().value == 0) {
            String errorMsg = NO_DOCS_IN_USER_INDEX_MSG + Arrays.toString(anomalyDetector.getIndices().toArray(new String[0]));
            logger.error(errorMsg);
            listener.onFailure(new ADValidationException(errorMsg, DetectorValidationIssueType.INDICES, ValidationAspect.DETECTOR));
        } else {
            validateAnomalyDetectorFeatures(detectorId, indexingDryrun);
        }
    }

    protected void checkADNameExists(String detectorId, boolean indexingDryrun) throws IOException {
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
                            searchResponse -> onSearchADNameResponse(searchResponse, detectorId, anomalyDetector.getName(), indexingDryrun),
                            exception -> listener.onFailure(exception)
                        )
                );
        } else {
            tryIndexingAnomalyDetector(indexingDryrun);
        }

    }

    protected void onSearchADNameResponse(SearchResponse response, String detectorId, String name, boolean indexingDryrun)
        throws IOException {
        if (response.getHits().getTotalHits().value > 0) {
            String errorMsg = String
                .format(
                    Locale.ROOT,
                    "Cannot create anomaly detector with name [%s] as it's already used by detector %s",
                    name,
                    Arrays.stream(response.getHits().getHits()).map(hit -> hit.getId()).collect(Collectors.toList())
                );
            logger.warn(errorMsg);
            listener.onFailure(new ADValidationException(errorMsg, DetectorValidationIssueType.NAME, ValidationAspect.DETECTOR));
        } else {
            tryIndexingAnomalyDetector(indexingDryrun);
        }
    }

    protected void tryIndexingAnomalyDetector(boolean indexingDryrun) throws IOException {
        if (!indexingDryrun) {
            indexAnomalyDetector(detectorId);
        } else {
            logger.info("Skipping indexing detector. No issue found so far.");
            listener.onResponse(null);
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
            anomalyDetector.getDetectorType(),
            anomalyDetector.getDetectionDateRange()
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

    protected void onCreateMappingsResponse(CreateIndexResponse response, boolean indexingDryrun) throws IOException {
        if (response.isAcknowledged()) {
            logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
            prepareAnomalyDetectorIndexing(indexingDryrun);
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
     * @param indexingDryrun if false, then will eventually index detector; true, skip indexing detector
     * @throws IOException when fail to parse feature aggregation
     */
    // TODO: move this method to util class so that it can be re-usable for more use cases
    // https://github.com/opensearch-project/anomaly-detection/issues/39
    protected void validateAnomalyDetectorFeatures(String detectorId, boolean indexingDryrun) throws IOException {
        if (anomalyDetector != null
            && (anomalyDetector.getFeatureAttributes() == null || anomalyDetector.getFeatureAttributes().isEmpty())) {
            checkADNameExists(detectorId, indexingDryrun);
            return;
        }
        // checking configuration/syntax error of detector features
        String error = RestHandlerUtils.checkAnomalyDetectorFeaturesSyntax(anomalyDetector, maxAnomalyFeatures);
        if (StringUtils.isNotBlank(error)) {
            listener.onFailure(new ADValidationException(error, DetectorValidationIssueType.FEATURE_ATTRIBUTES, ValidationAspect.DETECTOR));
            return;
        }

        // checking runtime error of detector features
        ActionListener<MergeableList<Optional<double[]>>> validateFeatureQueriesListener = ActionListener
            .wrap(response -> { checkADNameExists(detectorId, indexingDryrun); }, exception -> {
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
                String.format(Locale.ROOT, "Validation failed for feature(s) of detector %s", anomalyDetector.getName()),
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
                    String errorMessage = FEATURE_WITH_EMPTY_DATA_MSG + feature.getName();
                    logger.error(errorMessage);
                    multiFeatureQueriesResponseListener.onFailure(new OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST));
                }
            }, e -> {
                String errorMessage;
                if (isExceptionCausedByInvalidQuery(e)) {
                    errorMessage = FEATURE_WITH_INVALID_QUERY_MSG + feature.getName();
                } else {
                    errorMessage = UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG + feature.getName();
                }

                logger.error(errorMessage, e);
                multiFeatureQueriesResponseListener.onFailure(new OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST, e));
            }));
        }
    }
}
