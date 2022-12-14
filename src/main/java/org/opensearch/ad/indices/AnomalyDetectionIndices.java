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

package org.opensearch.ad.indices;

import static org.opensearch.ad.constant.CommonErrorMessages.CAN_NOT_FIND_RESULT_INDEX;
import static org.opensearch.ad.constant.CommonName.DUMMY_AD_RESULT_ID;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTORS_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_RESULTS_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_PRIMARY_SHARDS;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.AdminClient;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ExpandWildcard;
import org.opensearch.client.opensearch._types.mapping.SourceField;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.indices.Alias;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.GetAliasRequest;
import org.opensearch.client.opensearch.indices.GetAliasResponse;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.opensearch.indices.PutMappingRequest;
import org.opensearch.client.opensearch.indices.PutMappingResponse;
import org.opensearch.client.opensearch.indices.RolloverRequest;
import org.opensearch.client.opensearch.indices.RolloverResponse;
import org.opensearch.client.opensearch.indices.get_alias.IndexAliases;
import org.opensearch.client.opensearch.indices.rollover.IndexRolloverMapping;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.LocalNodeMasterListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParser.Token;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import jakarta.json.stream.JsonParser;

/**
 * This class provides utility methods for various anomaly detection indices.
 */
public class AnomalyDetectionIndices implements LocalNodeMasterListener {
    private static final Logger logger = LogManager.getLogger(AnomalyDetectionIndices.class);

    // The index name pattern to query all the AD result history indices
    public static final String AD_RESULT_HISTORY_INDEX_PATTERN = "<.opendistro-anomaly-results-history-{now/d}-1>";

    // The index name pattern to query all AD result, history and current AD result
    public static final String ALL_AD_RESULTS_INDEX_PATTERN = ".opendistro-anomaly-results*";

    // minimum shards of the job index
    public static int minJobIndexReplicas = 1;
    // maximum shards of the job index
    public static int maxJobIndexReplicas = 20;

    // package private for testing
    static final String META = "_meta";
    private static final String SCHEMA_VERSION = "schema_version";

    private final OpenSearchClient client;
    private final AdminClient adminClient;
    private final ThreadPool threadPool;

    private volatile TimeValue historyRolloverPeriod;
    private volatile Long historyMaxDocs;
    private volatile TimeValue historyRetentionPeriod;

    private Scheduler.Cancellable scheduledRollover = null;

    private DiscoveryNodeFilterer nodeFilter;
    private int maxPrimaryShards;
    // keep track of whether the mapping version is up-to-date
    private EnumMap<ADIndex, IndexState> indexStates;
    // whether all index have the correct mappings
    private boolean allMappingUpdated;
    // whether all index settings are updated
    private boolean allSettingUpdated;
    // we only want one update at a time
    private final AtomicBoolean updateRunning;
    // don't retry updating endlessly. Can be annoying if there are too many exception logs.
    private final int maxUpdateRunningTimes;
    // the number of times updates run
    private int updateRunningTimes;
    // AD index settings
    private final Settings settings;

    // result index mapping to valida custom index
    private Map<String, Object> AD_RESULT_FIELD_CONFIGS;

    private TransportService transportService;
    private ExtensionsRunner extensionsRunner;

    class IndexState {
        // keep track of whether the mapping version is up-to-date
        private Boolean mappingUpToDate;
        // keep track of whether the setting needs to change
        private Boolean settingUpToDate;
        // record schema version reading from the mapping file
        private Integer schemaVersion;

        IndexState(ADIndex index) {
            this.mappingUpToDate = false;
            settingUpToDate = false;
            this.schemaVersion = parseSchemaVersion(index.getMapping());
        }
    }

    /**
     * Constructor function
     *
     * @param client         OS client supports administrative actions
     * @param threadPool     OS thread pool
     * @param nodeFilter     Used to filter eligible nodes to host AD indices
     * @param maxUpdateRunningTimes max number of retries to update index mapping and setting
     * @param transportService The TransportService defining the connection to OpenSearch
     * @param extensionsRunner Primary runner of this extension
     */
    public AnomalyDetectionIndices(
        OpenSearchClient client,
        ThreadPool threadPool,
        Settings settings,
        DiscoveryNodeFilterer nodeFilter,
        int maxUpdateRunningTimes,
        TransportService transportService,
        ExtensionsRunner extensionsRunner
    )
        throws Exception {
        this.client = client;
        this.adminClient = client.admin();
        this.threadPool = threadPool;

        this.transportService = transportService;
        this.extensionsRunner = extensionsRunner;

        // this.clusterService.addLocalNodeMasterListener(this);

        this.historyRolloverPeriod = AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings);
        this.historyMaxDocs = AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD.get(settings);
        this.historyRetentionPeriod = AD_RESULT_HISTORY_RETENTION_PERIOD.get(settings);
        this.maxPrimaryShards = MAX_PRIMARY_SHARDS.get(settings);

        this.nodeFilter = nodeFilter;

        this.indexStates = new EnumMap<ADIndex, IndexState>(ADIndex.class);

        this.allMappingUpdated = false;
        this.allSettingUpdated = false;
        this.updateRunning = new AtomicBoolean(false);

        Map<Setting<?>, Consumer<?>> settingUpdateConsumers = new HashMap<Setting<?>, Consumer<?>>();
        Consumer<Long> historyMaxDocsConsumer = it -> this.historyMaxDocs = it;
        Consumer<TimeValue> historyRolloverPeriodConsumer = it -> {
            this.historyRolloverPeriod = it;
            rescheduleRollover();
        };
        Consumer<TimeValue> historyRetentionPeriodConsumer = it -> { this.historyRetentionPeriod = it; };
        Consumer<Integer> maxPrimaryShardsConsumer = it -> this.maxPrimaryShards = it;

        settingUpdateConsumers.put(AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD, historyMaxDocsConsumer);
        settingUpdateConsumers.put(AD_RESULT_HISTORY_ROLLOVER_PERIOD, historyRolloverPeriodConsumer);
        settingUpdateConsumers.put(AD_RESULT_HISTORY_RETENTION_PERIOD, historyRetentionPeriodConsumer);
        settingUpdateConsumers.put(MAX_PRIMARY_SHARDS, maxPrimaryShardsConsumer);

        extensionsRunner.sendAddSettingsUpdateConsumerRequest(transportService, settingUpdateConsumers);

        this.settings = Settings.builder().put("index.hidden", true).build();

        this.maxUpdateRunningTimes = maxUpdateRunningTimes;
        this.updateRunningTimes = 0;

        this.AD_RESULT_FIELD_CONFIGS = null;
    }

    /**
     * Send a request to OpenSearch to retrieve the cluster state
     *
     * @return the cluster state of OpenSearch
     */
    private ClusterState getClusterState() {
        return extensionsRunner.sendClusterStateRequest(transportService);
    }

    private void initResultMapping() throws IOException {
        if (AD_RESULT_FIELD_CONFIGS != null) {
            // we have already initiated the field
            return;
        }
        String resultMapping = getAnomalyResultMappings();

        Map<String, Object> asMap = XContentHelper.convertToMap(new BytesArray(resultMapping), false, XContentType.JSON).v2();
        Object properties = asMap.get(CommonName.PROPERTIES);
        if (properties instanceof Map) {
            AD_RESULT_FIELD_CONFIGS = (Map<String, Object>) properties;
        } else {
            logger.error("Fail to read result mapping file.");
        }
    }

    /**
     * Get anomaly detector index mapping json content.
     *
     * @return anomaly detector index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getAnomalyDetectorMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTORS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly result index mapping json content.
     *
     * @return anomaly result index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getAnomalyResultMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_RESULTS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly detector job index mapping json content.
     *
     * @return anomaly detector job index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getAnomalyDetectorJobMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Get anomaly detector state index mapping json content.
     *
     * @return anomaly detector state index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getDetectionStateMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE);
        String detectionStateMappings = Resources.toString(url, Charsets.UTF_8);
        String detectorIndexMappings = AnomalyDetectionIndices.getAnomalyDetectorMappings();
        detectorIndexMappings = detectorIndexMappings
            .substring(detectorIndexMappings.indexOf("\"properties\""), detectorIndexMappings.lastIndexOf("}"));
        return detectionStateMappings.replace("DETECTOR_INDEX_MAPPING_PLACE_HOLDER", detectorIndexMappings);
    }

    /**
     * Get checkpoint index mapping json content.
     *
     * @return checkpoint index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getCheckpointMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(CHECKPOINT_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Anomaly detector index exist or not.
     *
     * @return true if anomaly detector index exists
     */
    public boolean doesAnomalyDetectorIndexExist() {
        return getClusterState().getRoutingTable().hasIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
    }

    /**
     * Anomaly detector job index exist or not.
     *
     * @return true if anomaly detector job index exists
     */
    // public boolean doesAnomalyDetectorJobIndexExist() {
    // return clusterService.state().getRoutingTable().hasIndex(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX);
    // }

    /**
     * anomaly result index exist or not.
     *
     * @return true if anomaly result index exists
     */
    public boolean doesDefaultAnomalyResultIndexExist() {
        return getClusterState().metadata().hasAlias(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    public boolean doesIndexExist(String indexName) {
        return getClusterState().metadata().hasIndex(indexName);
    }

    public <T> void initCustomResultIndexAndExecute(String resultIndex, AnomalyDetectorFunction function, ActionListener<T> listener) {
        try {
            if (!doesIndexExist(resultIndex)) {
                initCustomAnomalyResultIndexDirectly(resultIndex, ActionListener.wrap(response -> {
                    if (response.isAcknowledged()) {
                        logger.info("Successfully created anomaly detector result index {}", resultIndex);
                        validateCustomResultIndexAndExecute(resultIndex, function, listener);
                    } else {
                        String error = "Creating anomaly detector result index with mappings call not acknowledged: " + resultIndex;
                        logger.error(error);
                        listener.onFailure(new EndRunException(error, true));
                    }
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                        // It is possible the index has been created while we sending the create request
                        validateCustomResultIndexAndExecute(resultIndex, function, listener);
                    } else {
                        logger.error("Failed to create anomaly detector result index " + resultIndex, exception);
                        listener.onFailure(exception);
                    }
                }));
            } else {
                validateCustomResultIndexAndExecute(resultIndex, function, listener);
            }
        } catch (Exception e) {
            logger.error("Failed to create custom result index " + resultIndex, e);
            listener.onFailure(e);
        }
    }

    public <T> void validateCustomResultIndexAndExecute(String resultIndex, AnomalyDetectorFunction function, ActionListener<T> listener) {
        try {
            if (!isValidResultIndexMapping(resultIndex)) {
                logger.warn("Can't create detector with custom result index {} as its mapping is invalid", resultIndex);
                throw new IllegalArgumentException(CommonErrorMessages.INVALID_RESULT_INDEX_MAPPING + resultIndex);
            }

            AnomalyResult dummyResult = AnomalyResult.getDummyResult();
            IndexRequest<AnomalyResult> indexRequest = new IndexRequest.Builder<AnomalyResult>()
                .id(DUMMY_AD_RESULT_ID)
                .index(resultIndex)
                .document(dummyResult)
                .build();
            // dummyResult.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS)
            // User may have no write permission on custom result index. Talked with security plugin team, seems no easy way to verify
            // if user has write permission. So just tried to write and delete a dummy anomaly result to verify.
            client.index(indexRequest);
            logger.debug("Successfully wrote dummy AD result to result index {}", resultIndex);
            DeleteRequest deleteRequest = new DeleteRequest.Builder().index(resultIndex).build();
            client.delete(deleteRequest);
            logger.debug("Successfully deleted dummy AD result from result index {}", resultIndex);
        } catch (Exception e) {
            logger.error("Failed to create detector with custom result index " + resultIndex, e);
        }
    }

    public <T> void validateCustomIndexForBackendJob(
        String resultIndex,
        String securityLogId,
        String user,
        List<String> roles,
        AnomalyDetectorFunction function,
        ActionListener<T> listener
    ) {
        if (!doesIndexExist(resultIndex)) {
            listener.onFailure(new EndRunException(CAN_NOT_FIND_RESULT_INDEX + resultIndex, true));
            return;
        }
        if (!isValidResultIndexMapping(resultIndex)) {
            listener.onFailure(new EndRunException("Result index mapping is not correct", true));
            return;
        }
        try {
            ActionListener<T> wrappedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> { listener.onFailure(e); });
            validateCustomResultIndexAndExecute(resultIndex, () -> { function.execute(); }, wrappedListener);
        } catch (Exception e) {
            logger.error("Failed to validate custom index for backend job " + securityLogId, e);
            listener.onFailure(e);
        }
    }

    /**
     * Check if custom result index has correct index mapping.
     * @param resultIndex result index
     * @return true if result index mapping is valid
     */
    public boolean isValidResultIndexMapping(String resultIndex) {
        try {
            initResultMapping();
            if (AD_RESULT_FIELD_CONFIGS == null) {
                // failed to populate the field
                return false;
            }
            IndexMetadata indexMetadata = getClusterState().metadata().index(resultIndex);
            Map<String, Object> indexMapping = indexMetadata.mapping().sourceAsMap();
            String propertyName = CommonName.PROPERTIES;
            if (!indexMapping.containsKey(propertyName) || !(indexMapping.get(propertyName) instanceof LinkedHashMap)) {
                return false;
            }
            LinkedHashMap<String, Object> mapping = (LinkedHashMap<String, Object>) indexMapping.get(propertyName);

            boolean correctResultIndexMapping = true;

            for (String fieldName : AD_RESULT_FIELD_CONFIGS.keySet()) {
                Object defaultSchema = AD_RESULT_FIELD_CONFIGS.get(fieldName);
                // the field might be a map or map of map
                // example: map: {type=date, format=strict_date_time||epoch_millis}
                // map of map: {type=nested, properties={likelihood={type=double}, value_list={type=nested, properties={data={type=double},
                // feature_id={type=keyword}}}}}
                // if it is a map of map, Object.equals can compare them regardless of order
                if (!mapping.containsKey(fieldName) || !defaultSchema.equals(mapping.get(fieldName))) {
                    correctResultIndexMapping = false;
                    break;
                }
            }
            return correctResultIndexMapping;
        } catch (Exception e) {
            logger.error("Failed to validate result index mapping for index " + resultIndex, e);
            return false;
        }

    }

    /**
     * Anomaly state index exist or not.
     *
     * @return true if anomaly state index exists
     */
    public boolean doesDetectorStateIndexExist() {
        return getClusterState().getRoutingTable().hasIndex(CommonName.DETECTION_STATE_INDEX);
    }

    /**
     * Checkpoint index exist or not.
     *
     * @return true if checkpoint index exists
     */
    public boolean doesCheckpointIndexExist() {
        return getClusterState().getRoutingTable().hasIndex(CommonName.CHECKPOINT_INDEX_NAME);
    }

    /**
     * Index exists or not
     * @param clusterState Cluster State
     * @param name Index name
     * @return true if the index exists
     */
    public static boolean doesIndexExists(ClusterState clusterState, String name) {
        return clusterState.getRoutingTable().hasIndex(name);
    }

    /**
     * Alias exists or not
     * @param clusterState Cluster State
     * @param alias Alias name
     * @return true if the alias exists
     */
    public static boolean doesAliasExists(ClusterState clusterState, String alias) {
        return clusterState.metadata().hasAlias(alias);
    }

    private void markMappingUpToDate(ADIndex index, CreateIndexResponse createdResponse) {
        try {
            if (createdResponse.acknowledged()) {
                IndexState indexStatetate = indexStates.computeIfAbsent(index, IndexState::new);
                if (Boolean.FALSE.equals(indexStatetate.mappingUpToDate)) {
                    indexStatetate.mappingUpToDate = Boolean.TRUE;
                    logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", index.getIndexName()));
                }
            }
        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }

    /**
     * Create anomaly detector index if not exist.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initAnomalyDetectorIndexIfAbsent() throws IOException {
        if (!doesAnomalyDetectorIndexExist()) {
            initAnomalyDetectorIndex();
        }
    }

    /**
     * Create anomaly detector index directly.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initAnomalyDetectorIndex() throws IOException {
        JsonpMapper mapper = client._transport().jsonpMapper();
        ((JacksonJsonpMapper) mapper).objectMapper().registerModule(new JavaTimeModule());
        JsonParser parser = null;
        try {
            parser = mapper
                .jsonProvider()
                .createParser(new ByteArrayInputStream(getAnomalyDetectorMappings().getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        CreateIndexRequest request = null;
        try {
            request = new CreateIndexRequest.Builder()
                .index(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                .mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper))
                .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        CreateIndexResponse createIndexResponse = client.indices().create(request);

        markMappingUpToDate(ADIndex.CONFIG, createIndexResponse);
    }

    /**
     * Create anomaly result index if not exist.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyResultMappings}
     */
    public void initDefaultAnomalyResultIndexIfAbsent() throws IOException {
        if (!doesDefaultAnomalyResultIndexExist()) {
            initDefaultAnomalyResultIndexDirectly();
        }
    }

    /**
     * choose the number of primary shards for checkpoint, multientity result, and job scheduler based on the number of hot nodes. Max 10.
     * @param request The request to add the setting
     */
    private CreateIndexRequest.Builder choosePrimaryShards(CreateIndexRequest.Builder builder) {
        return choosePrimaryShards(builder, true);
    }

    private CreateIndexRequest.Builder choosePrimaryShards(CreateIndexRequest.Builder builder, boolean hiddenIndex) {
        return builder
            .settings(
                new IndexSettings.Builder()
                    // put 1 primary shards per hot node if possible
                    .numberOfShards(String.valueOf(getNumberOfPrimaryShards()))
                    // 1 replica for better search performance and fail-over
                    .numberOfReplicas("1")
                    .hidden(hiddenIndex)
                    .build()
            );
    }

    private int getNumberOfPrimaryShards() {
        return Math.min(nodeFilter.getNumberOfEligibleDataNodes(), maxPrimaryShards);
    }

    /**
     * Create anomaly result index without checking exist or not.
     *
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyResultMappings}
     */
    public void initDefaultAnomalyResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        initAnomalyResultIndexDirectly(AD_RESULT_HISTORY_INDEX_PATTERN, CommonName.ANOMALY_RESULT_INDEX_ALIAS, true);
    }

    public void initCustomAnomalyResultIndexDirectly(String resultIndex) throws IOException {
        initAnomalyResultIndexDirectly(resultIndex, null, false);
    }

    public void initAnomalyResultIndexDirectly(String resultIndex, String alias, boolean hiddenIndex) throws IOException {
        JsonpMapper mapper = client._transport().jsonpMapper();
        ((JacksonJsonpMapper) mapper).objectMapper().registerModule(new JavaTimeModule());
        JsonParser parser = null;
        try {
            parser = mapper
                .jsonProvider()
                .createParser(new ByteArrayInputStream(getAnomalyDetectorMappings().getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        CreateIndexRequest request = null;
        try {
            CreateIndexRequest.Builder builder = new CreateIndexRequest.Builder()
                .index(resultIndex)
                .mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper));
            if (alias != null) {
                builder = builder.aliases(alias, new Alias.Builder().indexRouting(CommonName.ANOMALY_RESULT_INDEX_ALIAS).build());
            }
            builder = choosePrimaryShards(builder, hiddenIndex);
            request = builder.build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        CreateIndexResponse createIndexResponse = client.indices().create(request);
        if (AD_RESULT_HISTORY_INDEX_PATTERN.equals(resultIndex)) {
            markMappingUpToDate(ADIndex.CONFIG, createIndexResponse);
        }
    }

    /**
     * Create anomaly detector job index.
     *
     * @param actionListener action called after create index
     */
    // @anomaly-detection.create-detector Commented this code until we have support of Job Scheduler for extensibility
    // public void initAnomalyDetectorJobIndex(ActionListener<CreateIndexResponse> actionListener) {
    // try {
    // CreateIndexRequest request = new CreateIndexRequest(".opendistro-anomaly-detector-jobs")
    // .mapping(getAnomalyDetectorJobMappings(), XContentType.JSON);
    // request
    // .settings(
    // Settings
    // .builder()
    // // AD job index is small. 1 primary shard is enough
    // .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
    // // Job scheduler puts both primary and replica shards in the
    // // hash ring. Auto-expand the number of replicas based on the
    // // number of data nodes (up to 20) in the cluster so that each node can
    // // become a coordinating node. This is useful when customers
    // // scale out their cluster so that we can do adaptive scaling
    // // accordingly.
    // // At least 1 replica for fail-over.
    // .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, minJobIndexReplicas + "-" + maxJobIndexReplicas)
    // .put("index.hidden", true)
    // );
    // adminClient.indices().create(request, markMappingUpToDate(ADIndex.JOB, actionListener));
    // } catch (IOException e) {
    // logger.error("Fail to init AD job index", e);
    // actionListener.onFailure(e);
    // }
    // }

    /**
     * Create the state index.
     *
     */
    public void initDetectionStateIndex() {
        JsonpMapper mapper = client._transport().jsonpMapper();
        ((JacksonJsonpMapper) mapper).objectMapper().registerModule(new JavaTimeModule());
        JsonParser parser = null;
        try {
            parser = mapper
                .jsonProvider()
                .createParser(new ByteArrayInputStream(getDetectionStateMappings().getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        CreateIndexRequest request = null;
        try {
            request = new CreateIndexRequest.Builder()
                .index(CommonName.DETECTION_STATE_INDEX)
                .mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper))
                .build();
        } catch (Exception e) {
            logger.error("Fail to init AD detection state index", e);
            e.printStackTrace();
        }
        CreateIndexResponse createIndexResponse = client.indices().create(request);

        markMappingUpToDate(ADIndex.STATE, createIndexResponse);
    }

    /**
     * Create the checkpoint index.
     *
     * @param actionListener action called after create index
     * @throws EndRunException EndRunException due to failure to get mapping
     */
    public void initCheckpointIndex() {
        JsonpMapper mapper = client._transport().jsonpMapper();
        ((JacksonJsonpMapper) mapper).objectMapper().registerModule(new JavaTimeModule());
        JsonParser parser = null;
        String mapping;
        try {
            mapping = getCheckpointMappings();
        } catch (IOException e) {
            throw new EndRunException("", "Cannot find checkpoint mapping file", true);
        }
        try {
            parser = mapper.jsonProvider().createParser(new ByteArrayInputStream(mapping.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        CreateIndexRequest request = null;
        try {
            CreateIndexRequest.Builder builder = new CreateIndexRequest.Builder()
                .index(CommonName.CHECKPOINT_INDEX_NAME)
                .mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper));
            builder = choosePrimaryShards(builder);
            request = builder.build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        CreateIndexResponse createIndexResponse = client.indices().create(request);

        markMappingUpToDate(ADIndex.CHECKPOINT, createIndexResponse);
    }

    @Override
    public void onMaster() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverAndDeleteHistoryIndex();

            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        } catch (Exception e) {
            // This should be run on cluster startup
            logger.error("Error rollover AD result indices. " + "Can't rollover AD result until clusterManager node is restarted.", e);
        }
    }

    @Override
    public void offMaster() {
        if (scheduledRollover != null) {
            scheduledRollover.cancel();
        }
    }

    private String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    private void rescheduleRollover() {
        if (getClusterState().getNodes().isLocalNodeElectedMaster()) {
            if (scheduledRollover != null) {
                scheduledRollover.cancel();
            }
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        }
    }

    void rolloverAndDeleteHistoryIndex() {
        if (!doesDefaultAnomalyResultIndexExist()) {
            return;
        }

        String adResultMapping = null;
        try {
            adResultMapping = getAnomalyResultMappings();
        } catch (IOException e) {
            logger.error("Fail to roll over AD result index, as can't get AD result index mapping");
            return;
        }

        JsonpMapper mapper = client._transport().jsonpMapper();
        ((JacksonJsonpMapper) mapper).objectMapper().registerModule(new JavaTimeModule());
        JsonParser parser = null;
        String mapping;
        try {
            mapping = adResultMapping;
        } catch (IOException e) {
            throw new EndRunException("", "Cannot find checkpoint mapping file", true);
        }
        try {
            parser = mapper.jsonProvider().createParser(new ByteArrayInputStream(mapping.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        RolloverRequest rolloverRequest = new RolloverRequest.Builder()
            .alias(CommonName.ANOMALY_RESULT_INDEX_ALIAS)
            .newIndex(AD_RESULT_HISTORY_INDEX_PATTERN)
            .mappings(IndexRolloverMapping._DESERIALIZER.deserialize(parser, mapper))
            .settings("number_of_shards", JsonData.of(getNumberOfPrimaryShards()))
            .settings("number_of_replicas", JsonData.of(1))
            .settings("hidden", JsonData.of(true))
            .build();

        try {
            RolloverResponse rolloverResponse = client.indices().rollover(rolloverRequest);
            if (!rolloverResponse.rolledOver()) {
                logger
                    .warn("{} not rolled over. Conditions were: {}", CommonName.ANOMALY_RESULT_INDEX_ALIAS, rolloverResponse.conditions());
            } else {
                IndexState indexStatetate = indexStates.computeIfAbsent(ADIndex.RESULT, IndexState::new);
                indexStatetate.mappingUpToDate = true;
                logger.info("{} rolled over. Conditions were: {}", CommonName.ANOMALY_RESULT_INDEX_ALIAS, rolloverResponse.conditions());
                deleteOldHistoryIndices();
            }
        } catch (Exception e) {
            logger.error("Fail to roll over result index", e);
        }
    }

    void deleteOldHistoryIndices() {
        Set<String> candidates = new HashSet<String>();
        try {
            ClusterState clusterState = getClusterState();
            String latestToDelete = null;
            long latest = Long.MIN_VALUE;
            for (ObjectCursor<IndexMetadata> cursor : clusterState.metadata().indices().values()) {
                IndexMetadata indexMetaData = cursor.value;
                long creationTime = indexMetaData.getCreationDate();

                if ((Instant.now().toEpochMilli() - creationTime) > historyRetentionPeriod.millis()) {
                    String indexName = indexMetaData.getIndex().getName();
                    candidates.add(indexName);
                    if (latest < creationTime) {
                        latest = creationTime;
                        latestToDelete = indexName;
                    }
                }
            }

            if (candidates.size() > 1) {
                // delete all indices except the last one because the last one may contain docs newer than the retention period
                candidates.remove(latestToDelete);
                String[] toDelete = candidates.toArray(Strings.EMPTY_ARRAY);
                for (String index : toDelete) {
                    DeleteRequest deleteRequest = new DeleteRequest.Builder().index(index).build();
                    DeleteResponse deleteResponse = client.delete(deleteRequest);
                    if (deleteResponse.result().jsonValue().equals("deleted")) {
                        logger.info("Succeeded in deleting expired anomaly result index: {}.", index);
                    } else {
                        logger.error("Deleting {} does not succeed.", index);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Fail to delete result indices", e);
        }
    }

    public void update() {
        if ((allMappingUpdated && allSettingUpdated) || updateRunningTimes >= maxUpdateRunningTimes || updateRunning.get()) {
            return;
        }
        updateRunning.set(true);
        updateRunningTimes++;

        // set updateRunning to false when both updateMappingIfNecessary and updateSettingIfNecessary
        // stop running
        final GroupedActionListener<Void> groupListeneer = new GroupedActionListener<>(
            ActionListener.wrap(r -> updateRunning.set(false), exception -> {
                updateRunning.set(false);
                logger.error("Fail to update AD indices", exception);
            }),
            // 2 since we need both updateMappingIfNecessary and updateSettingIfNecessary to return
            // before setting updateRunning to false
            2
        );

        updateMappingIfNecessary(groupListeneer);
        updateSettingIfNecessary(groupListeneer);
    }

    private void updateSettingIfNecessary(GroupedActionListener<Void> delegateListeneer) {
        if (allSettingUpdated) {
            delegateListeneer.onResponse(null);
            return;
        }

        List<ADIndex> updates = new ArrayList<>();
        for (ADIndex index : ADIndex.values()) {
            Boolean updated = indexStates.computeIfAbsent(index, IndexState::new).settingUpToDate;
            if (Boolean.FALSE.equals(updated)) {
                updates.add(index);
            }
        }
        if (updates.size() == 0) {
            allSettingUpdated = true;
            delegateListeneer.onResponse(null);
            return;
        }

        final GroupedActionListener<Void> conglomerateListeneer = new GroupedActionListener<>(
            ActionListener.wrap(r -> delegateListeneer.onResponse(null), exception -> {
                delegateListeneer.onResponse(null);
                logger.error("Fail to update AD indices' mappings", exception);
            }),
            updates.size()
        );
        for (ADIndex adIndex : updates) {
            logger.info(new ParameterizedMessage("Check [{}]'s setting", adIndex.getIndexName()));
            switch (adIndex) {
                // @anomaly-detection.create-detector Commented this code until we have support of Job Scheduler for extensibility
                // case JOB:
                // updateJobIndexSettingIfNecessary(indexStates.computeIfAbsent(adIndex, IndexState::new), conglomerateListeneer);
                // break;
                default:
                    // we don't have settings to update for other indices
                    IndexState indexState = indexStates.computeIfAbsent(adIndex, IndexState::new);
                    indexState.settingUpToDate = true;
                    logger.info(new ParameterizedMessage("Mark [{}]'s setting up-to-date", adIndex.getIndexName()));
                    conglomerateListeneer.onResponse(null);
                    break;
            }

        }
    }

    /**
     * Update mapping if schema version changes.
     */
    private void updateMappingIfNecessary(GroupedActionListener<Void> delegateListeneer) {
        if (allMappingUpdated) {
            delegateListeneer.onResponse(null);
            return;
        }

        List<ADIndex> updates = new ArrayList<>();
        for (ADIndex index : ADIndex.values()) {
            Boolean updated = indexStates.computeIfAbsent(index, IndexState::new).mappingUpToDate;
            if (Boolean.FALSE.equals(updated)) {
                updates.add(index);
            }
        }
        if (updates.size() == 0) {
            allMappingUpdated = true;
            delegateListeneer.onResponse(null);
            return;
        }

        final GroupedActionListener<Void> conglomerateListeneer = new GroupedActionListener<>(
            ActionListener.wrap(r -> delegateListeneer.onResponse(null), exception -> {
                delegateListeneer.onResponse(null);
                logger.error("Fail to update AD indices' mappings", exception);
            }),
            updates.size()
        );

        for (ADIndex adIndex : updates) {
            logger.info(new ParameterizedMessage("Check [{}]'s mapping", adIndex.getIndexName()));
            shouldUpdateIndex(adIndex, ActionListener.wrap(shouldUpdate -> {
                if (shouldUpdate) {

                    PutMappingRequest putMappingRequest = new PutMappingRequest.Builder()
                                                                            .index(adIndex.getIndexName())
                                                                            .source(new SourceField.Builder().includes(adIndex.getMapping()).build())
                                                                            .build();

                    adminClient
                        .indices()
                        .putMapping(
                            new PutMappingRequest().indices(adIndex.getIndexName()).source(adIndex.getMapping(), XContentType.JSON),
                            ActionListener.wrap(putMappingResponse -> {
                                if (putMappingResponse.isAcknowledged()) {
                                    logger.info(new ParameterizedMessage("Succeeded in updating [{}]'s mapping", adIndex.getIndexName()));
                                    markMappingUpdated(adIndex);
                                } else {
                                    logger.error(new ParameterizedMessage("Fail to update [{}]'s mapping", adIndex.getIndexName()));
                                }
                                conglomerateListeneer.onResponse(null);
                            }, exception -> {
                                logger
                                    .error(
                                        new ParameterizedMessage(
                                            "Fail to update [{}]'s mapping due to [{}]",
                                            adIndex.getIndexName(),
                                            exception.getMessage()
                                        )
                                    );
                                conglomerateListeneer.onFailure(exception);
                            })
                        );
                } else {
                    // index does not exist or the version is already up-to-date.
                    // When creating index, new mappings will be used.
                    // We don't need to update it.
                    logger.info(new ParameterizedMessage("We don't need to update [{}]'s mapping", adIndex.getIndexName()));
                    markMappingUpdated(adIndex);
                    conglomerateListeneer.onResponse(null);
                }
            }, exception -> {
                logger
                    .error(
                        new ParameterizedMessage("Fail to check whether we should update [{}]'s mapping", adIndex.getIndexName()),
                        exception
                    );
                conglomerateListeneer.onFailure(exception);
            }));

        }
    }

    private void markMappingUpdated(ADIndex adIndex) {
        IndexState indexState = indexStates.computeIfAbsent(adIndex, IndexState::new);
        if (Boolean.FALSE.equals(indexState.mappingUpToDate)) {
            indexState.mappingUpToDate = Boolean.TRUE;
            logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", adIndex.getIndexName()));
        }
    }

    private void shouldUpdateIndex(ADIndex index, ActionListener<Boolean> thenDo) {
        boolean exists = false;
        if (index.isAlias()) {
            exists = AnomalyDetectionIndices.doesAliasExists(getClusterState(), index.getIndexName());
        } else {
            exists = AnomalyDetectionIndices.doesIndexExists(getClusterState(), index.getIndexName());
        }
        if (false == exists) {
            thenDo.onResponse(Boolean.FALSE);
            return;
        }

        Integer newVersion = indexStates.computeIfAbsent(index, IndexState::new).schemaVersion;
        if (index.isAlias()) {
            GetAliasRequest getAliasRequest = new GetAliasRequest.Builder()
                .index(index.getIndexName())
                .ignoreUnavailable(true)
                .allowNoIndices(true)
                .expandWildcards(ExpandWildcard.Open, ExpandWildcard.Hidden)
                .build();

            try {
                GetAliasResponse getAliasResponse = client.indices().getAlias(getAliasRequest);
                String concreteIndex = null;
                for (Entry<String, IndexAliases> entry : getAliasResponse.result().entrySet()) {
                    if (false == entry.getValue().aliases().isEmpty()) {
                        // we assume the alias map to one concrete index, thus we can return after finding one
                        concreteIndex = entry.getKey();
                        break;
                    }
                }
                if (concreteIndex == null) {
                    thenDo.onResponse(Boolean.FALSE);
                    return;
                }
                shouldUpdateConcreteIndex(concreteIndex, newVersion, thenDo);
            } catch (Exception e) {
                logger.error(new ParameterizedMessage("Fail to get [{}]'s alias", index.getIndexName()), e);
            }
        } else {
            shouldUpdateConcreteIndex(index.getIndexName(), newVersion, thenDo);
        }
    }

    @SuppressWarnings("unchecked")
    private void shouldUpdateConcreteIndex(String concreteIndex, Integer newVersion, ActionListener<Boolean> thenDo) {
        IndexMetadata indexMeataData = getClusterState().getMetadata().indices().get(concreteIndex);
        if (indexMeataData == null) {
            thenDo.onResponse(Boolean.FALSE);
            return;
        }
        Integer oldVersion = CommonValue.NO_SCHEMA_VERSION;

        Map<String, Object> indexMapping = indexMeataData.mapping().getSourceAsMap();
        Object meta = indexMapping.get(META);
        if (meta != null && meta instanceof Map) {
            Map<String, Object> metaMapping = (Map<String, Object>) meta;
            Object schemaVersion = metaMapping.get(CommonName.SCHEMA_VERSION_FIELD);
            if (schemaVersion instanceof Integer) {
                oldVersion = (Integer) schemaVersion;
            }
        }
        thenDo.onResponse(newVersion > oldVersion);
    }

    private static Integer parseSchemaVersion(String mapping) {
        try {
            XContentParser xcp = XContentType.JSON
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, mapping);

            while (!xcp.isClosed()) {
                Token token = xcp.currentToken();
                if (token != null && token != XContentParser.Token.END_OBJECT && token != XContentParser.Token.START_OBJECT) {
                    if (xcp.currentName() != META) {
                        xcp.nextToken();
                        xcp.skipChildren();
                    } else {
                        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                            if (xcp.currentName().equals(SCHEMA_VERSION)) {

                                Integer version = xcp.intValue();
                                if (version < 0) {
                                    version = CommonValue.NO_SCHEMA_VERSION;
                                }
                                return version;
                            } else {
                                xcp.nextToken();
                            }
                        }

                    }
                }
                xcp.nextToken();
            }
            return CommonValue.NO_SCHEMA_VERSION;
        } catch (Exception e) {
            // since this method is called in the constructor that is called by AnomalyDetectorPlugin.createComponents,
            // we cannot throw checked exception
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param index Index metadata
     * @return The schema version of the given Index
     */
    public int getSchemaVersion(ADIndex index) {
        IndexState indexState = this.indexStates.computeIfAbsent(index, IndexState::new);
        return indexState.schemaVersion;
    }

    // @anomaly-detection.create-detector Commented this code until we have support of Job Scheduler for extensibility
    // private void updateJobIndexSettingIfNecessary(IndexState jobIndexState, ActionListener<Void> listener) {
    // GetSettingsRequest getSettingsRequest = new GetSettingsRequest()
    // .indices(ADIndex.JOB.getIndexName())
    // .names(
    // new String[] {
    // IndexMetadata.SETTING_NUMBER_OF_SHARDS,
    // IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
    // IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS }
    // );
    // client.execute(GetSettingsAction.INSTANCE, getSettingsRequest, ActionListener.wrap(settingResponse -> {
    // // auto expand setting is a range string like "1-all"
    // String autoExpandReplica = getStringSetting(settingResponse, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS);
    // // if the auto expand setting is already there, return immediately
    // if (autoExpandReplica != null) {
    // jobIndexState.settingUpToDate = true;
    // logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", ADIndex.JOB.getIndexName()));
    // listener.onResponse(null);
    // return;
    // }
    // Integer primaryShardsNumber = getIntegerSetting(settingResponse, IndexMetadata.SETTING_NUMBER_OF_SHARDS);
    // Integer replicaNumber = getIntegerSetting(settingResponse, IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
    // if (primaryShardsNumber == null || replicaNumber == null) {
    // logger
    // .error(
    // new ParameterizedMessage(
    // "Fail to find AD job index's primary or replica shard number: primary [{}], replica [{}]",
    // primaryShardsNumber,
    // replicaNumber
    // )
    // );
    // // don't throw exception as we don't know how to handle it and retry next time
    // listener.onResponse(null);
    // return;
    // }
    // // at least minJobIndexReplicas
    // // at most maxJobIndexReplicas / primaryShardsNumber replicas.
    // // For example, if we have 2 primary shards, since the max number of shards are maxJobIndexReplicas (20),
    // // we will use 20 / 2 = 10 replicas as the upper bound of replica.
    // int maxExpectedReplicas = Math.max(maxJobIndexReplicas / primaryShardsNumber, minJobIndexReplicas);
    // Settings updatedSettings = Settings
    // .builder()
    // .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, minJobIndexReplicas + "-" + maxExpectedReplicas)
    // .build();
    // final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(ADIndex.JOB.getIndexName())
    // .settings(updatedSettings);
    // client.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(response -> {
    // jobIndexState.settingUpToDate = true;
    // logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", ADIndex.JOB.getIndexName()));
    // listener.onResponse(null);
    // }, listener::onFailure));
    // }, e -> {
    // if (e instanceof IndexNotFoundException) {
    // // new index will be created with auto expand replica setting
    // jobIndexState.settingUpToDate = true;
    // logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", ADIndex.JOB.getIndexName()));
    // listener.onResponse(null);
    // } else {
    // listener.onFailure(e);
    // }
    // }));
    // }

    private static Integer getIntegerSetting(GetSettingsResponse settingsResponse, String settingKey) {
        Integer value = null;
        Iterator<Settings> iter = settingsResponse.getIndexToSettings().valuesIt();
        while (iter.hasNext()) {
            Settings settings = iter.next();
            value = settings.getAsInt(settingKey, null);
            if (value != null) {
                break;
            }
        }
        return value;
    }

    private static String getStringSetting(GetSettingsResponse settingsResponse, String settingKey) {
        String value = null;
        Iterator<Settings> iter = settingsResponse.getIndexToSettings().valuesIt();
        while (iter.hasNext()) {
            Settings settings = iter.next();
            value = settings.get(settingKey, null);
            if (value != null) {
                break;
            }
        }
        return value;
    }
}
