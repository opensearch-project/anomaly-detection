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

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.cluster.LocalNodeMasterListener;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParser.Token;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

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

    private ClusterService clusterService;
    private final Client client;
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
     * @param client         ES client supports administrative actions
     * @param clusterService ES cluster service
     * @param threadPool     ES thread pool
     * @param settings       ES cluster setting
     * @param nodeFilter     Used to filter eligible nodes to host AD indices
     * @param maxUpdateRunningTimes max number of retries to update index mapping and setting
     */
    public AnomalyDetectionIndices(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        DiscoveryNodeFilterer nodeFilter,
        int maxUpdateRunningTimes
    ) {
        this.client = client;
        this.adminClient = client.admin();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addLocalNodeMasterListener(this);
        this.historyRolloverPeriod = AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings);
        this.historyMaxDocs = AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD.get(settings);
        this.historyRetentionPeriod = AD_RESULT_HISTORY_RETENTION_PERIOD.get(settings);
        this.maxPrimaryShards = MAX_PRIMARY_SHARDS.get(settings);

        this.nodeFilter = nodeFilter;

        this.indexStates = new EnumMap<ADIndex, IndexState>(ADIndex.class);

        this.allMappingUpdated = false;
        this.allSettingUpdated = false;
        this.updateRunning = new AtomicBoolean(false);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD, it -> historyMaxDocs = it);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_ROLLOVER_PERIOD, it -> {
            historyRolloverPeriod = it;
            rescheduleRollover();
        });
        this.clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(AD_RESULT_HISTORY_RETENTION_PERIOD, it -> { historyRetentionPeriod = it; });

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_PRIMARY_SHARDS, it -> maxPrimaryShards = it);

        this.settings = Settings.builder().put("index.hidden", true).build();

        this.maxUpdateRunningTimes = maxUpdateRunningTimes;
        this.updateRunningTimes = 0;

        this.AD_RESULT_FIELD_CONFIGS = null;
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
        return clusterService.state().getRoutingTable().hasIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
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
        return clusterService.state().metadata().hasAlias(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    public boolean doesIndexExist(String indexName) {
        return clusterService.state().metadata().hasIndex(indexName);
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
                listener.onFailure(new IllegalArgumentException(CommonErrorMessages.INVALID_RESULT_INDEX_MAPPING + resultIndex));
                return;
            }

            AnomalyResult dummyResult = AnomalyResult.getDummyResult();
            IndexRequest indexRequest = new IndexRequest(resultIndex)
                .id(DUMMY_AD_RESULT_ID)
                .source(dummyResult.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS));
            // User may have no write permission on custom result index. Talked with security plugin team, seems no easy way to verify
            // if user has write permission. So just tried to write and delete a dummy anomaly result to verify.
            client.index(indexRequest, ActionListener.wrap(response -> {
                logger.debug("Successfully wrote dummy AD result to result index {}", resultIndex);
                client.delete(new DeleteRequest(resultIndex).id(DUMMY_AD_RESULT_ID), ActionListener.wrap(deleteResponse -> {
                    logger.debug("Successfully deleted dummy AD result from result index {}", resultIndex);
                    function.execute();
                }, ex -> {
                    logger.error("Failed to delete dummy AD result from result index " + resultIndex, ex);
                    listener.onFailure(ex);
                }));
            }, exception -> {
                logger.error("Failed to write dummy AD result to result index " + resultIndex, exception);
                listener.onFailure(exception);
            }));
        } catch (Exception e) {
            logger.error("Failed to create detector with custom result index " + resultIndex, e);
            listener.onFailure(e);
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
            IndexMetadata indexMetadata = clusterService.state().metadata().index(resultIndex);
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
        return clusterService.state().getRoutingTable().hasIndex(CommonName.DETECTION_STATE_INDEX);
    }

    /**
     * Checkpoint index exist or not.
     *
     * @return true if checkpoint index exists
     */
    public boolean doesCheckpointIndexExist() {
        return clusterService.state().getRoutingTable().hasIndex(CommonName.CHECKPOINT_INDEX_NAME);
    }

    /**
     * Index exists or not
     * @param clusterServiceAccessor Cluster service
     * @param name Index name
     * @return true if the index exists
     */
    public static boolean doesIndexExists(ClusterService clusterServiceAccessor, String name) {
        return clusterServiceAccessor.state().getRoutingTable().hasIndex(name);
    }

    /**
     * Alias exists or not
     * @param clusterServiceAccessor Cluster service
     * @param alias Alias name
     * @return true if the alias exists
     */
    public static boolean doesAliasExists(ClusterService clusterServiceAccessor, String alias) {
        return clusterServiceAccessor.state().metadata().hasAlias(alias);
    }

    private ActionListener<CreateIndexResponse> markMappingUpToDate(ADIndex index, ActionListener<CreateIndexResponse> followingListener) {
        return ActionListener.wrap(createdResponse -> {
            if (createdResponse.isAcknowledged()) {
                IndexState indexStatetate = indexStates.computeIfAbsent(index, IndexState::new);
                if (Boolean.FALSE.equals(indexStatetate.mappingUpToDate)) {
                    indexStatetate.mappingUpToDate = Boolean.TRUE;
                    logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", index.getIndexName()));
                }
            }
            followingListener.onResponse(createdResponse);
        }, exception -> followingListener.onFailure(exception));
    }

    /**
     * Create anomaly detector index if not exist.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initAnomalyDetectorIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!doesAnomalyDetectorIndexExist()) {
            initAnomalyDetectorIndex(actionListener);
        }
    }

    /**
     * Create anomaly detector index directly.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyDetectorMappings}
     */
    public void initAnomalyDetectorIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
            .mapping(getAnomalyDetectorMappings(), XContentType.JSON)
            .settings(settings);
        adminClient.indices().create(request, markMappingUpToDate(ADIndex.CONFIG, actionListener));
    }

    /**
     * Create anomaly result index if not exist.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyResultMappings}
     */
    public void initDefaultAnomalyResultIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!doesDefaultAnomalyResultIndexExist()) {
            initDefaultAnomalyResultIndexDirectly(actionListener);
        }
    }

    /**
     * choose the number of primary shards for checkpoint, multientity result, and job scheduler based on the number of hot nodes. Max 10.
     * @param request The request to add the setting
     */
    private void choosePrimaryShards(CreateIndexRequest request) {
        choosePrimaryShards(request, true);
    }

    private void choosePrimaryShards(CreateIndexRequest request, boolean hiddenIndex) {
        request
            .settings(
                Settings
                    .builder()
                    // put 1 primary shards per hot node if possible
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, getNumberOfPrimaryShards())
                    // 1 replica for better search performance and fail-over
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put("index.hidden", hiddenIndex)
            );
    }

    private int getNumberOfPrimaryShards() {
        return Math.min(nodeFilter.getNumberOfEligibleDataNodes(), maxPrimaryShards);
    }

    /**
     * Create anomaly result index without checking exist or not.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link AnomalyDetectionIndices#getAnomalyResultMappings}
     */
    public void initDefaultAnomalyResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        initAnomalyResultIndexDirectly(AD_RESULT_HISTORY_INDEX_PATTERN, CommonName.ANOMALY_RESULT_INDEX_ALIAS, true, actionListener);
    }

    public void initCustomAnomalyResultIndexDirectly(String resultIndex, ActionListener<CreateIndexResponse> actionListener)
        throws IOException {
        initAnomalyResultIndexDirectly(resultIndex, null, false, actionListener);
    }

    public void initAnomalyResultIndexDirectly(
        String resultIndex,
        String alias,
        boolean hiddenIndex,
        ActionListener<CreateIndexResponse> actionListener
    ) throws IOException {
        String mapping = getAnomalyResultMappings();
        CreateIndexRequest request = new CreateIndexRequest(resultIndex).mapping(mapping, XContentType.JSON);
        if (alias != null) {
            request.alias(new Alias(CommonName.ANOMALY_RESULT_INDEX_ALIAS));
        }
        choosePrimaryShards(request, hiddenIndex);
        if (AD_RESULT_HISTORY_INDEX_PATTERN.equals(resultIndex)) {
            adminClient.indices().create(request, markMappingUpToDate(ADIndex.RESULT, actionListener));
        } else {
            adminClient.indices().create(request, actionListener);
        }
    }

    /**
     * Create anomaly detector job index.
     *
     * @param actionListener action called after create index
     */
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
     * @param actionListener action called after create index
     */
    public void initDetectionStateIndex(ActionListener<CreateIndexResponse> actionListener) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(CommonName.DETECTION_STATE_INDEX)
                .mapping(getDetectionStateMappings(), XContentType.JSON)
                .settings(settings);
            adminClient.indices().create(request, markMappingUpToDate(ADIndex.STATE, actionListener));
        } catch (IOException e) {
            logger.error("Fail to init AD detection state index", e);
            actionListener.onFailure(e);
        }
    }

    /**
     * Create the checkpoint index.
     *
     * @param actionListener action called after create index
     * @throws EndRunException EndRunException due to failure to get mapping
     */
    public void initCheckpointIndex(ActionListener<CreateIndexResponse> actionListener) {
        String mapping;
        try {
            mapping = getCheckpointMappings();
        } catch (IOException e) {
            throw new EndRunException("", "Cannot find checkpoint mapping file", true);
        }
        CreateIndexRequest request = new CreateIndexRequest(CommonName.CHECKPOINT_INDEX_NAME).mapping(mapping, XContentType.JSON);
        choosePrimaryShards(request);
        adminClient.indices().create(request, markMappingUpToDate(ADIndex.CHECKPOINT, actionListener));
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
        if (clusterService.state().getNodes().isLocalNodeElectedMaster()) {
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

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        RolloverRequest rollOverRequest = new RolloverRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS, null);
        String adResultMapping = null;
        try {
            adResultMapping = getAnomalyResultMappings();
        } catch (IOException e) {
            logger.error("Fail to roll over AD result index, as can't get AD result index mapping");
            return;
        }
        CreateIndexRequest createRequest = rollOverRequest.getCreateIndexRequest();

        createRequest.index(AD_RESULT_HISTORY_INDEX_PATTERN).mapping(adResultMapping, XContentType.JSON);

        choosePrimaryShards(createRequest);

        rollOverRequest.addMaxIndexDocsCondition(historyMaxDocs * getNumberOfPrimaryShards());
        adminClient.indices().rolloverIndex(rollOverRequest, ActionListener.wrap(response -> {
            if (!response.isRolledOver()) {
                logger
                    .warn("{} not rolled over. Conditions were: {}", CommonName.ANOMALY_RESULT_INDEX_ALIAS, response.getConditionStatus());
            } else {
                IndexState indexStatetate = indexStates.computeIfAbsent(ADIndex.RESULT, IndexState::new);
                indexStatetate.mappingUpToDate = true;
                logger.info("{} rolled over. Conditions were: {}", CommonName.ANOMALY_RESULT_INDEX_ALIAS, response.getConditionStatus());
                deleteOldHistoryIndices();
            }
        }, exception -> { logger.error("Fail to roll over result index", exception); }));
    }

    void deleteOldHistoryIndices() {
        Set<String> candidates = new HashSet<String>();

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest()
            .clear()
            .indices(AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN)
            .metadata(true)
            .local(true)
            .indicesOptions(IndicesOptions.strictExpand());

        adminClient.cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
            String latestToDelete = null;
            long latest = Long.MIN_VALUE;
            for (ObjectCursor<IndexMetadata> cursor : clusterStateResponse.getState().metadata().indices().values()) {
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
                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(toDelete);
                adminClient.indices().delete(deleteIndexRequest, ActionListener.wrap(deleteIndexResponse -> {
                    if (!deleteIndexResponse.isAcknowledged()) {
                        logger
                            .error(
                                "Could not delete one or more Anomaly result indices: {}. Retrying one by one.",
                                Arrays.toString(toDelete)
                            );
                        deleteIndexIteration(toDelete);
                    } else {
                        logger.info("Succeeded in deleting expired anomaly result indices: {}.", Arrays.toString(toDelete));
                    }
                }, exception -> {
                    logger.error("Failed to delete expired anomaly result indices: {}.", Arrays.toString(toDelete));
                    deleteIndexIteration(toDelete);
                }));
            }
        }, exception -> { logger.error("Fail to delete result indices", exception); }));
    }

    private void deleteIndexIteration(String[] toDelete) {
        for (String index : toDelete) {
            DeleteIndexRequest singleDeleteRequest = new DeleteIndexRequest(index);
            adminClient.indices().delete(singleDeleteRequest, ActionListener.wrap(singleDeleteResponse -> {
                if (!singleDeleteResponse.isAcknowledged()) {
                    logger.error("Retrying deleting {} does not succeed.", index);
                }
            }, exception -> {
                if (exception instanceof IndexNotFoundException) {
                    logger.info("{} was already deleted.", index);
                } else {
                    logger.error(new ParameterizedMessage("Retrying deleting {} does not succeed.", index), exception);
                }
            }));
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
            exists = AnomalyDetectionIndices.doesAliasExists(clusterService, index.getIndexName());
        } else {
            exists = AnomalyDetectionIndices.doesIndexExists(clusterService, index.getIndexName());
        }
        if (false == exists) {
            thenDo.onResponse(Boolean.FALSE);
            return;
        }

        Integer newVersion = indexStates.computeIfAbsent(index, IndexState::new).schemaVersion;
        if (index.isAlias()) {
            GetAliasesRequest getAliasRequest = new GetAliasesRequest()
                .aliases(index.getIndexName())
                .indicesOptions(IndicesOptions.lenientExpandOpenHidden());
            adminClient.indices().getAliases(getAliasRequest, ActionListener.wrap(getAliasResponse -> {
                String concreteIndex = null;
                for (ObjectObjectCursor<String, List<AliasMetadata>> entry : getAliasResponse.getAliases()) {
                    if (false == entry.value.isEmpty()) {
                        // we assume the alias map to one concrete index, thus we can return after finding one
                        concreteIndex = entry.key;
                        break;
                    }
                }
                if (concreteIndex == null) {
                    thenDo.onResponse(Boolean.FALSE);
                    return;
                }
                shouldUpdateConcreteIndex(concreteIndex, newVersion, thenDo);
            }, exception -> logger.error(new ParameterizedMessage("Fail to get [{}]'s alias", index.getIndexName()), exception)));
        } else {
            shouldUpdateConcreteIndex(index.getIndexName(), newVersion, thenDo);
        }
    }

    @SuppressWarnings("unchecked")
    private void shouldUpdateConcreteIndex(String concreteIndex, Integer newVersion, ActionListener<Boolean> thenDo) {
        IndexMetadata indexMeataData = clusterService.state().getMetadata().indices().get(concreteIndex);
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
