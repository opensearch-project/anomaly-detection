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

package org.opensearch.timeseries.indices;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.indices.replication.common.ReplicationType.DOCUMENT;
import static org.opensearch.timeseries.constant.CommonMessages.CAN_NOT_FIND_RESULT_INDEX;

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
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
import org.opensearch.action.admin.indices.settings.get.GetSettingsAction;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.InjectSecurity;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.constant.CommonValue;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public abstract class IndexManagement<IndexType extends Enum<IndexType> & TimeSeriesIndex> implements LocalNodeClusterManagerListener {
    private static final Logger logger = LogManager.getLogger(IndexManagement.class);

    // minimum shards of the job index
    public static int minJobIndexReplicas = 1;
    // maximum shards of the job index
    public static int maxJobIndexReplicas = 20;
    // package private for testing
    public static final String META = "_meta";
    public static final String SCHEMA_VERSION = "schema_version";

    protected ClusterService clusterService;
    protected final Client client;
    protected final AdminClient adminClient;
    protected final ThreadPool threadPool;
    protected DiscoveryNodeFilterer nodeFilter;
    // index settings
    protected final Settings settings;
    // don't retry updating endlessly. Can be annoying if there are too many exception logs.
    protected final int maxUpdateRunningTimes;

    // whether all index have the correct mappings
    protected boolean allMappingUpdated;
    // whether all index settings are updated
    protected boolean allSettingUpdated;
    // we only want one update at a time
    protected final AtomicBoolean updateRunning;
    // the number of times updates run
    protected int updateRunningTimes;
    private final Class<IndexType> indexType;
    // keep track of whether the mapping version is up-to-date
    protected EnumMap<IndexType, IndexState> indexStates;
    protected int maxPrimaryShards;
    private Scheduler.Cancellable scheduledRollover = null;
    protected volatile TimeValue historyRolloverPeriod;
    protected volatile Long historyMaxDocs;
    protected volatile TimeValue historyRetentionPeriod;
    // result index mapping to valida custom index
    private Map<String, Object> RESULT_FIELD_CONFIGS;
    private String resultMapping;

    protected class IndexState {
        // keep track of whether the mapping version is up-to-date
        public Boolean mappingUpToDate;
        // keep track of whether the setting needs to change
        public Boolean settingUpToDate;
        // record schema version reading from the mapping file
        public Integer schemaVersion;

        public IndexState(String mappingFile) {
            this.mappingUpToDate = false;
            this.settingUpToDate = false;
            this.schemaVersion = IndexManagement.parseSchemaVersion(mappingFile);
        }
    }

    protected IndexManagement(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        DiscoveryNodeFilterer nodeFilter,
        int maxUpdateRunningTimes,
        Class<IndexType> indexType,
        int maxPrimaryShards,
        TimeValue historyRolloverPeriod,
        Long historyMaxDocs,
        TimeValue historyRetentionPeriod,
        String resultMapping
    )
        throws IOException {
        this.client = client;
        this.adminClient = client.admin();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addLocalNodeClusterManagerListener(this);
        this.nodeFilter = nodeFilter;
        this.settings = Settings.builder().put("index.hidden", true).build();
        this.maxUpdateRunningTimes = maxUpdateRunningTimes;
        this.indexType = indexType;
        this.maxPrimaryShards = maxPrimaryShards;
        this.historyRolloverPeriod = historyRolloverPeriod;
        this.historyMaxDocs = historyMaxDocs;
        this.historyRetentionPeriod = historyRetentionPeriod;

        this.allMappingUpdated = false;
        this.allSettingUpdated = false;
        this.updateRunning = new AtomicBoolean(false);
        this.updateRunningTimes = 0;
        this.resultMapping = resultMapping;
    }

    /**
     * Alias exists or not
     * @param alias Alias name
     * @return true if the alias exists
     */
    public boolean doesAliasExist(String alias) {
        return clusterService.state().metadata().hasAlias(alias);
    }

    public static Integer parseSchemaVersion(String mapping) {
        try {
            XContentParser xcp = XContentType.JSON
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, mapping);

            while (!xcp.isClosed()) {
                Token token = xcp.currentToken();
                if (token != null && token != XContentParser.Token.END_OBJECT && token != XContentParser.Token.START_OBJECT) {
                    if (xcp.currentName() != IndexManagement.META) {
                        xcp.nextToken();
                        xcp.skipChildren();
                    } else {
                        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                            if (xcp.currentName().equals(IndexManagement.SCHEMA_VERSION)) {

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
            // since this method is called in the constructor that is called by TimeSeriesAnalyticsPlugin.createComponents,
            // we cannot throw checked exception
            throw new RuntimeException(e);
        }
    }

    protected static Integer getIntegerSetting(GetSettingsResponse settingsResponse, String settingKey) {
        Integer value = null;
        for (Settings settings : settingsResponse.getIndexToSettings().values()) {
            value = settings.getAsInt(settingKey, null);
            if (value != null) {
                break;
            }
        }
        return value;
    }

    protected static String getStringSetting(GetSettingsResponse settingsResponse, String settingKey) {
        String value = null;
        for (Settings settings : settingsResponse.getIndexToSettings().values()) {
            value = settings.get(settingKey, null);
            if (value != null) {
                break;
            }
        }
        return value;
    }

    public boolean doesIndexExist(String indexName) {
        return clusterService.state().metadata().hasIndex(indexName);
    }

    protected static String getMappings(String mappingFileRelativePath) throws IOException {
        URL url = IndexManagement.class.getClassLoader().getResource(mappingFileRelativePath);
        return Resources.toString(url, Charsets.UTF_8);
    }

    protected void choosePrimaryShards(CreateIndexRequest request, boolean hiddenIndex) {
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

    protected void deleteOldHistoryIndices(String indexPattern, TimeValue historyRetentionPeriod) {
        Set<String> candidates = new HashSet<String>();

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest()
            .clear()
            .indices(indexPattern)
            .metadata(true)
            .local(true)
            .indicesOptions(IndicesOptions.strictExpand());

        adminClient.cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
            String latestToDelete = null;
            long latest = Long.MIN_VALUE;
            for (IndexMetadata indexMetaData : clusterStateResponse.getState().metadata().indices().values()) {
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
                        logger.error("Could not delete one or more result indices: {}. Retrying one by one.", Arrays.toString(toDelete));
                        deleteIndexIteration(toDelete);
                    } else {
                        logger.info("Succeeded in deleting expired result indices: {}.", Arrays.toString(toDelete));
                    }
                }, exception -> {
                    logger.error("Failed to delete expired result indices: {}.", Arrays.toString(toDelete));
                    deleteIndexIteration(toDelete);
                }));
            }
        }, exception -> { logger.error("Fail to delete result indices", exception); }));
    }

    protected void deleteIndexIteration(String[] toDelete) {
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

    @SuppressWarnings("unchecked")
    protected void shouldUpdateConcreteIndex(String concreteIndex, Integer newVersion, ActionListener<Boolean> thenDo) {
        IndexMetadata indexMeataData = clusterService.state().getMetadata().indices().get(concreteIndex);
        if (indexMeataData == null) {
            thenDo.onResponse(Boolean.FALSE);
            return;
        }
        Integer oldVersion = CommonValue.NO_SCHEMA_VERSION;

        Map<String, Object> indexMapping = indexMeataData.mapping().getSourceAsMap();
        Object meta = indexMapping.get(IndexManagement.META);
        if (meta != null && meta instanceof Map) {
            Map<String, Object> metaMapping = (Map<String, Object>) meta;
            Object schemaVersion = metaMapping.get(org.opensearch.timeseries.constant.CommonName.SCHEMA_VERSION_FIELD);
            if (schemaVersion instanceof Integer) {
                oldVersion = (Integer) schemaVersion;
            }
        }
        thenDo.onResponse(newVersion > oldVersion);
    }

    protected void updateJobIndexSettingIfNecessary(String indexName, IndexState jobIndexState, ActionListener<Void> listener) {
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest()
            .indices(indexName)
            .names(
                new String[] {
                    IndexMetadata.SETTING_NUMBER_OF_SHARDS,
                    IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
                    IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS }
            );
        client.execute(GetSettingsAction.INSTANCE, getSettingsRequest, ActionListener.wrap(settingResponse -> {
            // auto expand setting is a range string like "1-all"
            String autoExpandReplica = getStringSetting(settingResponse, IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS);
            // if the auto expand setting is already there, return immediately
            if (autoExpandReplica != null) {
                jobIndexState.settingUpToDate = true;
                logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", indexName));
                listener.onResponse(null);
                return;
            }
            Integer primaryShardsNumber = getIntegerSetting(settingResponse, IndexMetadata.SETTING_NUMBER_OF_SHARDS);
            Integer replicaNumber = getIntegerSetting(settingResponse, IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
            if (primaryShardsNumber == null || replicaNumber == null) {
                logger
                    .error(
                        new ParameterizedMessage(
                            "Fail to find job index's primary or replica shard number: primary [{}], replica [{}]",
                            primaryShardsNumber,
                            replicaNumber
                        )
                    );
                // don't throw exception as we don't know how to handle it and retry next time
                listener.onResponse(null);
                return;
            }
            // at least minJobIndexReplicas
            // at most maxJobIndexReplicas / primaryShardsNumber replicas.
            // For example, if we have 2 primary shards, since the max number of shards are maxJobIndexReplicas (20),
            // we will use 20 / 2 = 10 replicas as the upper bound of replica.
            int maxExpectedReplicas = Math
                .max(IndexManagement.maxJobIndexReplicas / primaryShardsNumber, IndexManagement.minJobIndexReplicas);
            Settings updatedSettings = Settings
                .builder()
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, IndexManagement.minJobIndexReplicas + "-" + maxExpectedReplicas)
                .build();
            final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName).settings(updatedSettings);
            client.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(response -> {
                jobIndexState.settingUpToDate = true;
                logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", indexName));
                listener.onResponse(null);
            }, listener::onFailure));
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                // new index will be created with auto expand replica setting
                jobIndexState.settingUpToDate = true;
                logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", indexName));
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Create config index if not exist.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link IndexManagement#getConfigMappings}
     */
    public void initConfigIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        if (!doesConfigIndexExist()) {
            initConfigIndex(actionListener);
        }
    }

    /**
     * Create config index directly.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link IndexManagement#getConfigMappings}
     */
    public void initConfigIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        // time series indices need RAW (e.g., we want users to be able to consume AD results as soon as possible
        // and send out an alert if anomalies found).
        Settings replicationSettings = Settings.builder().put(SETTING_REPLICATION_TYPE, DOCUMENT.name()).build();
        CreateIndexRequest request = new CreateIndexRequest(CommonName.CONFIG_INDEX, replicationSettings)
            .mapping(getConfigMappings(), XContentType.JSON)
            .settings(settings);
        adminClient.indices().create(request, actionListener);
    }

    /**
     * Config index exist or not.
     *
     * @return true if config index exists
     */
    public boolean doesConfigIndexExist() {
        return doesIndexExist(CommonName.CONFIG_INDEX);
    }

    /**
     * Job index exist or not.
     *
     * @return true if anomaly detector job index exists
     */
    public boolean doesJobIndexExist() {
        return doesIndexExist(CommonName.JOB_INDEX);
    }

    /**
     * Get config index mapping in json format.
     *
     * @return config index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getConfigMappings() throws IOException {
        return getMappings(TimeSeriesSettings.CONFIG_INDEX_MAPPING_FILE);
    }

    /**
     * Get job index mapping in json format.
     *
     * @return job index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getJobMappings() throws IOException {
        return getMappings(TimeSeriesSettings.JOBS_INDEX_MAPPING_FILE);
    }

    /**
     * Createjob index.
     *
     * @param actionListener action called after create index
     */
    public void initJobIndex(ActionListener<CreateIndexResponse> actionListener) {
        try {
            // time series indices need RAW (e.g., we want users to be able to consume AD results as soon as
            // possible and send out an alert if anomalies found).
            Settings replicationSettings = Settings.builder().put(SETTING_REPLICATION_TYPE, DOCUMENT.name()).build();
            CreateIndexRequest request = new CreateIndexRequest(CommonName.JOB_INDEX, replicationSettings)
                .mapping(getJobMappings(), XContentType.JSON);
            request
                .settings(
                    Settings
                        .builder()
                        // AD job index is small. 1 primary shard is enough
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        // Job scheduler puts both primary and replica shards in the
                        // hash ring. Auto-expand the number of replicas based on the
                        // number of data nodes (up to 20) in the cluster so that each node can
                        // become a coordinating node. This is useful when customers
                        // scale out their cluster so that we can do adaptive scaling
                        // accordingly.
                        // At least 1 replica for fail-over.
                        .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, minJobIndexReplicas + "-" + maxJobIndexReplicas)
                        .put("index.hidden", true)
                );
            adminClient.indices().create(request, actionListener);
        } catch (IOException e) {
            logger.error("Fail to init AD job index", e);
            actionListener.onFailure(e);
        }
    }

    public <T> void validateCustomResultIndexAndExecute(String resultIndex, ExecutorFunction function, ActionListener<T> listener) {
        try {
            if (!isValidResultIndexMapping(resultIndex)) {
                logger.warn("Can't create detector with custom result index {} as its mapping is invalid", resultIndex);
                listener.onFailure(new IllegalArgumentException(CommonMessages.INVALID_RESULT_INDEX_MAPPING + resultIndex));
                return;
            }

            IndexRequest indexRequest = createDummyIndexRequest(resultIndex);

            // User may have no write permission on custom result index. Talked with security plugin team, seems no easy way to verify
            // if user has write permission. So just tried to write and delete a dummy forecast result to verify.
            client.index(indexRequest, ActionListener.wrap(response -> {
                logger.debug("Successfully wrote dummy result to result index {}", resultIndex);
                client.delete(createDummyDeleteRequest(resultIndex), ActionListener.wrap(deleteResponse -> {
                    logger.debug("Successfully deleted dummy result from result index {}", resultIndex);
                    function.execute();
                }, ex -> {
                    logger.error("Failed to delete dummy result from result index " + resultIndex, ex);
                    listener.onFailure(ex);
                }));
            }, exception -> {
                logger.error("Failed to write dummy result to result index " + resultIndex, exception);
                listener.onFailure(exception);
            }));
        } catch (Exception e) {
            logger.error("Failed to validate custom result index " + resultIndex, e);
            listener.onFailure(e);
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
                logger.error("Fail to update time series indices", exception);
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

        List<IndexType> updates = new ArrayList<>();
        for (IndexType index : indexType.getEnumConstants()) {
            Boolean updated = indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping())).settingUpToDate;
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
                logger.error("Fail to update time series indices' mappings", exception);
            }),
            updates.size()
        );
        for (IndexType timeseriesIndex : updates) {
            logger.info(new ParameterizedMessage("Check [{}]'s setting", timeseriesIndex.getIndexName()));
            if (timeseriesIndex.isJobIndex()) {
                updateJobIndexSettingIfNecessary(
                    ADIndex.JOB.getIndexName(),
                    indexStates.computeIfAbsent(timeseriesIndex, k -> new IndexState(k.getMapping())),
                    conglomerateListeneer
                );
            } else {
                // we don't have settings to update for other indices
                IndexState indexState = indexStates.computeIfAbsent(timeseriesIndex, k -> new IndexState(k.getMapping()));
                indexState.settingUpToDate = true;
                logger.info(new ParameterizedMessage("Mark [{}]'s setting up-to-date", timeseriesIndex.getIndexName()));
                conglomerateListeneer.onResponse(null);
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

        List<IndexType> updates = new ArrayList<>();
        for (IndexType index : indexType.getEnumConstants()) {
            Boolean updated = indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping())).mappingUpToDate;
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
                logger.error("Fail to update time series indices' mappings", exception);
            }),
            updates.size()
        );

        for (IndexType adIndex : updates) {
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

    private void markMappingUpdated(IndexType adIndex) {
        IndexState indexState = indexStates.computeIfAbsent(adIndex, k -> new IndexState(k.getMapping()));
        if (Boolean.FALSE.equals(indexState.mappingUpToDate)) {
            indexState.mappingUpToDate = Boolean.TRUE;
            logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", adIndex.getIndexName()));
        }
    }

    private void shouldUpdateIndex(IndexType index, ActionListener<Boolean> thenDo) {
        boolean exists = false;
        if (index.isAlias()) {
            exists = doesAliasExist(index.getIndexName());
        } else {
            exists = doesIndexExist(index.getIndexName());
        }
        if (false == exists) {
            thenDo.onResponse(Boolean.FALSE);
            return;
        }

        Integer newVersion = indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping())).schemaVersion;
        if (index.isAlias()) {
            GetAliasesRequest getAliasRequest = new GetAliasesRequest()
                .aliases(index.getIndexName())
                .indicesOptions(IndicesOptions.lenientExpandOpenHidden());
            adminClient.indices().getAliases(getAliasRequest, ActionListener.wrap(getAliasResponse -> {
                String concreteIndex = null;
                for (Map.Entry<String, List<AliasMetadata>> entry : getAliasResponse.getAliases().entrySet()) {
                    if (false == entry.getValue().isEmpty()) {
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
            }, exception -> logger.error(new ParameterizedMessage("Fail to get [{}]'s alias", index.getIndexName()), exception)));
        } else {
            shouldUpdateConcreteIndex(index.getIndexName(), newVersion, thenDo);
        }
    }

    /**
     *
     * @param index Index metadata
     * @return The schema version of the given Index
     */
    public int getSchemaVersion(IndexType index) {
        IndexState indexState = this.indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping()));
        return indexState.schemaVersion;
    }

    public <T> void initCustomResultIndexAndExecute(String resultIndex, ExecutorFunction function, ActionListener<T> listener) {
        if (!doesIndexExist(resultIndex)) {
            initCustomResultIndexDirectly(resultIndex, ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    logger.info("Successfully created result index {}", resultIndex);
                    validateCustomResultIndexAndExecute(resultIndex, function, listener);
                } else {
                    String error = "Creating result index with mappings call not acknowledged: " + resultIndex;
                    logger.error(error);
                    listener.onFailure(new EndRunException(error, false));
                }
            }, exception -> {
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    // It is possible the index has been created while we sending the create request
                    validateCustomResultIndexAndExecute(resultIndex, function, listener);
                } else {
                    logger.error("Failed to create result index " + resultIndex, exception);
                    listener.onFailure(exception);
                }
            }));
        } else {
            validateCustomResultIndexAndExecute(resultIndex, function, listener);
        }
    }

    public <T> void validateCustomIndexForBackendJob(
        String resultIndex,
        String securityLogId,
        String user,
        List<String> roles,
        ExecutorFunction function,
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
        try (InjectSecurity injectSecurity = new InjectSecurity(securityLogId, settings, client.threadPool().getThreadContext())) {
            injectSecurity.inject(user, roles);
            ActionListener<T> wrappedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
                injectSecurity.close();
                listener.onFailure(e);
            });
            validateCustomResultIndexAndExecute(resultIndex, () -> {
                injectSecurity.close();
                function.execute();
            }, wrappedListener);
        } catch (Exception e) {
            logger.error("Failed to validate custom index for backend job " + securityLogId, e);
            listener.onFailure(e);
        }
    }

    protected int getNumberOfPrimaryShards() {
        return Math.min(nodeFilter.getNumberOfEligibleDataNodes(), maxPrimaryShards);
    }

    @Override
    public void onClusterManager() {
        try {
            // try to rollover immediately as we might be restarting the cluster
            rolloverAndDeleteHistoryIndex();

            // schedule the next rollover for approx MAX_AGE later
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        } catch (Exception e) {
            // This should be run on cluster startup
            logger.error("Error rollover result indices. " + "Can't rollover result until clusterManager node is restarted.", e);
        }
    }

    @Override
    public void offClusterManager() {
        if (scheduledRollover != null) {
            scheduledRollover.cancel();
        }
    }

    private String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    protected void rescheduleRollover() {
        if (clusterService.state().getNodes().isLocalNodeElectedClusterManager()) {
            if (scheduledRollover != null) {
                scheduledRollover.cancel();
            }
            scheduledRollover = threadPool
                .scheduleWithFixedDelay(() -> rolloverAndDeleteHistoryIndex(), historyRolloverPeriod, executorName());
        }
    }

    private void initResultMapping() throws IOException {
        if (RESULT_FIELD_CONFIGS != null) {
            // we have already initiated the field
            return;
        }

        Map<String, Object> asMap = XContentHelper.convertToMap(new BytesArray(resultMapping), false, XContentType.JSON).v2();
        Object properties = asMap.get(CommonName.PROPERTIES);
        if (properties instanceof Map) {
            RESULT_FIELD_CONFIGS = (Map<String, Object>) properties;
        } else {
            logger.error("Fail to read result mapping file.");
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
            if (RESULT_FIELD_CONFIGS == null) {
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

            for (String fieldName : RESULT_FIELD_CONFIGS.keySet()) {
                Object defaultSchema = RESULT_FIELD_CONFIGS.get(fieldName);
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
     * Create forecast result index if not exist.
     *
     * @param actionListener action called after create index
     */
    public void initDefaultResultIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) {
        if (!doesDefaultResultIndexExist()) {
            initDefaultResultIndexDirectly(actionListener);
        }
    }

    protected ActionListener<CreateIndexResponse> markMappingUpToDate(
        IndexType index,
        ActionListener<CreateIndexResponse> followingListener
    ) {
        return ActionListener.wrap(createdResponse -> {
            if (createdResponse.isAcknowledged()) {
                IndexState indexStatetate = indexStates.computeIfAbsent(index, k -> new IndexState(k.getMapping()));
                if (Boolean.FALSE.equals(indexStatetate.mappingUpToDate)) {
                    indexStatetate.mappingUpToDate = Boolean.TRUE;
                    logger.info(new ParameterizedMessage("Mark [{}]'s mapping up-to-date", index.getIndexName()));
                }
            }
            followingListener.onResponse(createdResponse);
        }, exception -> followingListener.onFailure(exception));
    }

    protected void rolloverAndDeleteHistoryIndex(
        String resultIndexAlias,
        String allResultIndicesPattern,
        String rolloverIndexPattern,
        IndexType resultIndex
    ) {
        if (!doesDefaultResultIndexExist()) {
            return;
        }

        // We have to pass null for newIndexName in order to get Elastic to increment the index count.
        RolloverRequest rollOverRequest = new RolloverRequest(resultIndexAlias, null);

        CreateIndexRequest createRequest = rollOverRequest.getCreateIndexRequest();

        // time series indices need RAW (e.g., we want users to be able to consume AD results as soon as possible
        // and send out an alert if anomalies found).
        Settings replicationSettings = Settings.builder().put(SETTING_REPLICATION_TYPE, DOCUMENT.name()).build();
        createRequest.index(rolloverIndexPattern).settings(replicationSettings).mapping(resultMapping, XContentType.JSON);

        choosePrimaryShards(createRequest, true);

        rollOverRequest.addMaxIndexDocsCondition(historyMaxDocs * getNumberOfPrimaryShards());
        adminClient.indices().rolloverIndex(rollOverRequest, ActionListener.wrap(response -> {
            if (!response.isRolledOver()) {
                logger.warn("{} not rolled over. Conditions were: {}", resultIndexAlias, response.getConditionStatus());
            } else {
                IndexState indexStatetate = indexStates.computeIfAbsent(resultIndex, k -> new IndexState(k.getMapping()));
                indexStatetate.mappingUpToDate = true;
                logger.info("{} rolled over. Conditions were: {}", resultIndexAlias, response.getConditionStatus());
                deleteOldHistoryIndices(allResultIndicesPattern, historyRetentionPeriod);
            }
        }, exception -> { logger.error("Fail to roll over result index", exception); }));
    }

    protected void initResultIndexDirectly(
        String resultIndexName,
        String alias,
        boolean hiddenIndex,
        String resultIndexPattern,
        IndexType resultIndex,
        ActionListener<CreateIndexResponse> actionListener
    ) {
        // time series indices need RAW (e.g., we want users to be able to consume AD results as soon as possible
        // and send out an alert if anomalies found).
        Settings replicationSettings = Settings.builder().put(SETTING_REPLICATION_TYPE, DOCUMENT.name()).build();
        CreateIndexRequest request = new CreateIndexRequest(resultIndexName, replicationSettings).mapping(resultMapping, XContentType.JSON);
        if (alias != null) {
            request.alias(new Alias(alias));
        }
        choosePrimaryShards(request, hiddenIndex);
        if (resultIndexPattern.equals(resultIndexName)) {
            adminClient.indices().create(request, markMappingUpToDate(resultIndex, actionListener));
        } else {
            adminClient.indices().create(request, actionListener);
        }
    }

    public abstract boolean doesCheckpointIndexExist();

    public abstract void initCheckpointIndex(ActionListener<CreateIndexResponse> actionListener);

    public abstract boolean doesDefaultResultIndexExist();

    public abstract boolean doesStateIndexExist();

    public abstract void initDefaultResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener);

    protected abstract IndexRequest createDummyIndexRequest(String resultIndex) throws IOException;

    protected abstract DeleteRequest createDummyDeleteRequest(String resultIndex) throws IOException;

    protected abstract void rolloverAndDeleteHistoryIndex();

    public abstract void initCustomResultIndexDirectly(String resultIndex, ActionListener<CreateIndexResponse> actionListener);

    public abstract void initStateIndex(ActionListener<CreateIndexResponse> actionListener);
}
