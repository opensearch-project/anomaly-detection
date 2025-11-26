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

import static org.opensearch.ad.constant.ADCommonName.DUMMY_AD_RESULT_ID;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_PRIMARY_SHARDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_RESULTS_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_INDEX_MAPPING_FILE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INSIGHTS_RESULT_INDEX_MAPPING_FILE;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.Client;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class provides utility methods for various anomaly detection indices.
 */
public class ADIndexManagement extends IndexManagement<ADIndex> {
    private static final Logger logger = LogManager.getLogger(ADIndexManagement.class);

    // The index name pattern to query all the AD result history indices
    public static final String AD_RESULT_HISTORY_INDEX_PATTERN = "<.opendistro-anomaly-results-history-{now/d}-1>";

    // The index name pattern to query all AD result, history and current AD result
    public static final String ALL_AD_RESULTS_INDEX_PATTERN = ".opendistro-anomaly-results*";

    /**
     * Constructor function
     *
     * @param client         OS client supports administrative actions
     * @param clusterService OS cluster service
     * @param threadPool     OS thread pool
     * @param settings       OS cluster setting
     * @param nodeFilter     Used to filter eligible nodes to host AD indices
     * @param maxUpdateRunningTimes max number of retries to update index mapping and setting
     * @param xContentRegistry registry for json parser
     * @throws IOException
     */
    public ADIndexManagement(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        DiscoveryNodeFilterer nodeFilter,
        int maxUpdateRunningTimes,
        NamedXContentRegistry xContentRegistry
    )
        throws IOException {
        super(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            maxUpdateRunningTimes,
            ADIndex.class,
            AD_MAX_PRIMARY_SHARDS.get(settings),
            AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings),
            AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD.get(settings),
            AD_RESULT_HISTORY_RETENTION_PERIOD.get(settings),
            ADIndex.RESULT.getMapping(),
            xContentRegistry,
            AnomalyDetector::parse,
            ADCommonName.CUSTOM_RESULT_INDEX_PREFIX,
            ADCommonName.CONFIG_INDEX
        );

        this.indexStates = new EnumMap<ADIndex, IndexState>(ADIndex.class);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD, it -> historyMaxDocs = it);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_ROLLOVER_PERIOD, it -> {
            historyRolloverPeriod = it;
            rescheduleRollover();
        });
        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_RESULT_HISTORY_RETENTION_PERIOD, it -> {
            historyRetentionPeriod = it;
        });

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_MAX_PRIMARY_SHARDS, it -> maxPrimaryShards = it);
    }

    /**
     * Get anomaly result index mapping json content.
     *
     * @return anomaly result index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getResultMappings() throws IOException {
        return getMappings(ANOMALY_RESULTS_INDEX_MAPPING_FILE);
    }

    /**
     * Retrieves the JSON mapping for the flattened result index with the "dynamic" field set to true
     * @return JSON mapping for the flattened result index.
     * @throws IOException if the mapping file cannot be read.
     */
    public static String getFlattenedResultMappings() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, Object> mapping = objectMapper
            .readValue(ADIndexManagement.class.getClassLoader().getResourceAsStream(ANOMALY_RESULTS_INDEX_MAPPING_FILE), Map.class);

        mapping.put("dynamic", true);

        return objectMapper.writeValueAsString(mapping);
    }

    /**
     * Get insights result index mapping json content.
     *
     * @return insights result index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getInsightsResultMappings() throws IOException {
        return getMappings(INSIGHTS_RESULT_INDEX_MAPPING_FILE);
    }

    /**
     * Get anomaly detector state index mapping json content.
     *
     * @return anomaly detector state index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getStateMappings() throws IOException {
        String detectionStateMappings = getMappings(ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE);
        String detectorIndexMappings = getConfigMappings();
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
        return getMappings(CHECKPOINT_INDEX_MAPPING_FILE);
    }

    /**
     * anomaly result index exist or not.
     *
     * @return true if anomaly result index exists
     */
    @Override
    public boolean doesDefaultResultIndexExist() {
        return doesAliasExist(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    /**
     * Anomaly state index exist or not.
     *
     * @return true if anomaly state index exists
     */
    @Override
    public boolean doesStateIndexExist() {
        return doesIndexExist(ADCommonName.DETECTION_STATE_INDEX);
    }

    /**
     * Checkpoint index exist or not.
     *
     * @return true if checkpoint index exists
     */
    @Override
    public boolean doesCheckpointIndexExist() {
        return doesIndexExist(ADCommonName.CHECKPOINT_INDEX_NAME);
    }

    /**
     * Create anomaly result index without checking exist or not.
     *
     * @param actionListener action called after create index
     */
    @Override
    public void initDefaultResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) {
        initResultIndexDirectly(
            AD_RESULT_HISTORY_INDEX_PATTERN,
            ADCommonName.ANOMALY_RESULT_INDEX_ALIAS,
            true,
            true,
            ADIndex.RESULT,
            actionListener
        );
    }

    /**
     * Check if insights result index alias exists.
     *
     * @return true if insights result index alias exists
     */
    public boolean doesInsightsResultIndexExist() {
        return doesAliasExist(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS);
    }

    /**
     * Create insights result index directly.
     * Uses the same rollover pattern as custom result indices.
     *
     * @param actionListener action called after create index
     */
    public void initInsightsResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) {
        try {
            String insightsResultIndexPattern = getRolloverIndexPattern(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS);
            String mapping = getInsightsResultMappings();

            CreateIndexRequest request = new CreateIndexRequest(insightsResultIndexPattern)
                .mapping(mapping, XContentType.JSON)
                .alias(new Alias(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS).writeIndex(true));

            request.settings(Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, customResultIndexAutoExpandReplica));

            adminClient.indices().create(request, actionListener);
        } catch (IOException e) {
            logger.error("Failed to init insights result index", e);
            actionListener.onFailure(e);
        }
    }

    /**
     * Create insights result index if it does not exist.
     *
     * @param actionListener action called after create index
     */
    public void initInsightsResultIndexIfAbsent(ActionListener<CreateIndexResponse> actionListener) {
        if (!doesInsightsResultIndexExist()) {
            initInsightsResultIndexDirectly(actionListener);
        } else {
            actionListener.onResponse(null);
        }
    }

    /**
     * Create the state index.
     *
     * @param actionListener action called after create index
     */
    @Override
    public void initStateIndex(ActionListener<CreateIndexResponse> actionListener) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(ADCommonName.DETECTION_STATE_INDEX)
                .mapping(getStateMappings(), XContentType.JSON)
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
    @Override
    public void initCheckpointIndex(ActionListener<CreateIndexResponse> actionListener) {
        String mapping;
        try {
            mapping = getCheckpointMappings();
        } catch (IOException e) {
            throw new EndRunException("", "Cannot find checkpoint mapping file", true);
        }
        CreateIndexRequest request = new CreateIndexRequest(ADCommonName.CHECKPOINT_INDEX_NAME).mapping(mapping, XContentType.JSON);
        choosePrimaryShards(request, true);
        adminClient.indices().create(request, markMappingUpToDate(ADIndex.CHECKPOINT, actionListener));
    }

    @Override
    protected void rolloverAndDeleteHistoryIndex() {
        // rollover anomaly result index
        rolloverAndDeleteHistoryIndex(
            ADCommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ALL_AD_RESULTS_INDEX_PATTERN,
            AD_RESULT_HISTORY_INDEX_PATTERN,
            ADIndex.RESULT
        );

        // rollover insights result index
        rolloverAndDeleteInsightsHistoryIndex();
    }

    /**
     * rollover and delete old insights result indices.
     * Uses same retention policy as system result index.
     */
    protected void rolloverAndDeleteInsightsHistoryIndex() {
        if (doesInsightsResultIndexExist()) {
            rolloverAndDeleteHistoryIndex(
                ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
                getAllHistoryIndexPattern(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS),
                getRolloverIndexPattern(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS),
                ADIndex.CUSTOM_INSIGHTS_RESULT
            );
        }
    }

    /**
     * Create config index directly.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link IndexManagement#getConfigMappings}
     */
    @Override
    public void initConfigIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        super.initConfigIndex(markMappingUpToDate(ADIndex.CONFIG, actionListener));
    }

    /**
     * Create job index.
     *
     * @param actionListener action called after create index
     */
    @Override
    public void initJobIndex(ActionListener<CreateIndexResponse> actionListener) {
        super.initJobIndex(markMappingUpToDate(ADIndex.JOB, actionListener));
    }

    @Override
    protected IndexRequest createDummyIndexRequest(String resultIndex) throws IOException {
        AnomalyResult dummyResult = AnomalyResult.getDummyResult();
        return new IndexRequest(resultIndex)
            .id(DUMMY_AD_RESULT_ID)
            .source(dummyResult.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS));
    }

    @Override
    protected DeleteRequest createDummyDeleteRequest(String resultIndex) throws IOException {
        return new DeleteRequest(resultIndex).id(DUMMY_AD_RESULT_ID);
    }

    @Override
    public void initCustomResultIndexDirectly(String resultIndex, ActionListener<CreateIndexResponse> actionListener) {
        initResultIndexDirectly(getCustomResultIndexPattern(resultIndex), resultIndex, false, false, ADIndex.RESULT, actionListener);
    }
}
