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

package org.opensearch.forecast.indices;

import static org.opensearch.forecast.constant.ForecastCommonName.DUMMY_FORECAST_RESULT_ID;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_INDEX_MAPPING_FILE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_MAX_PRIMARY_SHARDS;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULTS_INDEX_MAPPING_FILE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULT_HISTORY_RETENTION_PERIOD;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_STATE_INDEX_MAPPING_FILE;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.InjectSecurity;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public class ForecastIndexManagement extends IndexManagement<ForecastIndex> {
    private static final Logger logger = LogManager.getLogger(ForecastIndexManagement.class);

    // The index name pattern to query all the forecast result history indices
    public static final String FORECAST_RESULT_HISTORY_INDEX_PATTERN = "<opensearch-forecast-results-history-{now/d}-1>";

    // The index name pattern to query all forecast results, history and current forecast results
    public static final String ALL_FORECAST_RESULTS_INDEX_PATTERN = "opensearch-forecast-results*";

    /**
     * Constructor function
     *
     * @param client         OS client supports administrative actions
     * @param clusterService OS cluster service
     * @param threadPool     OS thread pool
     * @param settings       OS cluster setting
     * @param nodeFilter     Used to filter eligible nodes to host forecast indices
     * @param maxUpdateRunningTimes max number of retries to update index mapping and setting
     * @param xContentRegistry registry for json parser
     * @throws IOException
     */
    public ForecastIndexManagement(
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
            ForecastIndex.class,
            FORECAST_MAX_PRIMARY_SHARDS.get(settings),
            FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings),
            FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD.get(settings),
            FORECAST_RESULT_HISTORY_RETENTION_PERIOD.get(settings),
            ForecastIndex.RESULT.getMapping(),
            xContentRegistry,
            Forecaster::parse,
            ForecastCommonName.CUSTOM_RESULT_INDEX_PREFIX
        );
        this.indexStates = new EnumMap<ForecastIndex, IndexState>(ForecastIndex.class);

        this.clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD, it -> historyMaxDocs = it);

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD, it -> {
            historyRolloverPeriod = it;
            rescheduleRollover();
        });
        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_RESULT_HISTORY_RETENTION_PERIOD, it -> {
            historyRetentionPeriod = it;
        });

        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_MAX_PRIMARY_SHARDS, it -> maxPrimaryShards = it);

        this.updateRunningTimes = 0;
    }

    /**
     * Get forecast result index mapping json content.
     *
     * @return forecast result index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getResultMappings() throws IOException {
        return getMappings(FORECAST_RESULTS_INDEX_MAPPING_FILE);
    }

    /**
     * Get forecaster state index mapping json content.
     *
     * @return forecaster state index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getStateMappings() throws IOException {
        String forecastStateMappings = getMappings(FORECAST_STATE_INDEX_MAPPING_FILE);
        String forecasterIndexMappings = getConfigMappings();
        forecasterIndexMappings = forecasterIndexMappings
            .substring(forecasterIndexMappings.indexOf("\"properties\""), forecasterIndexMappings.lastIndexOf("}"));
        return forecastStateMappings.replace("FORECASTER_INDEX_MAPPING_PLACE_HOLDER", forecasterIndexMappings);
    }

    /**
     * Get checkpoint index mapping json content.
     *
     * @return checkpoint index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getCheckpointMappings() throws IOException {
        return getMappings(FORECAST_CHECKPOINT_INDEX_MAPPING_FILE);
    }

    /**
     * default forecaster result index exist or not.
     *
     * @return true if default forecaster result index exists
     */
    @Override
    public boolean doesDefaultResultIndexExist() {
        return doesAliasExist(ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS);
    }

    /**
     * Forecast state index exist or not.
     *
     * @return true if forecast state index exists
     */
    @Override
    public boolean doesStateIndexExist() {
        return doesIndexExist(ForecastCommonName.FORECAST_STATE_INDEX);
    }

    /**
     * Checkpoint index exist or not.
     *
     * @return true if checkpoint index exists
     */
    @Override
    public boolean doesCheckpointIndexExist() {
        return doesIndexExist(ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME);
    }

    /**
     * Create the state index.
     *
     * @param actionListener action called after create index
     */
    @Override
    public void initStateIndex(ActionListener<CreateIndexResponse> actionListener) {
        try {
            CreateIndexRequest request = new CreateIndexRequest(ForecastCommonName.FORECAST_STATE_INDEX)
                .mapping(getStateMappings(), XContentType.JSON)
                .settings(settings);
            adminClient.indices().create(request, markMappingUpToDate(ForecastIndex.STATE, actionListener));
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
        CreateIndexRequest request = new CreateIndexRequest(ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME)
            .mapping(mapping, XContentType.JSON);
        choosePrimaryShards(request, true);
        adminClient.indices().create(request, markMappingUpToDate(ForecastIndex.CHECKPOINT, actionListener));
    }

    @Override
    protected void rolloverAndDeleteHistoryIndex() {
        rolloverAndDeleteHistoryIndex(
            ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS,
            ALL_FORECAST_RESULTS_INDEX_PATTERN,
            FORECAST_RESULT_HISTORY_INDEX_PATTERN,
            ForecastIndex.RESULT
        );
    }

    /**
     * Create config index directly.
     *
     * @param actionListener action called after create index
     * @throws IOException IOException from {@link IndexManagement#getConfigMappings}
     */
    @Override
    public void initConfigIndex(ActionListener<CreateIndexResponse> actionListener) throws IOException {
        super.initConfigIndex(markMappingUpToDate(ForecastIndex.CONFIG, actionListener));
    }

    /**
     * Create config index.
     *
     * @param actionListener action called after create index
     */
    @Override
    public void initJobIndex(ActionListener<CreateIndexResponse> actionListener) {
        super.initJobIndex(markMappingUpToDate(ForecastIndex.JOB, actionListener));
    }

    @Override
    protected IndexRequest createDummyIndexRequest(String resultIndex) throws IOException {
        ForecastResult dummyResult = ForecastResult.getDummyResult();
        return new IndexRequest(resultIndex)
            .id(DUMMY_FORECAST_RESULT_ID)
            .source(dummyResult.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS));
    }

    @Override
    protected DeleteRequest createDummyDeleteRequest(String resultIndex) throws IOException {
        return new DeleteRequest(resultIndex).id(DUMMY_FORECAST_RESULT_ID);
    }

    @Override
    public void initDefaultResultIndexDirectly(ActionListener<CreateIndexResponse> actionListener) {
        initResultIndexDirectly(
            FORECAST_RESULT_HISTORY_INDEX_PATTERN,
            ForecastIndex.RESULT.getIndexName(),
            false,
            true,
            ForecastIndex.RESULT,
            actionListener
        );
    }

    @Override
    public void initCustomResultIndexDirectly(String resultIndex, ActionListener<CreateIndexResponse> actionListener) {
        initResultIndexDirectly(getCustomResultIndexPattern(resultIndex), resultIndex, false, false, ForecastIndex.RESULT, actionListener);
    }

    public <T> void validateDefaultResultIndexForBackendJob(
        String configId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        if (doesAliasExist(ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS)) {
            validateResultIndexAndExecute(
                ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS,
                () -> executeWithSecurityContext(configId, user, roles, function, listener, ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS),
                false,
                listener
            );
        } else {
            initDefaultResultIndex(configId, user, roles, function, listener);
        }
    }

    private <T> void initDefaultResultIndex(
        String configId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener
    ) {
        initDefaultResultIndexDirectly(ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                executeWithSecurityContext(configId, user, roles, function, listener, ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS);
            } else {
                String error = "Creating result index with mappings call not acknowledged";
                logger.error(error);
                listener.onFailure(new TimeSeriesException(error));
            }
        }, exception -> {
            if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                executeWithSecurityContext(configId, user, roles, function, listener, ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS);
            } else {
                listener.onFailure(exception);
            }
        }));
    }

    private <T> void executeWithSecurityContext(
        String securityLogId,
        String user,
        List<String> roles,
        ExecutorFunction function,
        ActionListener<T> listener,
        String indexOrAlias
    ) {
        try (InjectSecurity injectSecurity = new InjectSecurity(securityLogId, settings, client.threadPool().getThreadContext())) {
            injectSecurity.inject(user, roles);
            ActionListener<T> wrappedListener = ActionListener.wrap(listener::onResponse, e -> {
                injectSecurity.close();
                listener.onFailure(e);
            });
            validateResultIndexAndExecute(indexOrAlias, () -> {
                injectSecurity.close();
                function.execute();
            }, true, wrappedListener);
        } catch (Exception e) {
            logger.error("Failed to validate custom index for backend job " + securityLogId, e);
            listener.onFailure(e);
        }
    }
}
