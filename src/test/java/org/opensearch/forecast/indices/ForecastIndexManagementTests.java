/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.indices;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagementIntegTestCase;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false)
public class ForecastIndexManagementTests extends IndexManagementIntegTestCase<ForecastIndex, ForecastIndexManagement> {
    private ForecastIndexManagement indices;
    private Settings settings;
    private DiscoveryNodeFilterer nodeFilter;

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    // help register setting using TimeSeriesAnalyticsPlugin.getSettings.
    // Otherwise, ForecastIndexManagement's constructor would fail due to
    // unregistered settings like FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD.
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TimeSeriesAnalyticsPlugin.class);
    }

    @Before
    public void setup() throws IOException {
        settings = Settings
            .builder()
            .put("plugins.forecast.forecast_result_history_rollover_period", TimeValue.timeValueHours(12))
            .put("plugins.forecast.forecast_result_history_retention_period", TimeValue.timeValueHours(24))
            .put("plugins.forecast.forecast_result_history_max_docs", 10000L)
            .put("plugins.forecast.request_timeout", TimeValue.timeValueSeconds(10))
            .build();

        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);

        nodeFilter = new DiscoveryNodeFilterer(clusterService());

        indices = new ForecastIndexManagement(
            client(),
            clusterService(),
            client().threadPool(),
            settings,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
            NamedXContentRegistry.EMPTY
        );
    }

    public void testForecastResultIndexNotExists() {
        boolean exists = indices.doesDefaultResultIndexExist();
        assertFalse(exists);
    }

    public void testForecastResultIndexExists() throws IOException {
        indices.initDefaultResultIndexIfAbsent(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), ForecastIndex.RESULT.getIndexName());
        assertTrue(indices.doesDefaultResultIndexExist());
    }

    public void testForecastResultIndexExistsAndNotRecreate() throws IOException {
        indices
            .initDefaultResultIndexIfAbsent(
                TestHelpers.createActionListener(response -> logger.info("Acknowledged: " + response.isAcknowledged()), failure -> {
                    throw new RuntimeException("should not recreate index");
                })
            );
        TestHelpers.waitForIndexCreationToComplete(client(), ForecastIndex.RESULT.getIndexName());
        if (client().admin().indices().prepareExists(ForecastIndex.RESULT.getIndexName()).get().isExists()) {
            indices.initDefaultResultIndexIfAbsent(TestHelpers.createActionListener(response -> {
                throw new RuntimeException("should not recreate index " + ForecastIndex.RESULT.getIndexName());
            }, failure -> { throw new RuntimeException("should not recreate index " + ForecastIndex.RESULT.getIndexName(), failure); }));
        }
    }

    public void testCheckpointIndexNotExists() {
        boolean exists = indices.doesCheckpointIndexExist();
        assertFalse(exists);
    }

    public void testCheckpointIndexExists() throws IOException {
        indices.initCheckpointIndex(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), ForecastIndex.STATE.getIndexName());
        assertTrue(indices.doesCheckpointIndexExist());
    }

    public void testStateIndexNotExists() {
        boolean exists = indices.doesStateIndexExist();
        assertFalse(exists);
    }

    public void testStateIndexExists() throws IOException {
        indices.initStateIndex(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), ForecastIndex.STATE.getIndexName());
        assertTrue(indices.doesStateIndexExist());
    }

    public void testConfigIndexNotExists() {
        boolean exists = indices.doesConfigIndexExist();
        assertFalse(exists);
    }

    public void testConfigIndexExists() throws IOException {
        indices.initConfigIndex(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), ForecastIndex.CONFIG.getIndexName());
        assertTrue(indices.doesConfigIndexExist());
    }

    public void testCustomResultIndexExists() throws IOException {
        String indexName = "a";
        assertTrue(!(client().admin().indices().prepareExists(indexName).get().isExists()));
        indices
            .initCustomResultIndexDirectly(
                indexName,
                TestHelpers.createActionListener(response -> logger.info("Acknowledged: " + response.isAcknowledged()), failure -> {
                    throw new RuntimeException("should not recreate index");
                })
            );
        TestHelpers.waitForIndexCreationToComplete(client(), indexName);
        assertTrue((client().admin().indices().prepareExists(indexName).get().isExists()));
    }

    public void testJobIndexNotExists() {
        boolean exists = indices.doesJobIndexExist();
        assertFalse(exists);
    }

    public void testJobIndexExists() throws IOException {
        indices.initJobIndex(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), ForecastIndex.JOB.getIndexName());
        assertTrue(indices.doesJobIndexExist());
    }

    public void testValidateCustomIndexForBackendJobNoIndex() {
        validateCustomIndexForBackendJobNoIndex(indices);
    }

    public void testValidateCustomIndexForBackendJobInvalidMapping() {
        validateCustomIndexForBackendJobInvalidMapping(indices);
    }

    public void testValidateCustomIndexForBackendJob() throws IOException, InterruptedException {
        validateCustomIndexForBackendJob(indices, ForecastIndexManagement.getResultMappings());
    }

    public void testRollOver() throws IOException, InterruptedException {
        indices.initDefaultResultIndexIfAbsent(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), ForecastIndex.RESULT.getIndexName());
        client().index(indices.createDummyIndexRequest(ForecastIndex.RESULT.getIndexName())).actionGet();

        GetAliasesResponse getAliasesResponse = admin().indices().prepareGetAliases(ForecastIndex.RESULT.getIndexName()).get();
        String oldIndex = getAliasesResponse.getAliases().keySet().iterator().next();

        settings = Settings
            .builder()
            .put("plugins.forecast.forecast_result_history_rollover_period", TimeValue.timeValueHours(12))
            .put("plugins.forecast.forecast_result_history_retention_period", TimeValue.timeValueHours(0))
            .put("plugins.forecast.forecast_result_history_max_docs", 0L)
            .put("plugins.forecast.forecast_result_history_max_docs_per_shard", 0L)
            .put("plugins.forecast.request_timeout", TimeValue.timeValueSeconds(10))
            .build();

        nodeFilter = new DiscoveryNodeFilterer(clusterService());

        indices = new ForecastIndexManagement(
            client(),
            clusterService(),
            client().threadPool(),
            settings,
            nodeFilter,
            TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
            NamedXContentRegistry.EMPTY
        );
        indices.rolloverAndDeleteHistoryIndex();

        // replace the last two characters "-1" to "000002"?
        // Example:
        // Input: opensearch-forecast-results-history-2023.06.15-1
        // Output: opensearch-forecast-results-history-2023.06.15-000002
        String newIndex = oldIndex.replaceFirst("-1$", "-000002");
        TestHelpers.waitForIndexCreationToComplete(client(), newIndex);

        getAliasesResponse = admin().indices().prepareGetAliases(ForecastIndex.RESULT.getIndexName()).get();
        String currentPointedIndex = getAliasesResponse.getAliases().keySet().iterator().next();
        assertEquals(newIndex, currentPointedIndex);

        client().index(indices.createDummyIndexRequest(ForecastIndex.RESULT.getIndexName())).actionGet();
        // now we have two indices
        indices.rolloverAndDeleteHistoryIndex();

        String thirdIndexName = getIncrementedIndex(newIndex);
        TestHelpers.waitForIndexCreationToComplete(client(), thirdIndexName);
        getAliasesResponse = admin().indices().prepareGetAliases(ForecastIndex.RESULT.getIndexName()).get();
        currentPointedIndex = getAliasesResponse.getAliases().keySet().iterator().next();
        assertEquals(thirdIndexName, currentPointedIndex);

        // we have already deleted the oldest index since retention period is 0 hrs
        int retry = 0;
        while (retry < 10) {
            try {
                client().admin().indices().prepareGetIndex().addIndices(oldIndex).get();
                retry++;
                // wait for index to be deleted
                Thread.sleep(1000);
            } catch (IndexNotFoundException e) {
                MatcherAssert.assertThat(e.getMessage(), is(String.format(Locale.ROOT, "no such index [%s]", oldIndex)));
                break;
            }
        }

        assertTrue(retry < 20);

        // 2nd oldest index should be fine as we keep at one old index
        GetIndexResponse response = client().admin().indices().prepareGetIndex().addIndices(newIndex).get();
        String[] indicesInResponse = response.indices();
        MatcherAssert.assertThat(indicesInResponse, notNullValue());
        MatcherAssert.assertThat(indicesInResponse.length, equalTo(1));
        MatcherAssert.assertThat(indicesInResponse[0], equalTo(newIndex));

        response = client().admin().indices().prepareGetIndex().addIndices(thirdIndexName).get();
        indicesInResponse = response.indices();
        MatcherAssert.assertThat(indicesInResponse, notNullValue());
        MatcherAssert.assertThat(indicesInResponse.length, equalTo(1));
        MatcherAssert.assertThat(indicesInResponse[0], equalTo(thirdIndexName));
    }

    /**
     * Increment the last digit oif an index name.
     * @param input. Example: opensearch-forecast-results-history-2023.06.15-000002
     * @return Example: opensearch-forecast-results-history-2023.06.15-000003
     */
    private String getIncrementedIndex(String input) {
        int lastDash = input.lastIndexOf('-');

        String prefix = input.substring(0, lastDash + 1);
        String numberPart = input.substring(lastDash + 1);

        // Increment the number part
        int incrementedNumber = Integer.parseInt(numberPart) + 1;

        // Use String.format to keep the leading zeros
        String newNumberPart = String.format(Locale.ROOT, "%06d", incrementedNumber);

        return prefix + newNumberPart;
    }

    public void testInitCustomResultIndexAndExecuteIndexNotExist() throws InterruptedException {
        String resultIndex = "abc";
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(function).execute();

        indices.initCustomResultIndexAndExecute(resultIndex, function, listener);
        latch.await(20, TimeUnit.SECONDS);
        verify(listener, never()).onFailure(any(Exception.class));
    }

    public void testInitCustomResultIndexAndExecuteIndex() throws InterruptedException, IOException {
        String indexName = "abc";
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        indices
            .initCustomResultIndexDirectly(
                indexName,
                TestHelpers.createActionListener(response -> logger.info("Acknowledged: " + response.isAcknowledged()), failure -> {
                    throw new RuntimeException("should not recreate index");
                })
            );
        TestHelpers.waitForIndexCreationToComplete(client(), indexName);
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(function).execute();

        indices.initCustomResultIndexAndExecute(indexName, function, listener);
        latch.await(20, TimeUnit.SECONDS);
        verify(listener, never()).onFailure(any(Exception.class));
    }
}
