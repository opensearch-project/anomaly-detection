/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.timeseries.CrossClusterRestTestCase;
import org.opensearch.timeseries.model.TaskState;

import com.google.common.collect.ImmutableSet;

/**
 * Running a forecaster once over a source index on a remote cluster reads that index across the cluster link and writes
 * forecast results, whether the config names the remote index directly, through a remote alias, or through a remote
 * wildcard. Run by the integTestCrossCluster task.
 *
 * Uses run-once rather than a real-time start: run-once executes the forecaster over the source window and writes
 * results, so reaching TEST_COMPLETE and finding a forecast value proves the remote read end to end. A real-time start
 * only schedules the job and returns before the first run reads anything.
 */
public class ForecastCrossClusterIT extends CrossClusterRestTestCase {
    private static final int TRAIN_SPLIT = 200;
    private static final String FEATURE_FIELD = "visitCount";
    private static final String RULE_CATEGORY_FIELD = "cityName";

    public void testRunOnceOverRemoteIndex() throws Exception {
        prepare();
        runOnceAndVerify(remote("rule"));
    }

    public void testRunOnceOverRemoteAlias() throws Exception {
        prepare();
        createFollowerAlias("rule-alias", "rule");
        runOnceAndVerify(remote("rule-alias"));
    }

    public void testRunOnceOverRemoteWildcard() throws Exception {
        prepare();
        runOnceAndVerify(remote("rul*"));
    }

    // Not in setUp()/@Before: these REST calls need the client, which the framework builds in its own @Before, after setUp().
    private Instant trainTime;

    private void prepare() throws Exception {
        updateClusterSettings(ForecastEnabledSetting.FORECAST_ENABLED, true);
        connectRemoteCluster();
        trainTime = loadRuleDataToFollower(TRAIN_SPLIT);
    }

    private void runOnceAndVerify(String index) throws Exception {
        String forecasterId = createForecaster(index);
        Response runOnce = client().performRequest(new Request("POST", String.format(Locale.ROOT, RUN_ONCE_FORECASTER, forecasterId)));
        assertNotNull(entityAsMap(runOnce).get("task_id"));

        // run-once runs as a task; TEST_COMPLETE means it read the remote source and finished the test run
        waitUntilTaskReachState(forecasterId, ImmutableSet.of(TaskState.TEST_COMPLETE.name()), client());

        // a forecast value for this forecaster confirms the remote source was read and a forecast was produced
        Request search = new Request("POST", SEARCH_RESULTS);
        search
            .setJsonEntity(
                String
                    .format(
                        Locale.ROOT,
                        "{\"size\":1,\"query\":{\"bool\":{\"filter\":["
                            + "{\"term\":{\"forecaster_id\":\"%s\"}},{\"exists\":{\"field\":\"forecast_value\"}}]}}}",
                        forecasterId
                    )
            );
        Map<String, Object> hits = (Map<String, Object>) entityAsMap(client().performRequest(search)).get("hits");
        int total = (int) ((Map<String, Object>) hits.get("total")).get("value");
        assertTrue("expected a forecast value for " + index + " but found none", total > 0);
    }

    private String createForecaster(String index) throws Exception {
        // +1 so the window covers the training data, which ends at trainTime
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        String body = String
            .format(
                Locale.ROOT,
                "{\"name\":\"ccit-%d\",\"time_field\":\"%s\",\"indices\":[\"%s\"],"
                    + "\"feature_attributes\":[{\"feature_id\":\"max1\",\"feature_name\":\"max1\",\"feature_enabled\":true,"
                    + "\"importance\":1,\"aggregation_query\":{\"max1\":{\"max\":{\"field\":\"%s\"}}}}],"
                    + "\"window_delay\":{\"period\":{\"interval\":%d,\"unit\":\"MINUTES\"}},\"schema_version\":2,\"horizon\":24,"
                    + "\"forecast_interval\":{\"period\":{\"interval\":10,\"unit\":\"MINUTES\"}},\"category_field\":[\"%s\"]}",
                System.nanoTime(),
                TIME_FIELD,
                index,
                FEATURE_FIELD,
                windowDelayMinutes,
                RULE_CATEGORY_FIELD
            );
        Request request = new Request("POST", CREATE_FORECASTER);
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        String forecasterId = (String) entityAsMap(response).get("_id");
        assertNotNull(forecasterId);
        return forecasterId;
    }
}
