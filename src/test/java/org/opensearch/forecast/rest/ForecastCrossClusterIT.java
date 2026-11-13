/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.timeseries.CrossClusterRestTestCase;

/**
 * A forecaster whose source index is on a remote cluster can be created and started, whether the config names the
 * remote index directly, through a remote alias, or through a remote wildcard. Exercises the cross-cluster read path
 * against two clusters; run by the integTestCrossCluster task.
 */
public class ForecastCrossClusterIT extends CrossClusterRestTestCase {
    private Instant end;

    public void testForecasterOverRemoteIndex() throws Exception {
        prepare();
        createFollowerIndex("ccit-metrics-a", 200, end);
        createAndStart(remote("ccit-metrics-a"));
    }

    public void testForecasterOverRemoteAlias() throws Exception {
        prepare();
        createFollowerIndex("ccit-metrics-a", 200, end);
        createFollowerIndex("ccit-metrics-b", 200, end);
        createFollowerAlias("ccit-metrics-alias", "ccit-metrics-a", "ccit-metrics-b");
        createAndStart(remote("ccit-metrics-alias"));
    }

    public void testForecasterOverRemoteWildcard() throws Exception {
        prepare();
        createFollowerIndex("ccit-metrics-a", 200, end);
        createFollowerIndex("ccit-metrics-b", 200, end);
        createAndStart(remote("ccit-metrics-*"));
    }

    // Not in setUp()/@Before: these REST calls need the client, which the framework builds in its own @Before, after setUp().
    private void prepare() throws Exception {
        updateClusterSettings(ForecastEnabledSetting.FORECAST_ENABLED, true);
        connectRemoteCluster();
        end = Instant.now().truncatedTo(ChronoUnit.MINUTES);
    }

    private void createAndStart(String index) throws Exception {
        String forecasterId = createForecaster(index);
        try {
            Response start = client().performRequest(new Request("POST", String.format(Locale.ROOT, START_FORECASTER, forecasterId)));
            assertEquals(RestStatus.OK.getStatus(), start.getStatusLine().getStatusCode());
        } finally {
            try {
                client().performRequest(new Request("POST", String.format(Locale.ROOT, STOP_FORECASTER, forecasterId)));
            } catch (Exception e) {
                logger.info("stop failed, ignoring", e);
            }
        }
    }

    private String createForecaster(String index) throws Exception {
        long windowDelayMinutes = 1;
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
                VALUE_FIELD,
                windowDelayMinutes,
                CATEGORY_FIELD
            );
        Request request = new Request("POST", CREATE_FORECASTER);
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.CREATED.getStatus(), response.getStatusLine().getStatusCode());
        Map<String, Object> created = entityAsMap(response);
        String forecasterId = (String) created.get("_id");
        assertNotNull(forecasterId);
        return forecasterId;
    }
}
