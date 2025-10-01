/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.AbstractForecastSyntheticDataTest;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.timeseries.TestHelpers;

import com.google.common.collect.ImmutableMap;

/**
 * Long-running forecast tests that should be excluded from regular test runs.
 * These tests are marked as long-running and test complex scenarios that take significant time.
 */
public class LongRunningForecastRestApiIT extends AbstractForecastSyntheticDataTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings(ForecastEnabledSetting.FORECAST_ENABLED, true);
        if (isHttps() && isResourceSharingFeatureEnabled()) {
            // this is needed for tests running with security enabled and resource-sharing flag enabled
            // the client being used doesn't seem to automatically grant access for some reason
            createRoleMapping("all_access", new ArrayList<>(Arrays.asList("admin")));
        }
    }

    public void testSuggestApiWithMinTaxfulPriceAndSkuCategory() throws Exception {
        String dataSet = "opensearch_sample_ecommerce_data";
        String minimalForecaster = createMinimalForecasterWithSumTaxfulPricePerCustomer(dataSet);

        /* │── index + data ──────────────────────────────────────────── */
        // Use the existing sample logs data or create ecommerce-style data
        loadSampleEcommerce(dataSet);

        /* │── suggest: interval only ───────────────────────────────── */
        Response response = TestHelpers
            .makeRequest(client(), "POST", SUGGEST_INTERVAL_URI, ImmutableMap.of(), TestHelpers.toHttpEntity(minimalForecaster), null);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        Map<String, Object> rsp = entityAsMap(response);
        int intervalM = ((Number) ((Map<?, ?>) ((Map<?, ?>) rsp.get("interval")).get("period")).get("interval")).intValue();
        assertTrue("interval must be > 0", intervalM > 0);

        /* │── suggest: interval + horizon + history + windowDelay ──── */
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                SUGGEST_INTERVAL_HORIZON_HISTORY_DELAY_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(minimalForecaster),
                null
            );
        rsp = entityAsMap(response);
        assertTrue("interval must be > 0", intervalM > 0);
        assertEquals(24, ((Number) rsp.get("horizon")).intValue());
        int history = ((Number) rsp.get("history")).intValue();
        assertTrue("history must be > 0", history > 0);
        int windowDelay = ((Number) ((Map<?, ?>) ((Map<?, ?>) rsp.get("windowDelay")).get("period")).get("interval")).intValue();
        assertTrue("window delay must be >= 0", windowDelay >= 0);

        /* │── validate (no issues) ─────────────────────────────────── */
        Response validateRsp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                VALIDATE_FORECASTER_MODEL,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(createFullForecasterWithMinTaxfulPriceAndSku(dataSet, intervalM, history, windowDelay)),
                null
            );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(validateRsp));
        Map<String, Object> validateResp = entityAsMap(validateRsp);
        assertTrue("validation should pass without issues: " + validateResp, validateResp.isEmpty());

        /* │── run-once until TEST_COMPLETE ─────────────────────────── */
        String forecasterId = createForecasterWithSumTaxfulPriceAndSku(dataSet, intervalM, history, windowDelay);

        if (isResourceSharingFeatureEnabled()) {
            response = waitForSharingVisibility("POST", String.format(Locale.ROOT, RUN_ONCE_FORECASTER, forecasterId), null, client());
        } else {
            TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    String.format(Locale.ROOT, RUN_ONCE_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                );
        }

        waitForState(forecasterId, "run_once_task", "TEST_COMPLETE", Duration.ofSeconds(20), 6, client());

        /* │── start realtime until RUNNING ─────────────────────────── */
        TestHelpers
            .makeRequest(client(), "POST", String.format(Locale.ROOT, START_FORECASTER, forecasterId), null, (HttpEntity) null, null);
        waitForState(forecasterId, "realtime_task", "RUNNING", Duration.ofSeconds(60), 6, client());
    }

    /** Creates a minimal forecaster configuration for testing suggest API with min taxful_total_price feature and sku categorical field. */
    private String createMinimalForecasterWithSumTaxfulPricePerCustomer(String dataSet) {
        return "{\n"
            + "  \"name\":\"test_min_taxful_price_sku\",\n"
            + "  \"description\":\"Test forecaster for min taxful_total_price with sku categorical field\",\n"
            + "  \"indices\":[\""
            + dataSet
            + "\"],\n"
            + "  \"filter_query\":{\"match_all\":{}},\n"
            + "  \"time_field\":\"order_date\",\n"
            + "  \"feature_attributes\":[{\n"
            + "     \"feature_id\":\"min_taxful_price\",\n"
            + "     \"feature_name\":\"min_taxful_price\",\n"
            + "     \"feature_enabled\":true,\n"
            + "     \"aggregation_query\":{\"sum_taxful_price\":{\"sum\":{\"field\":\"taxful_total_price\"}}}\n"
            // + " }]\n"
            + "  }],\n"
            // + " \"category_field\":[\"category.keyword\"]\n"
            + "  \"category_field\":[\"products.category.keyword\"]\n"
            + "}";
    }

    /** Creates a full forecaster configuration with suggested parameters for min taxful_total_price feature and sku categorical field. */
    private String createFullForecasterWithMinTaxfulPriceAndSku(String dataSet, int interval, int history, int windowDelay) {
        long nowMillis = Instant.now().toEpochMilli();
        String featureName = "min_taxful_price";

        return "{\n"
            + "  \"name\":\"test_min_taxful_price_sku\",\n"
            + "  \"description\":\"Test forecaster for min taxful_total_price with sku categorical field\",\n"
            + "  \"time_field\":\"order_date\",\n"
            + "  \"indices\":[\""
            + dataSet
            + "\"],\n"
            + "  \"filter_query\":{\"match_all\":{\"boost\":1.0}},\n"
            + "  \"window_delay\":{\"period\":{\"interval\":"
            + windowDelay
            + ",\"unit\":\"Minutes\"}},\n"
            + "  \"shingle_size\":8,\n"
            + "  \"schema_version\":0,\n"
            + "  \"feature_attributes\":[{\n"
            + "     \"feature_id\":\""
            + featureName
            + "\",\n"
            + "     \"feature_name\":\""
            + featureName
            + "\",\n"
            + "     \"feature_enabled\":true,\n"
            + "     \"aggregation_query\":{\""
            + featureName
            + "\":{\"sum\":{\"field\":\"taxful_total_price\"}}}\n"
            + "  }],\n"
            + "  \"recency_emphasis\":2560,\n"
            + "  \"history\":"
            + history
            + ",\n"
            + "  \"ui_metadata\":{ \"features\":{ \""
            + featureName
            + "\":{ \"aggregationBy\":\"sum\", \"aggregationOf\":\"taxful_total_price\", \"featureType\":\"simple_aggs\" } }, \"filters\":[] },\n"
            + "  \"last_update_time\":"
            + nowMillis
            + ",\n"
            + "  \"forecast_interval\":{\"period\":{\"interval\":"
            + interval
            + ",\"unit\":\"Minutes\"}},\n"
            + "  \"horizon\":24,\n"
            // + " \"category_field\":[\"category.keyword\"]\n"
            + "  \"category_field\":[\"products.category.keyword\"]\n"
            + "}";
    }

    /** Creates and returns a forecaster ID for testing min taxful_total_price feature with sku categorical field. */
    private String createForecasterWithSumTaxfulPriceAndSku(String dataSet, int interval, int history, int windowDelay) throws Exception {
        String body = createFullForecasterWithMinTaxfulPriceAndSku(dataSet, interval, history, windowDelay);

        Response rsp = TestHelpers.makeRequest(client(), "POST", CREATE_FORECASTER, null, TestHelpers.toHttpEntity(body), null);
        assertEquals(HttpStatus.SC_CREATED, rsp.getStatusLine().getStatusCode());
        return entityAsMap(rsp).get("_id").toString();
    }
}
