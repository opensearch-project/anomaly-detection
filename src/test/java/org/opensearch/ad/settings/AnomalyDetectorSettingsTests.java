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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.settings;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings({ "rawtypes" })
public class AnomalyDetectorSettingsTests extends OpenSearchTestCase {
    AnomalyDetectorPlugin plugin;

    @Before
    public void setup() {
        this.plugin = new AnomalyDetectorPlugin();
    }

    public void testAllLegacyOpenDistroSettingsReturned() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue(
            "legacy setting must be returned from settings",
            settings
                .containsAll(
                    Arrays
                        .asList(
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                            LegacyOpenDistroAnomalyDetectorSettings.REQUEST_TIMEOUT,
                            LegacyOpenDistroAnomalyDetectorSettings.DETECTION_INTERVAL,
                            LegacyOpenDistroAnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                            LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                            LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                            LegacyOpenDistroAnomalyDetectorSettings.COOLDOWN_MINUTES,
                            LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_MINUTES,
                            LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
                            LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                            LegacyOpenDistroAnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                            LegacyOpenDistroAnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
                            LegacyOpenDistroAnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
                            LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
                            LegacyOpenDistroAnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
                            LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE
                        )
                )
        );
    }

    public void testAllOpenSearchSettingsReturned() {
        List<Setting<?>> settings = plugin.getSettings();
        assertTrue(
            "opensearch setting must be returned from settings",
            settings
                .containsAll(
                    Arrays
                        .asList(
                            AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                            AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
                            AnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                            AnomalyDetectorSettings.REQUEST_TIMEOUT,
                            AnomalyDetectorSettings.DETECTION_INTERVAL,
                            AnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                            AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                            AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                            AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                            AnomalyDetectorSettings.COOLDOWN_MINUTES,
                            AnomalyDetectorSettings.BACKOFF_MINUTES,
                            AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
                            AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
                            AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                            AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                            AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
                            AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                            AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT,
                            AnomalyDetectorSettings.INDEX_PRESSURE_HARD_LIMIT,
                            AnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
                            AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES,
                            AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
                            AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
                            AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
                            AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE,
                            AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_CONCURRENCY,
                            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
                            AnomalyDetectorSettings.ENTITY_COLDSTART_QUEUE_CONCURRENCY,
                            AnomalyDetectorSettings.RESULT_WRITE_QUEUE_CONCURRENCY,
                            AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_BATCH_SIZE,
                            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
                            AnomalyDetectorSettings.RESULT_WRITE_QUEUE_BATCH_SIZE,
                            AnomalyDetectorSettings.COLD_ENTITY_QUEUE_CONCURRENCY,
                            AnomalyDetectorSettings.DEDICATED_CACHE_SIZE,
                            AnomalyDetectorSettings.COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                            AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
                            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                            AnomalyDetectorSettings.RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                            AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
                            AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS,
                            AnomalyDetectorSettings.MAX_ENTITIES_PER_INTERVAL,
                            AnomalyDetectorSettings.PAGE_SIZE
                        )
                )
        );
    }

    public void testAllLegacyOpenDistroSettingsFallback() {
        assertEquals(
            AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.MAX_ANOMALY_FEATURES.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MAX_ANOMALY_FEATURES.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.REQUEST_TIMEOUT.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.REQUEST_TIMEOUT.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.DETECTION_INTERVAL.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.DETECTION_INTERVAL.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.DETECTION_WINDOW_DELAY.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.DETECTION_WINDOW_DELAY.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.COOLDOWN_MINUTES.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.COOLDOWN_MINUTES.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.BACKOFF_MINUTES.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_MINUTES.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY.get(Settings.EMPTY)
        );
        // MAX_ENTITIES_FOR_PREVIEW does not use legacy setting
        assertEquals(Integer.valueOf(10), AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW.get(Settings.EMPTY));
        // INDEX_PRESSURE_SOFT_LIMIT does not use legacy setting
        assertEquals(Float.valueOf(0.6f), AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT.get(Settings.EMPTY));
        assertEquals(
            AnomalyDetectorSettings.MAX_PRIMARY_SHARDS.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MAX_PRIMARY_SHARDS.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(Settings.EMPTY)
        );
        assertEquals(
            AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE.get(Settings.EMPTY),
            LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE.get(Settings.EMPTY)
        );
    }

    public void testSettingsGetValue() {
        Settings settings = Settings.builder().put("plugins.anomaly_detection.request_timeout", "42s").build();
        assertEquals(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings), TimeValue.timeValueSeconds(42));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings), TimeValue.timeValueSeconds(10));

        settings = Settings.builder().put("plugins.anomaly_detection.max_anomaly_detectors", 99).build();
        assertEquals(AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS.get(settings), Integer.valueOf(99));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS.get(settings), Integer.valueOf(1000));

        settings = Settings.builder().put("plugins.anomaly_detection.max_multi_entity_anomaly_detectors", 98).build();
        assertEquals(AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS.get(settings), Integer.valueOf(98));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS.get(settings), Integer.valueOf(10));

        settings = Settings.builder().put("plugins.anomaly_detection.max_anomaly_features", 97).build();
        assertEquals(AnomalyDetectorSettings.MAX_ANOMALY_FEATURES.get(settings), Integer.valueOf(97));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_ANOMALY_FEATURES.get(settings), Integer.valueOf(5));

        settings = Settings.builder().put("plugins.anomaly_detection.detection_interval", TimeValue.timeValueMinutes(96)).build();
        assertEquals(AnomalyDetectorSettings.DETECTION_INTERVAL.get(settings), TimeValue.timeValueMinutes(96));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.DETECTION_INTERVAL.get(settings), TimeValue.timeValueMinutes(10));

        settings = Settings.builder().put("plugins.anomaly_detection.detection_window_delay", TimeValue.timeValueMinutes(95)).build();
        assertEquals(AnomalyDetectorSettings.DETECTION_WINDOW_DELAY.get(settings), TimeValue.timeValueMinutes(95));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.DETECTION_WINDOW_DELAY.get(settings), TimeValue.timeValueMinutes(0));

        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.ad_result_history_rollover_period", TimeValue.timeValueHours(94))
            .build();
        assertEquals(AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings), TimeValue.timeValueHours(94));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings), TimeValue.timeValueHours(12));

        settings = Settings.builder().put("plugins.anomaly_detection.ad_result_history_max_docs_per_shard", 93).build();
        assertEquals(AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD.get(settings), Long.valueOf(93));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS.get(settings), Long.valueOf(250000000));

        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.ad_result_history_retention_period", TimeValue.timeValueDays(92))
            .build();
        assertEquals(AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD.get(settings), TimeValue.timeValueDays(92));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD.get(settings), TimeValue.timeValueDays(30));

        settings = Settings.builder().put("plugins.anomaly_detection.max_retry_for_unresponsive_node", 91).build();
        assertEquals(AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE.get(settings), Integer.valueOf(91));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE.get(settings), Integer.valueOf(5));

        settings = Settings.builder().put("plugins.anomaly_detection.cooldown_minutes", TimeValue.timeValueMinutes(90)).build();
        assertEquals(AnomalyDetectorSettings.COOLDOWN_MINUTES.get(settings), TimeValue.timeValueMinutes(90));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.COOLDOWN_MINUTES.get(settings), TimeValue.timeValueMinutes(5));

        settings = Settings.builder().put("plugins.anomaly_detection.backoff_minutes", TimeValue.timeValueMinutes(89)).build();
        assertEquals(AnomalyDetectorSettings.BACKOFF_MINUTES.get(settings), TimeValue.timeValueMinutes(89));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_MINUTES.get(settings), TimeValue.timeValueMinutes(15));

        settings = Settings.builder().put("plugins.anomaly_detection.backoff_initial_delay", TimeValue.timeValueMillis(88)).build();
        assertEquals(AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(settings), TimeValue.timeValueMillis(88));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(settings), TimeValue.timeValueMillis(1000));

        settings = Settings.builder().put("plugins.anomaly_detection.max_retry_for_backoff", 87).build();
        assertEquals(AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(settings), Integer.valueOf(87));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(settings), Integer.valueOf(3));

        settings = Settings.builder().put("plugins.anomaly_detection.max_retry_for_end_run_exception", 86).build();
        assertEquals(AnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION.get(settings), Integer.valueOf(86));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION.get(settings), Integer.valueOf(6));

        settings = Settings.builder().put("plugins.anomaly_detection.filter_by_backend_roles", true).build();
        assertEquals(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings), Boolean.valueOf(true));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings), Boolean.valueOf(false));

        settings = Settings.builder().put("plugins.anomaly_detection.model_max_size_percent", 0.3).build();
        assertEquals(AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(settings), Double.valueOf(0.3));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(settings), Double.valueOf(0.1));

        settings = Settings.builder().put("plugins.anomaly_detection.max_entities_per_query", 83).build();
        assertEquals(AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY.get(settings), Integer.valueOf(83));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY.get(settings), Integer.valueOf(1000));

        settings = Settings.builder().put("plugins.anomaly_detection.max_entities_for_preview", 82).build();
        assertEquals(AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW.get(settings), Integer.valueOf(82));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW.get(settings), Integer.valueOf(30));

        settings = Settings.builder().put("plugins.anomaly_detection.index_pressure_soft_limit", 81f).build();
        assertEquals(AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT.get(settings), Float.valueOf(81f));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT.get(settings), Float.valueOf(0.8f));

        settings = Settings.builder().put("plugins.anomaly_detection.max_primary_shards", 80).build();
        assertEquals(AnomalyDetectorSettings.MAX_PRIMARY_SHARDS.get(settings), Integer.valueOf(80));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_PRIMARY_SHARDS.get(settings), Integer.valueOf(10));

        settings = Settings.builder().put("plugins.anomaly_detection.max_cache_miss_handling_per_second", 79).build();
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND.get(settings), Integer.valueOf(100));

        settings = Settings.builder().put("plugins.anomaly_detection.max_batch_task_per_node", 78).build();
        assertEquals(AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE.get(settings), Integer.valueOf(78));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE.get(settings), Integer.valueOf(2));

        settings = Settings.builder().put("plugins.anomaly_detection.max_old_ad_task_docs_per_detector", 77).build();
        assertEquals(AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(settings), Integer.valueOf(77));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(settings), Integer.valueOf(10));

        settings = Settings.builder().put("plugins.anomaly_detection.batch_task_piece_size", 76).build();
        assertEquals(AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE.get(settings), Integer.valueOf(76));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE.get(settings), Integer.valueOf(1000));

        settings = Settings.builder().put("plugins.anomaly_detection.batch_task_piece_interval_seconds", 76).build();
        assertEquals(AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings), Integer.valueOf(76));
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings), Integer.valueOf(5));
    }

    public void testSettingsGetValueWithLegacyFallback() {
        Settings settings = Settings
            .builder()
            .put("opendistro.anomaly_detection.max_anomaly_detectors", 1)
            .put("opendistro.anomaly_detection.max_multi_entity_anomaly_detectors", 2)
            .put("opendistro.anomaly_detection.max_anomaly_features", 3)
            .put("opendistro.anomaly_detection.request_timeout", "4s")
            .put("opendistro.anomaly_detection.detection_interval", "5m")
            .put("opendistro.anomaly_detection.detection_window_delay", "6m")
            .put("opendistro.anomaly_detection.ad_result_history_rollover_period", "7h")
            .put("opendistro.anomaly_detection.ad_result_history_max_docs", 8L)
            .put("opendistro.anomaly_detection.ad_result_history_retention_period", "9d")
            .put("opendistro.anomaly_detection.max_retry_for_unresponsive_node", 10)
            .put("opendistro.anomaly_detection.cooldown_minutes", "11m")
            .put("opendistro.anomaly_detection.backoff_minutes", "12m")
            .put("opendistro.anomaly_detection.backoff_initial_delay", "13ms") //
            .put("opendistro.anomaly_detection.max_retry_for_backoff", 14)
            .put("opendistro.anomaly_detection.max_retry_for_end_run_exception", 15)
            .put("opendistro.anomaly_detection.filter_by_backend_roles", true)
            .put("opendistro.anomaly_detection.model_max_size_percent", 0.6D)
            .put("opendistro.anomaly_detection.max_entities_per_query", 18)
            .put("opendistro.anomaly_detection.max_entities_for_preview", 19)
            .put("opendistro.anomaly_detection.index_pressure_soft_limit", 20F)
            .put("opendistro.anomaly_detection.max_primary_shards", 21)
            .put("opendistro.anomaly_detection.max_cache_miss_handling_per_second", 22)
            .put("opendistro.anomaly_detection.max_batch_task_per_node", 23)
            .put("opendistro.anomaly_detection.max_old_ad_task_docs_per_detector", 24)
            .put("opendistro.anomaly_detection.batch_task_piece_size", 25)
            .put("opendistro.anomaly_detection.batch_task_piece_interval_seconds", 26)
            .build();

        assertEquals(AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS.get(settings), Integer.valueOf(1));
        assertEquals(AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS.get(settings), Integer.valueOf(2));
        assertEquals(AnomalyDetectorSettings.MAX_ANOMALY_FEATURES.get(settings), Integer.valueOf(3));
        assertEquals(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings), TimeValue.timeValueSeconds(4));
        assertEquals(AnomalyDetectorSettings.DETECTION_INTERVAL.get(settings), TimeValue.timeValueMinutes(5));
        assertEquals(AnomalyDetectorSettings.DETECTION_WINDOW_DELAY.get(settings), TimeValue.timeValueMinutes(6));
        assertEquals(AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD.get(settings), TimeValue.timeValueHours(7));
        // AD_RESULT_HISTORY_MAX_DOCS is removed in the new release
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS.get(settings), Long.valueOf(8L));
        assertEquals(AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD.get(settings), TimeValue.timeValueDays(9));
        assertEquals(AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE.get(settings), Integer.valueOf(10));
        assertEquals(AnomalyDetectorSettings.COOLDOWN_MINUTES.get(settings), TimeValue.timeValueMinutes(11));
        assertEquals(AnomalyDetectorSettings.BACKOFF_MINUTES.get(settings), TimeValue.timeValueMinutes(12));
        assertEquals(AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY.get(settings), TimeValue.timeValueMillis(13));
        assertEquals(AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF.get(settings), Integer.valueOf(14));
        assertEquals(AnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION.get(settings), Integer.valueOf(15));
        assertEquals(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings), Boolean.valueOf(true));
        assertEquals(AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(settings), Double.valueOf(0.6D));
        assertEquals(AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY.get(settings), Integer.valueOf(18));
        // MAX_ENTITIES_FOR_PREVIEW uses default instead of legacy fallback
        assertEquals(AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW.get(settings), Integer.valueOf(10));
        // INDEX_PRESSURE_SOFT_LIMIT uses default instead of legacy fallback
        assertEquals(AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT.get(settings), Float.valueOf(0.6F));
        assertEquals(AnomalyDetectorSettings.MAX_PRIMARY_SHARDS.get(settings), Integer.valueOf(21));
        // MAX_CACHE_MISS_HANDLING_PER_SECOND is removed in the new release
        assertEquals(LegacyOpenDistroAnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND.get(settings), Integer.valueOf(22));
        assertEquals(AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE.get(settings), Integer.valueOf(23));
        assertEquals(AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(settings), Integer.valueOf(24));
        assertEquals(AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE.get(settings), Integer.valueOf(25));
        assertEquals(AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings), Integer.valueOf(26));

        assertSettingDeprecationsAndWarnings(
            new Setting[] {
                LegacyOpenDistroAnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                LegacyOpenDistroAnomalyDetectorSettings.REQUEST_TIMEOUT,
                LegacyOpenDistroAnomalyDetectorSettings.DETECTION_INTERVAL,
                LegacyOpenDistroAnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                LegacyOpenDistroAnomalyDetectorSettings.COOLDOWN_MINUTES,
                LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_MINUTES,
                LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION,
                LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                LegacyOpenDistroAnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
                LegacyOpenDistroAnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
                LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
                LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE }
        );
    }
}
