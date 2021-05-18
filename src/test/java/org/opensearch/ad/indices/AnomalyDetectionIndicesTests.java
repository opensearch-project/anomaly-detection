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

package org.opensearch.ad.indices;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.junit.Before;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;

public class AnomalyDetectionIndicesTests extends OpenSearchIntegTestCase {

    private AnomalyDetectionIndices indices;
    private Settings settings;
    private DiscoveryNodeFilterer nodeFilter;

    // help register setting using AnomalyDetectorPlugin.getSettings. Otherwise, AnomalyDetectionIndices's constructor would fail due to
    // unregistered settings like AD_RESULT_HISTORY_MAX_DOCS.
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    @Before
    public void setup() {
        settings = Settings
            .builder()
            .put("opendistro.anomaly_detection.ad_result_history_rollover_period", TimeValue.timeValueHours(12))
            .put("opendistro.anomaly_detection.ad_result_history_max_age", TimeValue.timeValueHours(24))
            .put("opendistro.anomaly_detection.ad_result_history_max_docs", 10000L)
            .put("opendistro.anomaly_detection.request_timeout", TimeValue.timeValueSeconds(10))
            .build();

        nodeFilter = new DiscoveryNodeFilterer(clusterService());

        indices = new AnomalyDetectionIndices(client(), clusterService(), client().threadPool(), settings, nodeFilter);
    }

    public void testAnomalyDetectorIndexNotExists() {
        boolean exists = indices.doesAnomalyDetectorIndexExist();
        assertFalse(exists);
    }

    public void testLegacyOpendistoAnomalyDetectorIndexNotExists() {
        assertFalse(indices.exists(AnomalyDetector.LEGACY_OPENDISTRO_ANOMALY_DETECTORS_INDEX));
    }

    public void testAnomalyDetectorIndexExists() throws IOException {
        indices.initAnomalyDetectorIndexIfAbsent(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);
    }

    public void testAnomalyDetectorIndexExistsAndNotRecreate() throws IOException {
        indices
            .initAnomalyDetectorIndexIfAbsent(
                TestHelpers
                    .createActionListener(
                        response -> response.isAcknowledged(),
                        failure -> { throw new RuntimeException("should not recreate index"); }
                    )
            );
        TestHelpers.waitForIndexCreationToComplete(client(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);
        if (client().admin().indices().prepareExists(AnomalyDetector.ANOMALY_DETECTORS_INDEX).get().isExists()) {
            indices
                .initAnomalyDetectorIndexIfAbsent(
                    TestHelpers
                        .createActionListener(
                            response -> {
                                throw new RuntimeException("should not recreate index " + AnomalyDetector.ANOMALY_DETECTORS_INDEX);
                            },
                            failure -> {
                                throw new RuntimeException("should not recreate index " + AnomalyDetector.ANOMALY_DETECTORS_INDEX);
                            }
                        )
                );
        }
    }

    public void testLegacyOpendisroAnomalyDetectorIndexExistsAndCreateAlias() throws IOException {
        // create a legacy index
        indices
            .initAnomalyDetectorIndex(
                TestHelpers
                    .createActionListener(
                        response -> response.isAcknowledged(),
                        failure -> { throw new RuntimeException("should not recreate index"); }
                    ),
                AnomalyDetector.LEGACY_OPENDISTRO_ANOMALY_DETECTORS_INDEX
            );
        TestHelpers.waitForIndexCreationToComplete(client(), AnomalyDetector.LEGACY_OPENDISTRO_ANOMALY_DETECTORS_INDEX);
        assertTrue(indices.exists(AnomalyDetector.LEGACY_OPENDISTRO_ANOMALY_DETECTORS_INDEX));
        // create an alias
        indices
            .initLegacyOpendistroAnomalyDetectorAlias(
                TestHelpers
                    .createResponseListener(
                        response -> response.isAcknowledged(),
                        failure -> {
                            throw new RuntimeException("should not recreate alias " + AnomalyDetector.ANOMALY_DETECTORS_INDEX);
                        }
                    )
            );
        TestHelpers.waitForIndexCreationToComplete(client(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);
        assertFalse(indices.exists(AnomalyDetector.ANOMALY_DETECTORS_INDEX));
        assertTrue(indices.exists(AnomalyDetector.LEGACY_OPENDISTRO_ANOMALY_DETECTORS_INDEX));
        assertTrue(indices.aliasExists(AnomalyDetector.ANOMALY_DETECTORS_INDEX));
    }    

    public void testAnomalyResultIndexNotExists() {
        boolean exists = indices.doesAnomalyResultIndexExist();
        assertFalse(exists);
    }

    public void testAnomalyResultIndexExists() throws IOException {
        indices.initAnomalyResultIndexIfAbsent(TestHelpers.createActionListener(response -> {
            boolean acknowledged = response.isAcknowledged();
            assertTrue(acknowledged);
        }, failure -> { throw new RuntimeException("should not recreate index"); }));
        TestHelpers.waitForIndexCreationToComplete(client(), CommonName.ANOMALY_RESULT_INDEX_ALIAS);
    }

    public void testAnomalyResultIndexExistsAndNotRecreate() throws IOException {
        indices
            .initAnomalyResultIndexIfAbsent(
                TestHelpers
                    .createActionListener(
                        response -> logger.info("Acknowledged: " + response.isAcknowledged()),
                        failure -> { throw new RuntimeException("should not recreate index"); }
                    )
            );
        TestHelpers.waitForIndexCreationToComplete(client(), CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        if (client().admin().indices().prepareExists(CommonName.ANOMALY_RESULT_INDEX_ALIAS).get().isExists()) {
            indices
                .initAnomalyResultIndexIfAbsent(
                    TestHelpers
                        .createActionListener(
                            response -> {
                                throw new RuntimeException("should not recreate index " + CommonName.ANOMALY_RESULT_INDEX_ALIAS);
                            },
                            failure -> {
                                throw new RuntimeException("should not recreate index " + CommonName.ANOMALY_RESULT_INDEX_ALIAS, failure);
                            }
                        )
                );
        }
    }

    private void createRandomDetector(String indexName) throws IOException {
        // creates a random anomaly detector and indexes it
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        detector.toXContent(xContentBuilder, RestHandlerUtils.XCONTENT_WITH_TYPE);

        IndexResponse indexResponse = client().index(new IndexRequest(indexName).source(xContentBuilder)).actionGet();
        assertEquals("Doc was not created", RestStatus.CREATED, indexResponse.status());
    }

    public void testGetDetectionStateIndexMapping() throws IOException {
        String detectorIndexMappings = AnomalyDetectionIndices.getAnomalyDetectorMappings();
        detectorIndexMappings = detectorIndexMappings
            .substring(detectorIndexMappings.indexOf("\"properties\""), detectorIndexMappings.lastIndexOf("}"));
        String detectionStateIndexMapping = AnomalyDetectionIndices.getDetectionStateMappings();
        assertTrue(detectionStateIndexMapping.contains(detectorIndexMappings));
    }
}
