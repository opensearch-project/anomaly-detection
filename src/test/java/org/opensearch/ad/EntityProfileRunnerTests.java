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

package org.opensearch.ad;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.EntityProfileName;
import org.opensearch.ad.model.EntityState;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.ad.model.ModelProfileOnNode;
import org.opensearch.ad.transport.EntityProfileAction;
import org.opensearch.ad.transport.EntityProfileResponse;
import org.opensearch.client.Client;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalMax;

public class EntityProfileRunnerTests extends AbstractADTest {
    private AnomalyDetector detector;
    private int detectorIntervalMin;
    private Client client;
    private EntityProfileRunner runner;
    private Set<EntityProfileName> state;
    private Set<EntityProfileName> initNInfo;
    private Set<EntityProfileName> model;
    private String detectorId;
    private String entityValue;
    private int requiredSamples;
    private AnomalyDetectorJob job;

    private int smallUpdates;
    private String categoryField;
    private long latestSampleTimestamp;
    private long latestActiveTimestamp;
    private Boolean isActive;
    private String modelId;
    private long modelSize;
    private String nodeId;
    private Entity entity;

    enum InittedEverResultStatus {
        UNKNOWN,
        INITTED,
        NOT_INITTED,
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        detectorIntervalMin = 3;

        state = new HashSet<EntityProfileName>();
        state.add(EntityProfileName.STATE);

        initNInfo = new HashSet<EntityProfileName>();
        initNInfo.add(EntityProfileName.INIT_PROGRESS);
        initNInfo.add(EntityProfileName.ENTITY_INFO);

        model = new HashSet<EntityProfileName>();
        model.add(EntityProfileName.MODELS);

        detectorId = "A69pa3UBHuCbh-emo9oR";
        entityValue = "app-0";

        requiredSamples = 128;
        client = mock(Client.class);

        runner = new EntityProfileRunner(client, xContentRegistry(), requiredSamples);

        categoryField = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(categoryField));
        job = TestHelpers.randomAnomalyDetectorJob(true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(ANOMALY_DETECTORS_INDEX)) {
                listener
                    .onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            } else if (indexName.equals(ANOMALY_DETECTOR_JOB_INDEX)) {
                listener
                    .onResponse(
                        TestHelpers.createGetResponse(job, detector.getDetectorId(), AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
                    );
            }

            return null;
        }).when(client).get(any(), any());

        entity = Entity.createSingleAttributeEntity(detectorId, categoryField, entityValue);
    }

    @SuppressWarnings("unchecked")
    private void setUpSearch() {
        latestSampleTimestamp = 1_603_989_830_158L;
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchRequest request = (SearchRequest) args[0];
            String indexName = request.indices()[0];
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            if (indexName.equals(CommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
                InternalMax maxAgg = new InternalMax(CommonName.AGG_NAME_MAX_TIME, latestSampleTimestamp, DocValueFormat.RAW, emptyMap());
                InternalAggregations internalAggregations = InternalAggregations.from(Collections.singletonList(maxAgg));

                SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);
                SearchResponseSections searchSections = new SearchResponseSections(hits, internalAggregations, null, false, false, null, 1);

                SearchResponse searchResponse = new SearchResponse(
                    searchSections,
                    null,
                    1,
                    1,
                    0,
                    30,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );

                listener.onResponse(searchResponse);
            }
            return null;
        }).when(client).search(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void setUpExecuteEntityProfileAction(InittedEverResultStatus initted) {
        smallUpdates = 1;
        latestActiveTimestamp = 1603999189758L;
        isActive = Boolean.TRUE;
        modelId = "T4c3dXUBj-2IZN7itix__entity_" + entityValue;
        modelSize = 712480L;
        nodeId = "g6pmr547QR-CfpEvO67M4g";
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<EntityProfileResponse> listener = (ActionListener<EntityProfileResponse>) args[2];

            EntityProfileResponse.Builder profileResponseBuilder = new EntityProfileResponse.Builder();
            if (InittedEverResultStatus.UNKNOWN == initted) {
                profileResponseBuilder.setTotalUpdates(0L);
            } else if (InittedEverResultStatus.NOT_INITTED == initted) {
                profileResponseBuilder.setTotalUpdates(smallUpdates);
                profileResponseBuilder.setLastActiveMs(latestActiveTimestamp);
                profileResponseBuilder.setActive(isActive);
            } else {
                profileResponseBuilder.setTotalUpdates(requiredSamples + 1);
                ModelProfileOnNode model = new ModelProfileOnNode(nodeId, new ModelProfile(modelId, entity, modelSize));
                profileResponseBuilder.setModelProfile(model);
            }

            listener.onResponse(profileResponseBuilder.build());

            return null;
        }).when(client).execute(any(EntityProfileAction.class), any(), any());
    }

    public void stateTestTemplate(InittedEverResultStatus returnedState, EntityState expectedState) throws InterruptedException {
        setUpExecuteEntityProfileAction(returnedState);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entity, state, ActionListener.wrap(response -> {
            assertEquals(expectedState, response.getState());
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testEmptyProfile() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entity, new HashSet<>(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(CommonErrorMessages.EMPTY_PROFILES_COLLECT));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
