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

package com.amazon.opendistroforelasticsearch.ad;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
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
import org.opensearch.client.Client;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalMax;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.EntityProfile;
import com.amazon.opendistroforelasticsearch.ad.model.EntityProfileName;
import com.amazon.opendistroforelasticsearch.ad.model.EntityState;
import com.amazon.opendistroforelasticsearch.ad.model.InitProgressProfile;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.model.ModelProfile;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityProfileResponse;

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
        modelId = "T4c3dXUBj-2IZN7itix__entity_app_6";
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
                ModelProfile model = new ModelProfile(modelId, modelSize, nodeId);
                profileResponseBuilder.setModelProfile(model);
            }

            listener.onResponse(profileResponseBuilder.build());

            return null;
        }).when(client).execute(any(EntityProfileAction.class), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testNotMultiEntityDetector() throws IOException, InterruptedException {
        detector = TestHelpers.randomAnomalyDetectorWithInterval(new IntervalTimeConfiguration(detectorIntervalMin, ChronoUnit.MINUTES));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(ANOMALY_DETECTORS_INDEX)) {
                listener
                    .onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            }

            return null;
        }).when(client).get(any(), any());

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entityValue, state, ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(EntityProfileRunner.NOT_HC_DETECTOR_ERR_MSG));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void stateTestTemplate(InittedEverResultStatus returnedState, EntityState expectedState) throws InterruptedException {
        setUpExecuteEntityProfileAction(returnedState);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entityValue, state, ActionListener.wrap(response -> {
            assertEquals(expectedState, response.getState());
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testUnknownState() throws InterruptedException {
        stateTestTemplate(InittedEverResultStatus.UNKNOWN, EntityState.UNKNOWN);
    }

    public void testInitState() throws InterruptedException {
        stateTestTemplate(InittedEverResultStatus.NOT_INITTED, EntityState.INIT);
    }

    public void testRunningState() throws InterruptedException {
        stateTestTemplate(InittedEverResultStatus.INITTED, EntityState.RUNNING);
    }

    public void testInitNInfo() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.NOT_INITTED);
        setUpSearch();

        EntityProfile.Builder expectedProfile = new EntityProfile.Builder(categoryField, entityValue);

        // 1 / 128 rounded to 1%
        int neededSamples = requiredSamples - smallUpdates;
        InitProgressProfile profile = new InitProgressProfile(
            "1%",
            neededSamples * detector.getDetectorIntervalInSeconds() / 60,
            neededSamples
        );
        expectedProfile.initProgress(profile);
        expectedProfile.isActive(isActive);
        expectedProfile.lastActiveTimestampMs(latestActiveTimestamp);
        expectedProfile.lastSampleTimestampMs(latestSampleTimestamp);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entityValue, initNInfo, ActionListener.wrap(response -> {
            assertEquals(expectedProfile.build(), response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error("Unexpected error", exception);
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testEmptyProfile() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entityValue, new HashSet<>(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(CommonErrorMessages.EMPTY_PROFILES_COLLECT));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testModel() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.INITTED);

        EntityProfile.Builder expectedProfile = new EntityProfile.Builder(categoryField, entityValue);
        ModelProfile modelProfile = new ModelProfile(modelId, modelSize, nodeId);
        expectedProfile.modelProfile(modelProfile);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entityValue, model, ActionListener.wrap(response -> {
            assertEquals(expectedProfile.build(), response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    public void testJobIndexNotFound() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(ANOMALY_DETECTORS_INDEX)) {
                listener
                    .onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            } else if (indexName.equals(ANOMALY_DETECTOR_JOB_INDEX)) {
                listener.onFailure(new IndexNotFoundException(ANOMALY_DETECTOR_JOB_INDEX));
            }

            return null;
        }).when(client).get(any(), any());

        EntityProfile expectedProfile = new EntityProfile.Builder(categoryField, entityValue).build();

        runner.profile(detectorId, entityValue, initNInfo, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error("Unexpected error", exception);
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
