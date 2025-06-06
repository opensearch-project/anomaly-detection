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

package org.opensearch.ad;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.AnomalyResultTests;
import org.opensearch.ad.util.*;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.ConfigProfile;
import org.opensearch.timeseries.model.ConfigState;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.transport.ProfileNodeResponse;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class MultiEntityProfileRunnerTests extends AbstractTimeSeriesTest {
    private AnomalyDetectorProfileRunner runner;
    private Client client;
    private SecurityClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;
    private int requiredSamples;
    private AnomalyDetector detector;
    private String detectorId;
    private Set<ProfileName> stateNError;
    private DetectorInternalState.Builder result;
    private String node1;
    private String nodeName1;
    private DiscoveryNode discoveryNode1;

    private String node2;
    private String nodeName2;
    private DiscoveryNode discoveryNode2;

    private long modelSize;
    private String model1Id;
    private String model0Id;

    private int shingleSize;
    private Job job;
    private TransportService transportService;
    private ADTaskManager adTaskManager;
    private ADTaskProfileRunner taskProfileRunner;

    enum InittedEverResultStatus {
        INITTED,
        NOT_INITTED,
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @SuppressWarnings("unchecked")
    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        taskProfileRunner = mock(ADTaskProfileRunner.class);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        requiredSamples = 128;

        detectorId = "A69pa3UBHuCbh-emo9oR";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a"));
        result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now());
        job = TestHelpers.randomJob(true);
        adTaskManager = mock(ADTaskManager.class);
        transportService = mock(TransportService.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<ADTask>> function = (Consumer<Optional<ADTask>>) args[2];

            function.accept(Optional.of(TestHelpers.randomAdTask()));
            return null;
        }).when(adTaskManager).getAndExecuteOnLatestConfigLevelTask(any(), any(), any(), any(), anyBoolean(), any());
        runner = new AnomalyDetectorProfileRunner(
            client,
            clientUtil,
            xContentRegistry(),
            nodeFilter,
            requiredSamples,
            transportService,
            adTaskManager,
            taskProfileRunner
        );

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(ADCommonName.CONFIG_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(detector, detector.getId(), ADCommonName.CONFIG_INDEX));
            } else if (indexName.equals(ADCommonName.DETECTION_STATE_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(result.build(), detector.getId(), ADCommonName.DETECTION_STATE_INDEX));
            } else if (indexName.equals(CommonName.JOB_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(job, detector.getId(), CommonName.JOB_INDEX));
            }

            return null;
        }).when(client).get(any(), any());

        stateNError = new HashSet<ProfileName>();
        stateNError.add(ProfileName.ERROR);
        stateNError.add(ProfileName.STATE);
    }

    @SuppressWarnings("unchecked")
    private void setUpClientExecuteProfileAction(InittedEverResultStatus initted) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<ProfileResponse> listener = (ActionListener<ProfileResponse>) args[2];

            node1 = "node1";
            nodeName1 = "nodename1";
            discoveryNode1 = new DiscoveryNode(
                nodeName1,
                node1,
                new TransportAddress(TransportAddress.META_ADDRESS, 9300),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            );

            node2 = "node2";
            nodeName2 = "nodename2";
            discoveryNode2 = new DiscoveryNode(
                nodeName2,
                node2,
                new TransportAddress(TransportAddress.META_ADDRESS, 9301),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            );

            modelSize = 712480L;
            model1Id = "A69pa3UBHuCbh-emo9oR_entity_host1";
            model0Id = "A69pa3UBHuCbh-emo9oR_entity_host0";

            shingleSize = -1;

            String clusterName = "test-cluster-name";

            Map<String, Long> modelSizeMap1 = new HashMap<String, Long>() {
                {
                    put(model1Id, modelSize);
                }
            };

            Map<String, Long> modelSizeMap2 = new HashMap<String, Long>() {
                {
                    put(model0Id, modelSize);
                }
            };

            // one model in each node; all fully initialized
            long updates = requiredSamples - 1;
            if (InittedEverResultStatus.INITTED == initted) {
                updates = requiredSamples + 1;
            }
            ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(
                discoveryNode1,
                modelSizeMap1,
                1L,
                updates,
                new ArrayList<>(),
                modelSizeMap1.size(),
                false
            );
            ProfileNodeResponse profileNodeResponse2 = new ProfileNodeResponse(
                discoveryNode2,
                modelSizeMap2,
                1L,
                updates,
                new ArrayList<>(),
                modelSizeMap2.size(),
                false
            );
            List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1, profileNodeResponse2);
            List<FailedNodeException> failures = Collections.emptyList();
            ProfileResponse profileResponse = new ProfileResponse(new ClusterName(clusterName), profileNodeResponses, failures);

            listener.onResponse(profileResponse);

            return null;
        }).when(client).execute(any(ADProfileAction.class), any(), any());

    }

    @SuppressWarnings("unchecked")
    private void setUpClientSearch(InittedEverResultStatus inittedEverResultStatus) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchRequest request = (SearchRequest) args[0];
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            AnomalyResult result = null;
            if (request.source().query().toString().contains(AnomalyResult.ANOMALY_SCORE_FIELD)) {
                switch (inittedEverResultStatus) {
                    case INITTED:
                        result = TestHelpers.randomAnomalyDetectResult(0.87);
                        listener.onResponse(TestHelpers.createSearchResponse(result));
                        break;
                    case NOT_INITTED:
                        listener.onResponse(TestHelpers.createEmptySearchResponse());
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            }

            return null;
        }).when(client).search(any(), any());
    }

    public void testInit() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.NOT_INITTED);
        setUpClientSearch(InittedEverResultStatus.NOT_INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        ConfigProfile expectedProfile = new DetectorProfile.Builder().state(ConfigState.INIT).build();
        runner.profile(detectorId, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testRunning() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.INITTED);
        setUpClientSearch(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        ConfigProfile expectedProfile = new DetectorProfile.Builder().state(ConfigState.RUNNING).build();
        runner.profile(detectorId, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    /**
     * Although profile action results indicate initted, we trust what result index tells us
     * @throws InterruptedException if CountDownLatch is interrupted while waiting
     */
    public void testResultIndexFinalTruth() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.NOT_INITTED);
        setUpClientSearch(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        ConfigProfile expectedProfile = new DetectorProfile.Builder().state(ConfigState.RUNNING).build();
        runner.profile(detectorId, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
