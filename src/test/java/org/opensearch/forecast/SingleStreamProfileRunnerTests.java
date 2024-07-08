/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
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
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.model.ForecasterProfile;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastProfileAction;
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

public class SingleStreamProfileRunnerTests extends AbstractTimeSeriesTest {
    private ForecastProfileRunner runner;
    private Client client;
    private SecurityClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;
    private int requiredSamples;
    private Forecaster forecaster;
    private String forecasterId;
    private Set<ProfileName> stateNError;
    private String node1;
    private String nodeName1;
    private DiscoveryNode discoveryNode1;

    private String node2;
    private String nodeName2;
    private DiscoveryNode discoveryNode2;

    private long modelSize;
    private String model1Id;
    private String model0Id;

    private Job job;
    private TransportService transportService;
    private ForecastTaskManager forecastTaskManager;
    private ForecastTaskProfileRunner taskProfileRunner;
    private ForecastTask task;

    enum InittedEverResultStatus {
        INITTED,
        NOT_INITTED,
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(SingleStreamProfileRunnerTests.class.getSimpleName());
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
        taskProfileRunner = mock(ForecastTaskProfileRunner.class);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        requiredSamples = 128;

        forecasterId = "A69pa3UBHuCbh-emo9oR";
        forecaster = TestHelpers.ForecasterBuilder.newInstance().setConfigId(forecasterId).setCategoryFields(null).build();
        job = TestHelpers.randomJob(true);
        forecastTaskManager = mock(ForecastTaskManager.class);
        transportService = mock(TransportService.class);
        task = TestHelpers.ForecastTaskBuilder.newInstance().build();
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<ForecastTask>> function = (Consumer<Optional<ForecastTask>>) args[2];

            function.accept(Optional.of(task));
            return null;
        }).when(forecastTaskManager).getAndExecuteOnLatestConfigLevelTask(any(), any(), any(), any(), anyBoolean(), any());
        runner = new ForecastProfileRunner(
            client,
            clientUtil,
            xContentRegistry(),
            nodeFilter,
            requiredSamples,
            transportService,
            forecastTaskManager,
            taskProfileRunner
        );

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(CommonName.CONFIG_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(forecaster, forecaster.getId(), CommonName.CONFIG_INDEX));
            } else if (indexName.equals(ForecastIndex.STATE.getIndexName())) {
                listener.onResponse(TestHelpers.createGetResponse(task, forecaster.getId(), ForecastIndex.STATE.getIndexName()));
            } else if (indexName.equals(CommonName.JOB_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(job, forecaster.getId(), CommonName.JOB_INDEX));
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
        }).when(client).execute(any(ForecastProfileAction.class), any(), any());

    }

    @SuppressWarnings("unchecked")
    private void setUpClientSearch(InittedEverResultStatus inittedEverResultStatus) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchRequest request = (SearchRequest) args[0];
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            ForecastResult result = null;
            if (request.source().query().toString().contains(ForecastResult.VALUE_FIELD)) {
                switch (inittedEverResultStatus) {
                    case INITTED:
                        result = TestHelpers.ForecastResultBuilder.newInstance().build();
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

        ConfigProfile expectedProfile = new ForecasterProfile.Builder().state(ConfigState.INIT).build();
        runner.profile(forecasterId, ActionListener.wrap(response -> {
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

        ConfigProfile expectedProfile = new ForecasterProfile.Builder().state(ConfigState.RUNNING).build();
        runner.profile(forecasterId, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    /**
     * Although profile action results indicate not initted, we trust what result index tells us
     * @throws InterruptedException if CountDownLatch is interrupted while waiting
     */
    public void testResultIndexFinalTruth() throws InterruptedException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.NOT_INITTED);
        setUpClientSearch(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        ConfigProfile expectedProfile = new ForecasterProfile.Builder().state(ConfigState.RUNNING).build();
        runner.profile(forecasterId, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    /**
     * Although profile action results indicate not initted, we trust what result index tells us
     * @throws InterruptedException if CountDownLatch is interrupted while waiting
     * @throws IOException
     */
    public void testCustomResultIndexFinalTruth() throws InterruptedException, IOException {
        setUpClientExecuteProfileAction(InittedEverResultStatus.NOT_INITTED);
        setUpClientSearch(InittedEverResultStatus.INITTED);

        forecaster = TestHelpers.ForecasterBuilder
            .newInstance()
            .setConfigId(forecasterId)
            .setCategoryFields(null)
            .setCustomResultIndex(ForecastCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test-index")
            .build();
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(CommonName.CONFIG_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(forecaster, forecaster.getId(), CommonName.CONFIG_INDEX));
            } else if (indexName.equals(ForecastIndex.STATE.getIndexName())) {
                listener.onResponse(TestHelpers.createGetResponse(task, forecaster.getId(), ForecastIndex.STATE.getIndexName()));
            } else if (indexName.equals(CommonName.JOB_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(job, forecaster.getId(), CommonName.JOB_INDEX));
            }

            return null;
        }).when(client).get(any(), any());

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        ConfigProfile expectedProfile = new ForecasterProfile.Builder().state(ConfigState.RUNNING).build();
        runner.profile(forecasterId, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
