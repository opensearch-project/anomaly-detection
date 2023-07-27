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
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyResultTests;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class AbstractProfileRunnerTests extends AbstractTimeSeriesTest {
    protected enum DetectorStatus {
        INDEX_NOT_EXIST,
        NO_DOC,
        EXIST
    }

    protected enum JobStatus {
        INDEX_NOT_EXIT,
        DISABLED,
        ENABLED
    }

    protected enum ErrorResultStatus {
        INDEX_NOT_EXIT,
        NO_ERROR,
        SHINGLE_ERROR,
        STOPPED_ERROR,
        NULL_POINTER_EXCEPTION
    }

    protected AnomalyDetectorProfileRunner runner;
    protected Client client;
    protected SecurityClientUtil clientUtil;
    protected DiscoveryNodeFilterer nodeFilter;
    protected AnomalyDetector detector;
    protected ClusterService clusterService;
    protected TransportService transportService;
    protected ADTaskManager adTaskManager;

    protected static Set<DetectorProfileName> stateOnly;
    protected static Set<DetectorProfileName> stateNError;
    protected static Set<DetectorProfileName> modelProfile;
    protected static Set<DetectorProfileName> stateInitProgress;
    protected static Set<DetectorProfileName> totalInitProgress;
    protected static Set<DetectorProfileName> initProgressErrorProfile;

    protected static String noFullShingleError = "No full shingle in current detection window";
    protected static String stoppedError =
        "Stopped detector as job failed consecutively for more than 3 times: Having trouble querying data."
            + " Maybe all of your features have been disabled.";

    protected static String clusterName;
    protected static DiscoveryNode discoveryNode1;

    protected int requiredSamples;
    protected int neededSamples;

    // profile model related
    protected String node1;
    protected String nodeName1;

    protected String node2;
    protected String nodeName2;
    protected DiscoveryNode discoveryNode2;

    protected long modelSize;
    protected String model1Id;
    protected String model0Id;

    protected int shingleSize;

    protected int detectorIntervalMin;
    protected GetResponse detectorGetReponse;
    protected String messaingExceptionError = "blah";

    @BeforeClass
    public static void setUpOnce() {
        stateOnly = new HashSet<DetectorProfileName>();
        stateOnly.add(DetectorProfileName.STATE);
        stateNError = new HashSet<DetectorProfileName>();
        stateNError.add(DetectorProfileName.ERROR);
        stateNError.add(DetectorProfileName.STATE);
        stateInitProgress = new HashSet<DetectorProfileName>();
        stateInitProgress.add(DetectorProfileName.INIT_PROGRESS);
        stateInitProgress.add(DetectorProfileName.STATE);
        modelProfile = new HashSet<DetectorProfileName>(
            Arrays
                .asList(
                    DetectorProfileName.SHINGLE_SIZE,
                    DetectorProfileName.MODELS,
                    DetectorProfileName.COORDINATING_NODE,
                    DetectorProfileName.TOTAL_SIZE_IN_BYTES
                )
        );
        totalInitProgress = new HashSet<DetectorProfileName>(
            Arrays.asList(DetectorProfileName.TOTAL_ENTITIES, DetectorProfileName.INIT_PROGRESS)
        );
        initProgressErrorProfile = new HashSet<DetectorProfileName>(
            Arrays.asList(DetectorProfileName.INIT_PROGRESS, DetectorProfileName.ERROR)
        );
        clusterName = "test-cluster-name";
        discoveryNode1 = new DiscoveryNode(
            "nodeName1",
            "node1",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        Clock clock = mock(Clock.class);

        nodeFilter = mock(DiscoveryNodeFilterer.class);
        clusterService = mock(ClusterService.class);
        adTaskManager = mock(ADTaskManager.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test cluster")).build());

        requiredSamples = 128;
        neededSamples = 5;

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<ADTask>> function = (Consumer<Optional<ADTask>>) args[2];
            function.accept(Optional.of(TestHelpers.randomAdTask()));
            return null;
        }).when(adTaskManager).getAndExecuteOnLatestDetectorLevelTask(any(), any(), any(), any(), anyBoolean(), any());

        detectorIntervalMin = 3;
        detectorGetReponse = mock(GetResponse.class);

    }
}
