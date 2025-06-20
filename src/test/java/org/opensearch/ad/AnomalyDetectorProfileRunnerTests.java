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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.RCFPollingAction;
import org.opensearch.ad.transport.RCFPollingResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.ConfigProfile;
import org.opensearch.timeseries.model.ConfigState;
import org.opensearch.timeseries.model.InitProgressProfile;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.ModelProfileOnNode;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.transport.ProfileNodeResponse;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.RemoteTransportException;

public class AnomalyDetectorProfileRunnerTests extends AbstractProfileRunnerTests {
    enum RCFPollingStatus {
        INIT_NOT_EXIT,
        REMOTE_INIT_NOT_EXIT,
        INDEX_NOT_FOUND,
        REMOTE_INDEX_NOT_FOUND,
        INIT_DONE,
        EMPTY,
        EXCEPTION,
        INITTING
    }

    private Instant jobEnabledTime = Instant.now().minus(1, ChronoUnit.DAYS);

    /**
     * Convenience methods for single-stream detector profile tests set up
     * @param detectorStatus Detector config status
     * @param jobStatus Detector job status
     * @param rcfPollingStatus RCF polling result status
     * @param errorResultStatus Error result status
     * @throws IOException when failing the getting request
     */
    @SuppressWarnings("unchecked")
    private void setUpClientGet(
        DetectorStatus detectorStatus,
        JobStatus jobStatus,
        RCFPollingStatus rcfPollingStatus,
        ErrorResultStatus errorResultStatus
    ) throws IOException {
        detector = TestHelpers.randomAnomalyDetectorWithInterval(new IntervalTimeConfiguration(detectorIntervalMin, ChronoUnit.MINUTES));
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(anyString(), eq(AnalysisType.AD), any(boolean.class), any(ActionListener.class));
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);
        oldRunner = new OldAnomalyDetectorProfileRunner(
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

            if (request.index().equals(ADCommonName.CONFIG_INDEX)) {
                switch (detectorStatus) {
                    case EXIST:
                        listener.onResponse(TestHelpers.createGetResponse(detector, detector.getId(), ADCommonName.CONFIG_INDEX));
                        break;
                    case INDEX_NOT_EXIST:
                        listener.onFailure(new IndexNotFoundException(ADCommonName.CONFIG_INDEX));
                        break;
                    case NO_DOC:
                        when(detectorGetReponse.isExists()).thenReturn(false);
                        listener.onResponse(detectorGetReponse);
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            } else if (request.index().equals(CommonName.JOB_INDEX)) {
                Job job = null;
                switch (jobStatus) {
                    case INDEX_NOT_EXIT:
                        listener.onFailure(new IndexNotFoundException(CommonName.JOB_INDEX));
                        break;
                    case DISABLED:
                        job = TestHelpers.randomJob(false, jobEnabledTime, null);
                        listener.onResponse(TestHelpers.createGetResponse(job, detector.getId(), CommonName.JOB_INDEX));
                        break;
                    case ENABLED:
                        job = TestHelpers.randomJob(true, jobEnabledTime, null);
                        listener.onResponse(TestHelpers.createGetResponse(job, detector.getId(), CommonName.JOB_INDEX));
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
            } else {
                if (errorResultStatus == ErrorResultStatus.INDEX_NOT_EXIT) {
                    listener.onFailure(new IndexNotFoundException(ADCommonName.DETECTION_STATE_INDEX));
                    return null;
                }
                DetectorInternalState.Builder result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now());

                String error = getError(errorResultStatus);
                if (error != null) {
                    result.error(error);
                }
                listener.onResponse(TestHelpers.createGetResponse(result.build(), detector.getId(), ADCommonName.DETECTION_STATE_INDEX));

            }

            return null;
        }).when(client).get(any(), any());

        setUpClientExecuteRCFPollingAction(rcfPollingStatus);
    }

    private String getError(ErrorResultStatus errorResultStatus) {
        switch (errorResultStatus) {
            case NO_ERROR:
                break;
            case SHINGLE_ERROR:
                return noFullShingleError;
            case STOPPED_ERROR:
                return stoppedError;
            default:
                assertTrue("should not reach here", false);
                break;
        }
        return null;
    }

    public void testDetectorNotExist() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.INDEX_NOT_EXIST, JobStatus.INDEX_NOT_EXIT, RCFPollingStatus.EMPTY, ErrorResultStatus.NO_ERROR);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile("x123", ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(CommonMessages.FAIL_TO_FIND_CONFIG_MSG));
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testDisabledJobIndexTemplate(JobStatus status) throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, status, RCFPollingStatus.EMPTY, ErrorResultStatus.NO_ERROR);
        ConfigProfile expectedProfile = new DetectorProfile.Builder().state(ConfigState.DISABLED).build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile(detector.getId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateOnly);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testNoJobIndex() throws IOException, InterruptedException {
        testDisabledJobIndexTemplate(JobStatus.INDEX_NOT_EXIT);
    }

    public void testJobDisabled() throws IOException, InterruptedException {
        testDisabledJobIndexTemplate(JobStatus.DISABLED);
    }

    public void testInitOrRunningStateTemplate(RCFPollingStatus status, ConfigState expectedState) throws IOException,
        InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, status, ErrorResultStatus.NO_ERROR);
        DetectorProfile expectedProfile = new DetectorProfile.Builder().state(expectedState).build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile(detector.getId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            logger.error(exception);
            for (StackTraceElement ste : exception.getStackTrace()) {
                logger.info(ste);
            }
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateOnly);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testResultNotExist() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.INIT_NOT_EXIT, ConfigState.INIT);
    }

    public void testRemoteResultNotExist() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.REMOTE_INIT_NOT_EXIT, ConfigState.INIT);
    }

    public void testCheckpointIndexNotExist() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.INDEX_NOT_FOUND, ConfigState.INIT);
    }

    public void testRemoteCheckpointIndexNotExist() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.REMOTE_INDEX_NOT_FOUND, ConfigState.INIT);
    }

    public void testResultEmpty() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.EMPTY, ConfigState.INIT);
    }

    public void testResultGreaterThanZero() throws IOException, InterruptedException {
        testInitOrRunningStateTemplate(RCFPollingStatus.INIT_DONE, ConfigState.RUNNING);
    }

    @SuppressWarnings("unchecked")
    public void testErrorStateTemplate(
        RCFPollingStatus initStatus,
        ErrorResultStatus status,
        ConfigState state,
        String error,
        JobStatus jobStatus,
        Set<ProfileName> profilesToCollect
    ) throws IOException,
        InterruptedException {
        ADTask adTask = TestHelpers.randomAdTask();

        adTask.setError(getError(status));
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<ADTask>> function = (Consumer<Optional<ADTask>>) args[2];
            function.accept(Optional.of(adTask));
            return null;
        }).when(adTaskManager).getAndExecuteOnLatestConfigLevelTask(any(), any(), any(), any(), anyBoolean(), any());

        setUpClientExecuteRCFPollingAction(initStatus);
        setUpClientGet(DetectorStatus.EXIST, jobStatus, initStatus, status);
        DetectorProfile.Builder builder = new DetectorProfile.Builder();
        if (profilesToCollect.contains(ProfileName.STATE)) {
            builder.state(state);
        }
        if (profilesToCollect.contains(ProfileName.ERROR)) {
            builder.error(error);
        }
        DetectorProfile expectedProfile = builder.build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile(detector.getId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            logger.info(exception);
            for (StackTraceElement ste : exception.getStackTrace()) {
                logger.info(ste);
            }
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }), profilesToCollect);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testErrorStateTemplate(
        RCFPollingStatus initStatus,
        ErrorResultStatus status,
        ConfigState state,
        String error,
        JobStatus jobStatus
    ) throws IOException,
        InterruptedException {
        testErrorStateTemplate(initStatus, status, state, error, jobStatus, stateNError);
    }

    public void testRunningNoError() throws IOException, InterruptedException {
        testErrorStateTemplate(RCFPollingStatus.INIT_DONE, ErrorResultStatus.NO_ERROR, ConfigState.RUNNING, null, JobStatus.ENABLED);
    }

    public void testRunningWithError() throws IOException, InterruptedException {
        testErrorStateTemplate(
            RCFPollingStatus.INIT_DONE,
            ErrorResultStatus.SHINGLE_ERROR,
            ConfigState.RUNNING,
            noFullShingleError,
            JobStatus.ENABLED
        );
    }

    public void testDisabledForStateError() throws IOException, InterruptedException {
        testErrorStateTemplate(
            RCFPollingStatus.INITTING,
            ErrorResultStatus.STOPPED_ERROR,
            ConfigState.DISABLED,
            stoppedError,
            JobStatus.DISABLED
        );
    }

    public void testDisabledForStateInit() throws IOException, InterruptedException {
        testErrorStateTemplate(
            RCFPollingStatus.INITTING,
            ErrorResultStatus.STOPPED_ERROR,
            ConfigState.DISABLED,
            stoppedError,
            JobStatus.DISABLED,
            stateInitProgress
        );
    }

    public void testInitWithError() throws IOException, InterruptedException {
        testErrorStateTemplate(
            RCFPollingStatus.EMPTY,
            ErrorResultStatus.SHINGLE_ERROR,
            ConfigState.INIT,
            noFullShingleError,
            JobStatus.ENABLED
        );
    }

    @SuppressWarnings("unchecked")
    private void setUpClientExecuteProfileAction() {
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

            modelSize = 4456448L;
            model1Id = "Pl536HEBnXkDrah03glg_model_rcf_1";
            model0Id = "Pl536HEBnXkDrah03glg_model_rcf_0";

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

            ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(
                discoveryNode1,
                modelSizeMap1,
                0L,
                0L,
                new ArrayList<>(),
                modelSizeMap1.size(),
                true
            );
            ProfileNodeResponse profileNodeResponse2 = new ProfileNodeResponse(
                discoveryNode2,
                modelSizeMap2,
                0L,
                0L,
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
    private void setUpClientExecuteRCFPollingAction(RCFPollingStatus inittedEverResultStatus) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<RCFPollingResponse> listener = (ActionListener<RCFPollingResponse>) args[2];

            Exception cause = null;
            String detectorId = "123";
            if (inittedEverResultStatus == RCFPollingStatus.INIT_NOT_EXIT
                || inittedEverResultStatus == RCFPollingStatus.REMOTE_INIT_NOT_EXIT
                || inittedEverResultStatus == RCFPollingStatus.INDEX_NOT_FOUND
                || inittedEverResultStatus == RCFPollingStatus.REMOTE_INDEX_NOT_FOUND) {
                switch (inittedEverResultStatus) {
                    case INIT_NOT_EXIT:
                    case REMOTE_INIT_NOT_EXIT:
                        cause = new ResourceNotFoundException(detectorId, messaingExceptionError);
                        break;
                    case INDEX_NOT_FOUND:
                    case REMOTE_INDEX_NOT_FOUND:
                        cause = new IndexNotFoundException(detectorId, ADCommonName.CHECKPOINT_INDEX_NAME);
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }
                cause = new TimeSeriesException(detectorId, cause);
                if (inittedEverResultStatus == RCFPollingStatus.REMOTE_INIT_NOT_EXIT
                    || inittedEverResultStatus == RCFPollingStatus.REMOTE_INDEX_NOT_FOUND) {
                    cause = new RemoteTransportException(RCFPollingAction.NAME, new NotSerializableExceptionWrapper(cause));
                }
                listener.onFailure(cause);
            } else {
                RCFPollingResponse result = null;
                switch (inittedEverResultStatus) {
                    case INIT_DONE:
                        result = new RCFPollingResponse(requiredSamples + 1);
                        break;
                    case INITTING:
                        result = new RCFPollingResponse(requiredSamples - neededSamples);
                        break;
                    case EMPTY:
                        result = new RCFPollingResponse(0);
                        break;
                    case EXCEPTION:
                        listener.onFailure(new RuntimeException());
                        break;
                    default:
                        assertTrue("should not reach here", false);
                        break;
                }

                listener.onResponse(result);
            }
            return null;
        }).when(client).execute(any(RCFPollingAction.class), any(), any());

    }

    public void testProfileModels() throws InterruptedException, IOException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.EMPTY, ErrorResultStatus.NO_ERROR);
        setUpClientExecuteProfileAction();

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile(detector.getId(), ActionListener.wrap(profileResponse -> {
            assertEquals(node1, profileResponse.getCoordinatingNode());
            assertEquals(modelSize * 2, profileResponse.getTotalSizeInBytes());
            assertEquals(2, profileResponse.getModelProfile().length);
            for (ModelProfileOnNode profile : profileResponse.getModelProfile()) {
                assertTrue(node1.equals(profile.getNodeId()) || node2.equals(profile.getNodeId()));
                assertEquals(modelSize, profile.getModelSize());
                if (node1.equals(profile.getNodeId())) {
                    assertEquals(model1Id, profile.getModelId());
                }
                if (node2.equals(profile.getNodeId())) {
                    assertEquals(model0Id, profile.getModelId());
                }
            }
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), modelProfile);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitProgress() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.INITTING, ErrorResultStatus.NO_ERROR);
        ConfigProfile expectedProfile = new DetectorProfile.Builder().state(ConfigState.INIT).build();

        // 123 / 128 rounded to 96%
        InitProgressProfile profile = new InitProgressProfile("96%", neededSamples * detectorIntervalMin, neededSamples);
        expectedProfile.setInitProgress(profile);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile(detector.getId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateInitProgress);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitProgressFailImmediately() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.NO_DOC, JobStatus.ENABLED, RCFPollingStatus.INITTING, ErrorResultStatus.NO_ERROR);
        ConfigProfile expectedProfile = new DetectorProfile.Builder().state(ConfigState.INIT).build();

        // 123 / 128 rounded to 96%
        InitProgressProfile profile = new InitProgressProfile("96%", neededSamples * detectorIntervalMin, neededSamples);
        expectedProfile.setInitProgress(profile);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile(detector.getId(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(CommonMessages.FAIL_TO_FIND_CONFIG_MSG));
            inProgressLatch.countDown();
        }), stateInitProgress);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitNoUpdateNoIndex() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.EMPTY, ErrorResultStatus.NO_ERROR);
        ConfigProfile expectedProfile = new DetectorProfile.Builder()
            .state(ConfigState.INIT)
            .initProgress(new InitProgressProfile("0%", detectorIntervalMin * requiredSamples, requiredSamples))
            .build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile(detector.getId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error(exception);
            for (StackTraceElement ste : exception.getStackTrace()) {
                LOG.info(ste);
            }
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateInitProgress);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitNoIndex() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.INDEX_NOT_FOUND, ErrorResultStatus.NO_ERROR);
        ConfigProfile expectedProfile = new DetectorProfile.Builder()
            .state(ConfigState.INIT)
            .initProgress(new InitProgressProfile("0%", 0, requiredSamples))
            .build();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile(detector.getId(), ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error(exception);
            for (StackTraceElement ste : exception.getStackTrace()) {
                LOG.info(ste);
            }
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }), stateInitProgress);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInvalidRequiredSamples() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new AnomalyDetectorProfileRunner(
                client,
                clientUtil,
                xContentRegistry(),
                nodeFilter,
                0,
                transportService,
                adTaskManager,
                taskProfileRunner
            )
        );
    }

    public void testFailRCFPolling() throws IOException, InterruptedException {
        setUpClientGet(DetectorStatus.EXIST, JobStatus.ENABLED, RCFPollingStatus.EXCEPTION, ErrorResultStatus.NO_ERROR);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        oldRunner.profile(detector.getId(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception instanceof RuntimeException);
            // this means we don't exit with failImmediately. failImmediately can make we return early when there are other concurrent
            // requests
            assertTrue(exception.getMessage(), exception.getMessage().contains("Exceptions:"));
            inProgressLatch.countDown();
        }), stateNError);
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitProgressProfile() {
        InitProgressProfile progressOne = new InitProgressProfile("0%", 0, requiredSamples);
        InitProgressProfile progressTwo = new InitProgressProfile("0%", 0, requiredSamples);
        InitProgressProfile progressThree = new InitProgressProfile("96%", 2, requiredSamples);
        assertTrue(progressOne.equals(progressTwo));
        assertFalse(progressOne.equals(progressThree));
    }
}
