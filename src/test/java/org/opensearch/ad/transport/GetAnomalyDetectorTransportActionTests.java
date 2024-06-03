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

package org.opensearch.ad.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import org.mockito.Mockito;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.ADTaskProfileRunner;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityProfile;
import org.opensearch.timeseries.model.InitProgressProfile;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableMap;

public class GetAnomalyDetectorTransportActionTests extends OpenSearchSingleNodeTestCase {
    private static ThreadPool threadPool;
    private GetAnomalyDetectorTransportAction action;
    private Task task;
    private ActionListener<GetAnomalyDetectorResponse> response;
    private ADTaskManager adTaskManager;
    private Entity entity;
    private String categoryField;
    private String categoryValue;

    @BeforeClass
    public static void beforeCLass() {
        threadPool = new TestThreadPool("GetAnomalyDetectorTransportActionTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        adTaskManager = mock(ADTaskManager.class);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        SecurityClientUtil clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);
        action = new GetAnomalyDetectorTransportAction(
            Mockito.mock(TransportService.class),
            Mockito.mock(DiscoveryNodeFilterer.class),
            Mockito.mock(ActionFilters.class),
            clusterService,
            client(),
            clientUtil,
            Settings.EMPTY,
            xContentRegistry(),
            adTaskManager,
            mock(ADTaskProfileRunner.class)
        );
        task = Mockito.mock(Task.class);
        response = new ActionListener<GetAnomalyDetectorResponse>() {
            @Override
            public void onResponse(GetAnomalyDetectorResponse getResponse) {
                // When no detectors exist, get response is not generated
                assertTrue(true);
            }

            @Override
            public void onFailure(Exception e) {}
        };
        categoryField = "catField";
        categoryValue = "app-0";
        entity = Entity.createSingleAttributeEntity(categoryField, categoryValue);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testGetTransportAction() throws IOException {
        GetConfigRequest getAnomalyDetectorRequest = new GetConfigRequest("1234", 4321, false, false, "nonempty", "", false, null);
        action.doExecute(task, getAnomalyDetectorRequest, response);
    }

    @Test
    public void testGetTransportActionWithReturnJob() throws IOException {
        GetConfigRequest getAnomalyDetectorRequest = new GetConfigRequest("1234", 4321, true, false, "", "abcd", false, null);
        action.doExecute(task, getAnomalyDetectorRequest, response);
    }

    @Test
    public void testGetAction() {
        Assert.assertNotNull(GetAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(GetAnomalyDetectorAction.INSTANCE.name(), GetAnomalyDetectorAction.NAME);
    }

    @Test
    public void testGetAnomalyDetectorRequest() throws IOException {
        GetConfigRequest request = new GetConfigRequest("1234", 4321, true, false, "", "abcd", false, entity);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        GetConfigRequest newRequest = new GetConfigRequest(input);
        Assert.assertEquals(request.getConfigID(), newRequest.getConfigID());
        Assert.assertEquals(request.getRawPath(), newRequest.getRawPath());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testGetAnomalyDetectorRequestNoEntityValue() throws IOException {
        GetConfigRequest request = new GetConfigRequest("1234", 4321, true, false, "", "abcd", false, null);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        GetConfigRequest newRequest = new GetConfigRequest(input);
        Assert.assertNull(newRequest.getEntity());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetAnomalyDetectorResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        Job adJob = TestHelpers.randomAnomalyDetectorJob();
        GetAnomalyDetectorResponse response = new GetAnomalyDetectorResponse(
            4321,
            "1234",
            5678,
            9867,
            detector,
            adJob,
            false,
            mock(ADTask.class),
            mock(ADTask.class),
            false,
            RestStatus.OK,
            null,
            null,
            false
        );
        response.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        GetAnomalyDetectorResponse newResponse = new GetAnomalyDetectorResponse(input);
        XContentBuilder builder = TestHelpers.builder();
        Assert.assertNotNull(newResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));

        Map<String, Object> map = TestHelpers.XContentBuilderToMap(builder);
        Assert.assertTrue(map.get(RestHandlerUtils.ANOMALY_DETECTOR) instanceof Map);
        Map<String, Object> map1 = (Map<String, Object>) map.get(RestHandlerUtils.ANOMALY_DETECTOR);
        Assert.assertEquals(map1.get("name"), detector.getName());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetAnomalyDetectorProfileResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        Job adJob = TestHelpers.randomAnomalyDetectorJob();
        InitProgressProfile initProgress = new InitProgressProfile("99%", 2L, 2);
        EntityProfile entityProfile = new EntityProfile.Builder().initProgress(initProgress).build();
        GetAnomalyDetectorResponse response = new GetAnomalyDetectorResponse(
            4321,
            "1234",
            5678,
            9867,
            detector,
            adJob,
            false,
            mock(ADTask.class),
            mock(ADTask.class),
            false,
            RestStatus.OK,
            null,
            entityProfile,
            true
        );
        response.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        GetAnomalyDetectorResponse newResponse = new GetAnomalyDetectorResponse(input);
        XContentBuilder builder = TestHelpers.builder();
        Assert.assertNotNull(newResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));

        // {init_progress={percentage=99%, estimated_minutes_left=2, needed_shingles=2}}
        Map<String, Object> map = TestHelpers.XContentBuilderToMap(builder);
        Map<String, Object> parsedInitProgress = (Map<String, Object>) (map.get(CommonName.INIT_PROGRESS));
        Assert.assertEquals(initProgress.getPercentage(), parsedInitProgress.get(InitProgressProfile.PERCENTAGE).toString());
        assertTrue(initProgress.toString().contains("[percentage=99%,estimated_minutes_left=2,needed_shingles=2]"));
        Assert
            .assertEquals(
                String.valueOf(initProgress.getEstimatedMinutesLeft()),
                parsedInitProgress.get(InitProgressProfile.ESTIMATED_MINUTES_LEFT).toString()
            );
        Assert
            .assertEquals(
                String.valueOf(initProgress.getNeededDataPoints()),
                parsedInitProgress.get(InitProgressProfile.NEEDED_SHINGLES).toString()
            );
    }
}
