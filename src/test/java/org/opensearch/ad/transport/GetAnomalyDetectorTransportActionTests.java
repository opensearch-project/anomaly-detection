// @anomaly-detection.create-detector Commented this code until we have support of Get Detector for extensibility
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
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.EntityProfile;
import org.opensearch.ad.model.InitProgressProfile;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.Extension;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import com.google.common.collect.ImmutableMap;

public class GetAnomalyDetectorTransportActionTests extends OpenSearchSingleNodeTestCase {

    private GetAnomalyDetectorTransportAction action;
    private Task task;
    private ActionListener<GetAnomalyDetectorResponse> response;
    private ADTaskManager adTaskManager;
    private Entity entity;
    private String categoryField;
    private String categoryValue;
    private SDKClusterService clusterService;
    private ExtensionsRunner mockRunner;
    private SDKRestClient client;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockRunner = mock(ExtensionsRunner.class);
        Settings settings = Settings.EMPTY;
        List<Setting<?>> settingsList = List.of(AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW, AnomalyDetectorSettings.PAGE_SIZE);
        clusterService = mock(SDKClusterService.class);
        client = mock(SDKRestClient.class);

        Extension mockExtension = mock(Extension.class);
        when(mockRunner.getEnvironmentSettings()).thenReturn(settings);
        when(mockRunner.getExtension()).thenReturn(mockExtension);
        when(mockExtension.getSettings()).thenReturn(settingsList);
        SDKClusterService.SDKClusterSettings clusterSettings = new SDKClusterService(mockRunner).getClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        adTaskManager = mock(ADTaskManager.class);

        SDKNamedXContentRegistry sdkNamedXContentRegistry = mock(SDKNamedXContentRegistry.class);
        when(mockRunner.getNamedXContentRegistry()).thenReturn(sdkNamedXContentRegistry);
        when(sdkNamedXContentRegistry.getRegistry()).thenReturn(xContentRegistry());

        action = new GetAnomalyDetectorTransportAction(
            mockRunner,
            mock(TaskManager.class),
            mock(DiscoveryNodeFilterer.class),
            mock(ActionFilters.class),
            clusterService,
            client,
            sdkNamedXContentRegistry,
            adTaskManager
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
        GetAnomalyDetectorRequest getAnomalyDetectorRequest = new GetAnomalyDetectorRequest(
            "1234",
            4321,
            false,
            false,
            "nonempty",
            "",
            false,
            null
        );
        action.doExecute(task, getAnomalyDetectorRequest, response);
    }

    @Test
    public void testGetTransportActionWithReturnJob() throws IOException {
        GetAnomalyDetectorRequest getAnomalyDetectorRequest = new GetAnomalyDetectorRequest(
            "1234",
            4321,
            true,
            false,
            "",
            "abcd",
            false,
            null
        );
        action.doExecute(task, getAnomalyDetectorRequest, response);
    }

    @Test
    public void testGetAction() {
        Assert.assertNotNull(GetAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(GetAnomalyDetectorAction.INSTANCE.name(), GetAnomalyDetectorAction.NAME);
    }

    @Test
    public void testGetAnomalyDetectorRequest() throws IOException {
        GetAnomalyDetectorRequest request = new GetAnomalyDetectorRequest("1234", 4321, true, false, "", "abcd", false, entity);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        GetAnomalyDetectorRequest newRequest = new GetAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
        Assert.assertEquals(request.getRawPath(), newRequest.getRawPath());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testGetAnomalyDetectorRequestNoEntityValue() throws IOException {
        GetAnomalyDetectorRequest request = new GetAnomalyDetectorRequest("1234", 4321, true, false, "", "abcd", false, null);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        GetAnomalyDetectorRequest newRequest = new GetAnomalyDetectorRequest(input);
        Assert.assertNull(newRequest.getEntity());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetAnomalyDetectorResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        AnomalyDetectorJob adJob = TestHelpers.randomAnomalyDetectorJob();
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
        AnomalyDetectorJob adJob = TestHelpers.randomAnomalyDetectorJob();
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
