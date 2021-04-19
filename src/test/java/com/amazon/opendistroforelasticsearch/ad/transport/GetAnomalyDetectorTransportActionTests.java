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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.EntityProfile;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.google.common.collect.ImmutableMap;

public class GetAnomalyDetectorTransportActionTests extends OpenSearchSingleNodeTestCase {
    private GetAnomalyDetectorTransportAction action;
    private Task task;
    private ActionListener<GetAnomalyDetectorResponse> response;
    private ADTaskManager adTaskManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        adTaskManager = mock(ADTaskManager.class);
        action = new GetAnomalyDetectorTransportAction(
            Mockito.mock(TransportService.class),
            Mockito.mock(DiscoveryNodeFilterer.class),
            Mockito.mock(ActionFilters.class),
            clusterService,
            client(),
            Settings.EMPTY,
            xContentRegistry(),
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
        GetAnomalyDetectorRequest request = new GetAnomalyDetectorRequest("1234", 4321, true, false, "", "abcd", false, "value");
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
        Assert.assertNull(newRequest.getEntityValue());
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

    @Test
    public void testGetAnomalyDetectorProfileResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        AnomalyDetectorJob adJob = TestHelpers.randomAnomalyDetectorJob();
        EntityProfile entityProfile = new EntityProfile.Builder("catField", "app-0").build();
        GetAnomalyDetectorResponse response = new GetAnomalyDetectorResponse(
            4321,
            "1234",
            5678,
            9867,
            detector,
            adJob,
            false,
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

        Map<String, Object> map = TestHelpers.XContentBuilderToMap(builder);
        Assert.assertEquals(map.get(EntityProfile.CATEGORY_FIELD), "catField");
        Assert.assertEquals(map.get(EntityProfile.ENTITY_VALUE), "app-0");
    }
}
