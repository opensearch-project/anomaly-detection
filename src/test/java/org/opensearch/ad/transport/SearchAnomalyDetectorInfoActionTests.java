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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.TestHelpers;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

public class SearchAnomalyDetectorInfoActionTests extends OpenSearchIntegTestCase {
    private SearchAnomalyDetectorInfoRequest request;
    private ActionListener<SearchAnomalyDetectorInfoResponse> response;
    private SearchAnomalyDetectorInfoTransportAction action;
    private Task task;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        action = new SearchAnomalyDetectorInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client(),
            clusterService()
        );
        task = mock(Task.class);
        response = new ActionListener<SearchAnomalyDetectorInfoResponse>() {
            @Override
            public void onResponse(SearchAnomalyDetectorInfoResponse response) {
                Assert.assertEquals(response.getCount(), 0);
                Assert.assertEquals(response.isNameExists(), false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testSearchCount() throws IOException {
        // Anomaly Detectors index will not exist, onResponse will be called
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest(null, "count");
        action.doExecute(task, request, response);
    }

    @Test
    public void testSearchMatch() throws IOException {
        // Anomaly Detectors index will not exist, onResponse will be called
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest("testDetector", "match");
        action.doExecute(task, request, response);
    }

    @Test
    public void testSearchInfoAction() {
        Assert.assertNotNull(SearchAnomalyDetectorInfoAction.INSTANCE.name());
        Assert.assertEquals(SearchAnomalyDetectorInfoAction.INSTANCE.name(), SearchAnomalyDetectorInfoAction.NAME);
    }

    @Test
    public void testSearchInfoRequest() throws IOException {
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest("testDetector", "match");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        SearchAnomalyDetectorInfoRequest newRequest = new SearchAnomalyDetectorInfoRequest(input);
        Assert.assertEquals(request.getName(), newRequest.getName());
        Assert.assertEquals(request.getRawPath(), newRequest.getRawPath());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testSearchInfoResponse() throws IOException {
        SearchAnomalyDetectorInfoResponse response = new SearchAnomalyDetectorInfoResponse(1, true);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        SearchAnomalyDetectorInfoResponse newResponse = new SearchAnomalyDetectorInfoResponse(input);
        Assert.assertEquals(response.getCount(), newResponse.getCount());
        Assert.assertEquals(response.isNameExists(), newResponse.isNameExists());
        Assert.assertNotNull(response.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
    }
}
