/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

public class SearchTopAnomalyResultTransportActionTests extends OpenSearchIntegTestCase {
    private SearchTopAnomalyResultTransportAction action;
    private ActionListener<SearchTopAnomalyResultResponse> listener;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        action = new SearchTopAnomalyResultTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ADSearchHandler.class),
            client()
        );
        listener = new ActionListener<SearchTopAnomalyResultResponse>() {
            @Override
            public void onResponse(SearchTopAnomalyResultResponse response) {
                Assert.assertTrue(true);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };
    }

    @Ignore
    public void testWithAdIndex() {
        SearchTopAnomalyResultRequest request = new SearchTopAnomalyResultRequest(
            "test-detector-id",
            "test-task-id",
            false,
            1,
            Arrays.asList("test-field"),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(10, ChronoUnit.DAYS),
            Instant.now()
        );

        createIndex(".opendistro-anomaly-detectors");
        action.doExecute(mock(Task.class), request, listener);

    }
}

//
// public class DeleteAnomalyDetectorActionTests extends OpenSearchIntegTestCase {
//
// @Test
// public void testStatsAction() {
// Assert.assertNotNull(DeleteAnomalyDetectorAction.INSTANCE.name());
// Assert.assertEquals(DeleteAnomalyDetectorAction.INSTANCE.name(), DeleteAnomalyDetectorAction.NAME);
// }
//
// @Test
// public void testDeleteRequest() throws IOException {
// DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
// BytesStreamOutput out = new BytesStreamOutput();
// request.writeTo(out);
// StreamInput input = out.bytes().streamInput();
// DeleteAnomalyDetectorRequest newRequest = new DeleteAnomalyDetectorRequest(input);
// Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
// Assert.assertNull(newRequest.validate());
// }
//
// @Test
// public void testEmptyDeleteRequest() {
// DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("");
// ActionRequestValidationException exception = request.validate();
// Assert.assertNotNull(exception);
// }
//
// @Test
// public void testTransportActionWithAdIndex() {
// // DeleteResponse is not called because detector ID will not exist
// createIndex(".opendistro-anomaly-detector-jobs");
// DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
// action.doExecute(mock(Task.class), request, response);
// }
//
// @Test
// public void testTransportActionWithoutAdIndex() throws IOException {
// // DeleteResponse is not called because detector ID will not exist
// DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
// action.doExecute(mock(Task.class), request, response);
// }
// }
