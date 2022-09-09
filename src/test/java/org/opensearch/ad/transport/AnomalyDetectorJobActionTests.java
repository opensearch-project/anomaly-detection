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

package org.opensearch.ad.transport;


public class AnomalyDetectorJobActionTests extends OpenSearchIntegTestCase {
    private AnomalyDetectorJobTransportAction action;
    private Task task;
    private AnomalyDetectorJobRequest request;
    private ActionListener<AnomalyDetectorJobResponse> response;

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
        Client client = mock(Client.class);

        action = new AnomalyDetectorJobTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService,
            indexSettings(),
            mock(AnomalyDetectionIndices.class),
            xContentRegistry(),
            mock(ADTaskManager.class)
        );
        task = mock(Task.class);
        request = new AnomalyDetectorJobRequest("1234", 4567, 7890, "_start");
        response = new ActionListener<AnomalyDetectorJobResponse>() {
            @Override
            public void onResponse(AnomalyDetectorJobResponse adResponse) {
                // Will not be called as there is no detector
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                // Will not be called as there is no detector
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testStartAdJobTransportAction() {
        action.doExecute(task, request, response);
    }

    @Test
    public void testStopAdJobTransportAction() {
        AnomalyDetectorJobRequest stopRequest = new AnomalyDetectorJobRequest("1234", 4567, 7890, "_stop");
        action.doExecute(task, stopRequest, response);
    }

    @Test
    public void testAdJobAction() {
        Assert.assertNotNull(AnomalyDetectorJobAction.INSTANCE.name());
        Assert.assertEquals(AnomalyDetectorJobAction.INSTANCE.name(), AnomalyDetectorJobAction.NAME);
    }

    @Test
    public void testAdJobRequest() throws IOException {
        DetectionDateRange detectionDateRange = new DetectionDateRange(Instant.MIN, Instant.now());
        request = new AnomalyDetectorJobRequest("1234", detectionDateRange, false, 4567, 7890, "_start");

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        AnomalyDetectorJobRequest newRequest = new AnomalyDetectorJobRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
    }

    @Test
    public void testAdJobRequest_NullDetectionDateRange() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        AnomalyDetectorJobRequest newRequest = new AnomalyDetectorJobRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
    }

    @Test
    public void testAdJobResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetectorJobResponse response = new AnomalyDetectorJobResponse("1234", 45, 67, 890, RestStatus.OK);
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        AnomalyDetectorJobResponse newResponse = new AnomalyDetectorJobResponse(input);
        Assert.assertEquals(response.getId(), newResponse.getId());
    }
}
*/
