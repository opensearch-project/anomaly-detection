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
/*


public class DeleteAnomalyDetectorActionTests extends OpenSearchIntegTestCase {
    private DeleteAnomalyDetectorTransportAction action;
    private ActionListener<DeleteResponse> response;
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
        action = new DeleteAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client(),
            clusterService,
            Settings.EMPTY,
            xContentRegistry(),
            adTaskManager
        );
        response = new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                Assert.assertTrue(true);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testStatsAction() {
        Assert.assertNotNull(DeleteAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(DeleteAnomalyDetectorAction.INSTANCE.name(), DeleteAnomalyDetectorAction.NAME);
    }

    @Test
    public void testDeleteRequest() throws IOException {
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        DeleteAnomalyDetectorRequest newRequest = new DeleteAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testEmptyDeleteRequest() {
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("");
        ActionRequestValidationException exception = request.validate();
        Assert.assertNotNull(exception);
    }

    @Test
    public void testTransportActionWithAdIndex() {
        // DeleteResponse is not called because detector ID will not exist
        createIndex(".opendistro-anomaly-detector-jobs");
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        action.doExecute(mock(Task.class), request, response);
    }

    @Test
    public void testTransportActionWithoutAdIndex() throws IOException {
        // DeleteResponse is not called because detector ID will not exist
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        action.doExecute(mock(Task.class), request, response);
    }
}
*/
