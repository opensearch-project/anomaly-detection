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
/* @anomaly-detection commenting until we have support for client.settings(), client.threadpool() 
package org.opensearch.ad.transport;


public class DeleteTests extends AbstractADTest {
    private DeleteModelResponse response;
    private List<FailedNodeException> failures;
    private List<DeleteModelNodeResponse> deleteModelResponse;
    private String node1, node2, nodename1, nodename2;
    private Client client;
    private ClusterService clusterService;
    private TransportService transportService;
    private ThreadPool threadPool;
    private ActionFilters actionFilters;
    private Task task;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        node1 = "node1";
        node2 = "node2";
        nodename1 = "nodename1";
        nodename2 = "nodename2";
        DiscoveryNode discoveryNode1 = new DiscoveryNode(
            nodename1,
            node1,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        DiscoveryNode discoveryNode2 = new DiscoveryNode(
            nodename2,
            node2,
            new TransportAddress(TransportAddress.META_ADDRESS, 9301),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        List<DiscoveryNode> discoveryNodes = new ArrayList<DiscoveryNode>(2);
        discoveryNodes.add(discoveryNode1);
        discoveryNodes.add(discoveryNode2);

        DeleteModelNodeResponse nodeResponse1 = new DeleteModelNodeResponse(discoveryNode1);
        DeleteModelNodeResponse nodeResponse2 = new DeleteModelNodeResponse(discoveryNode2);

        deleteModelResponse = new ArrayList<>();

        deleteModelResponse.add(nodeResponse1);
        deleteModelResponse.add(nodeResponse2);

        failures = new ArrayList<>();
        failures.add(new FailedNodeException("node3", "blah", new OpenSearchException("foo")));

        response = new DeleteModelResponse(new ClusterName("Cluster"), deleteModelResponse, failures);

        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(discoveryNode1);
        when(clusterService.state())
            .thenReturn(ClusterCreation.state(new ClusterName("test"), discoveryNode2, discoveryNode1, discoveryNodes));

        transportService = mock(TransportService.class);
        threadPool = mock(ThreadPool.class);
        actionFilters = mock(ActionFilters.class);
        Settings settings = Settings.builder().put("plugins.anomaly_detection.request_timeout", TimeValue.timeValueSeconds(10)).build();
        task = mock(Task.class);
        when(task.getId()).thenReturn(1000L);
        client = mock(Client.class);
        when(client.settings()).thenReturn(settings);
        when(client.threadPool()).thenReturn(threadPool);
    }

    public void testSerialzationResponse() throws IOException {

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        DeleteModelResponse readResponse = DeleteModelAction.INSTANCE.getResponseReader().read(streamInput);
        assertTrue(readResponse.hasFailures());

        assertEquals(failures.size(), readResponse.failures().size());
        assertEquals(deleteModelResponse.size(), readResponse.getNodes().size());
    }

    public void testEmptyIDDeleteModel() {
        ActionRequestValidationException e = new DeleteModelRequest("").validate();
        assertThat(e.validationErrors(), Matchers.hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testEmptyIDStopDetector() {
        ActionRequestValidationException e = new StopDetectorRequest().validate();
        assertThat(e.validationErrors(), hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testValidIDStopDetector() {
        ActionRequestValidationException e = new StopDetectorRequest().adID("foo").validate();
        assertThat(e, is(nullValue()));
    }

    public void testSerialzationRequestDeleteModel() throws IOException {
        DeleteModelRequest request = new DeleteModelRequest("123");
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        DeleteModelRequest readRequest = new DeleteModelRequest(streamInput);
        assertThat(request.getAdID(), equalTo(readRequest.getAdID()));
    }

    public void testSerialzationRequestStopDetector() throws IOException {
        StopDetectorRequest request = new StopDetectorRequest().adID("123");
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        StopDetectorRequest readRequest = new StopDetectorRequest(streamInput);
        assertThat(request.getAdID(), equalTo(readRequest.getAdID()));
    }

    public <R extends ToXContent> void testJsonRequestTemplate(R request, Supplier<String> requestSupplier) throws IOException,
        JsonPathNotFoundException {
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, CommonName.ID_JSON_KEY), requestSupplier.get());
    }

    public void testJsonRequestStopDetector() throws IOException, JsonPathNotFoundException {
        StopDetectorRequest request = new StopDetectorRequest().adID("123");
        testJsonRequestTemplate(request, request::getAdID);
    }

    public void testJsonRequestDeleteModel() throws IOException, JsonPathNotFoundException {
        DeleteModelRequest request = new DeleteModelRequest("123");
        testJsonRequestTemplate(request, request::getAdID);
    }

    public void testNewResponse() throws IOException {
        StreamInput input = mock(StreamInput.class);
        when(input.readByte()).thenReturn((byte) 0x01);
        AcknowledgedResponse response = new AcknowledgedResponse(input);

        assertTrue(response.isAcknowledged());
    }

    private enum DetectorExecutionMode {
        DELETE_MODEL_NORMAL,
        DELETE_MODEL_FAILURE
    }

    @SuppressWarnings("unchecked")
    public void StopDetectorResponseTemplate(DetectorExecutionMode mode) throws Exception {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length >= 3
            );
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<DeleteModelResponse> listener = (ActionListener<DeleteModelResponse>) args[2];

            assertTrue(listener != null);
            if (mode == DetectorExecutionMode.DELETE_MODEL_FAILURE) {
                listener.onFailure(new OpenSearchException(""));
            } else {
                listener.onResponse(response);
            }

            return null;
        }).when(client).execute(eq(DeleteModelAction.INSTANCE), any(), any());

        BulkByScrollResponse deleteByQueryResponse = mock(BulkByScrollResponse.class);
        when(deleteByQueryResponse.getDeleted()).thenReturn(10L);

        String detectorID = "123";

        DiscoveryNodeFilterer nodeFilter = mock(DiscoveryNodeFilterer.class);
        StopDetectorTransportAction action = new StopDetectorTransportAction(transportService, nodeFilter, actionFilters, client);

        StopDetectorRequest request = new StopDetectorRequest().adID(detectorID);
        PlainActionFuture<StopDetectorResponse> listener = new PlainActionFuture<>();
        action.doExecute(task, request, listener);

        StopDetectorResponse response = listener.actionGet();
        assertTrue(!response.success());

    }

    public void testNormalResponse() throws Exception {
        StopDetectorResponseTemplate(DetectorExecutionMode.DELETE_MODEL_NORMAL);
    }

    public void testFailureResponse() throws Exception {
        StopDetectorResponseTemplate(DetectorExecutionMode.DELETE_MODEL_FAILURE);
    }

}
*/
