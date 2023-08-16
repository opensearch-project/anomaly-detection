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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.ClusterCreation;
import test.org.opensearch.ad.util.JsonDeserializer;

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

        String json = builder.toString();
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
