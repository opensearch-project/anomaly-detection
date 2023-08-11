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

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Collections;

import org.hamcrest.Matchers;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.JsonDeserializer;

public class ThresholdResultTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testNormal() {
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        ModelManager manager = mock(ModelManager.class);
        ThresholdResultTransportAction action = new ThresholdResultTransportAction(mock(ActionFilters.class), transportService, manager);
        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener.onResponse(new ThresholdingResult(0, 1.0d, 0.2));
            return null;
        }).when(manager).getThresholdingResult(any(String.class), any(String.class), anyDouble(), any(ActionListener.class));

        final PlainActionFuture<ThresholdResultResponse> future = new PlainActionFuture<>();
        ThresholdResultRequest request = new ThresholdResultRequest("123", "123-threshold", 2);
        action.doExecute(mock(Task.class), request, future);

        ThresholdResultResponse response = future.actionGet();
        assertEquals(0, response.getAnomalyGrade(), 0.001);
        assertEquals(1, response.getConfidence(), 0.001);
    }

    @SuppressWarnings("unchecked")
    public void testExecutionException() {
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        ModelManager manager = mock(ModelManager.class);
        ThresholdResultTransportAction action = new ThresholdResultTransportAction(mock(ActionFilters.class), transportService, manager);
        doThrow(NullPointerException.class)
            .when(manager)
            .getThresholdingResult(any(String.class), any(String.class), anyDouble(), any(ActionListener.class));

        final PlainActionFuture<ThresholdResultResponse> future = new PlainActionFuture<>();
        ThresholdResultRequest request = new ThresholdResultRequest("123", "123-threshold", 2);
        action.doExecute(mock(Task.class), request, future);

        expectThrows(NullPointerException.class, () -> future.actionGet());
    }

    public void testSerialzationResponse() throws IOException {
        ThresholdResultResponse response = new ThresholdResultResponse(1, 0.8);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ThresholdResultResponse readResponse = ThresholdResultAction.INSTANCE.getResponseReader().read(streamInput);
        assertThat(response.getAnomalyGrade(), equalTo(readResponse.getAnomalyGrade()));
        assertThat(response.getConfidence(), equalTo(readResponse.getConfidence()));
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        ThresholdResultResponse response = new ThresholdResultResponse(1, 0.8);
        XContentBuilder builder = jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = builder.toString();
        assertEquals(JsonDeserializer.getDoubleValue(json, ADCommonName.ANOMALY_GRADE_JSON_KEY), response.getAnomalyGrade(), 0.001);
        assertEquals(JsonDeserializer.getDoubleValue(json, ADCommonName.CONFIDENCE_JSON_KEY), response.getConfidence(), 0.001);
    }

    public void testEmptyDetectorID() {
        ActionRequestValidationException e = new ThresholdResultRequest(null, "123-threshold", 2).validate();
        assertThat(e.validationErrors(), Matchers.hasItem(ADCommonMessages.AD_ID_MISSING_MSG));
    }

    public void testEmptyModelID() {
        ActionRequestValidationException e = new ThresholdResultRequest("123", "", 2).validate();
        assertThat(e.validationErrors(), Matchers.hasItem(ADCommonMessages.MODEL_ID_MISSING_MSG));
    }

    public void testSerialzationRequest() throws IOException {
        ThresholdResultRequest response = new ThresholdResultRequest("123", "123-threshold", 2);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ThresholdResultRequest readResponse = new ThresholdResultRequest(streamInput);
        assertThat(response.getAdID(), equalTo(readResponse.getAdID()));
        assertThat(response.getRCFScore(), equalTo(readResponse.getRCFScore()));
    }

    public void testJsonRequest() throws IOException, JsonPathNotFoundException {
        ThresholdResultRequest request = new ThresholdResultRequest("123", "123-threshold", 2);
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = builder.toString();
        assertEquals(JsonDeserializer.getTextValue(json, ADCommonName.ID_JSON_KEY), request.getAdID());
        assertEquals(JsonDeserializer.getDoubleValue(json, ADCommonName.RCF_SCORE_JSON_KEY), request.getRCFScore(), 0.001);
    }
}
