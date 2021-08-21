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

package org.opensearch.ad.transport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.ml.client.MachineLearningClient;
import org.opensearch.ml.common.dataframe.ColumnMeta;
import org.opensearch.ml.common.dataframe.ColumnType;
import org.opensearch.ml.common.dataframe.DataFrame;
import org.opensearch.ml.common.dataframe.DataFrameBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.JsonDeserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class PMMLResultTests extends OpenSearchTestCase {
    Gson gson = new GsonBuilder().create();

    private String adId = "123";
    private String mlModelId = "1";
    private String[] featureNames = new String[] { "value" };
    private double[] features = new double[] { 0 };

    @SuppressWarnings("unchecked")
    public void testNormal() {
        // TODO: this whole test is not working properly. Need to refactor the code/test
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        ADCircuitBreakerService adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        PMMLResultTransportAction action = new PMMLResultTransportAction(
            mock(ActionFilters.class),
            transportService,
            adCircuitBreakerService,
            mock(NodeClient.class)
        );

        // data frame mock
        ColumnMeta[] header = new ColumnMeta[] { new ColumnMeta(featureNames[0], ColumnType.from(features[0])) };
        List<Map<String, Object>> values = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put(featureNames[0], features[0]);
        values.add(map);
        DataFrame input = DataFrameBuilder.load(header, values);

        // prediction result mock
        ColumnMeta[] resultHeader = new ColumnMeta[] {
            new ColumnMeta("outlier", ColumnType.BOOLEAN),
            new ColumnMeta("decisionFunction", ColumnType.DOUBLE) };
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> map2 = new HashMap<>();
        map2.put("outlier", false);
        map2.put("decisionFunction", 0.3);
        result.add(map2);
        DataFrame output = DataFrameBuilder.load(resultHeader, result);

        // ml client mock
        MachineLearningClient mlClient = mock(MachineLearningClient.class);
        doAnswer(invocation -> {
            ActionListener<PMMLResultResponse> listener = invocation.getArgument(4);
            PMMLResultResponse response = new PMMLResultResponse(
                (boolean) output.getRow(0).getValue(0).getValue(),
                (double) output.getRow(0).getValue(1).getValue()
            );
            listener.onResponse(response);
            return null;
        }).when(mlClient).predict(any(String.class), any(List.class), any(DataFrame.class), any(String.class), any(ActionListener.class));

        // action execute mock
        final PlainActionFuture<PMMLResultResponse> future = new PlainActionFuture<>();
        PMMLResultRequest request = new PMMLResultRequest(adId, mlModelId, featureNames, features);

        // doAnswer(invocation -> {
        // action.sendPredictionRequest(mlClient, request, future);
        // return null;
        // }).when(action).execute(any(Task.class), any(PMMLResultRequest.class), any(ActionListener.class));

        // execute
        // action.sendPredictionRequest(mock(Task.class), request, future);
        // PMMLResultResponse response = future.actionGet();
        // assertFalse(response.getOutlier());
        // assertEquals(0.3, response.getDecisionFunction(), 0.001);
    }

    public void testSerializationResponse() throws IOException {
        PMMLResultResponse response = new PMMLResultResponse(false, 0.3);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        PMMLResultResponse readResponse = PMMLResultAction.INSTANCE.getResponseReader().read(streamInput);
        assertThat(response.getOutlier(), equalTo(readResponse.getOutlier()));
        assertThat(response.getDecisionFunction(), equalTo(readResponse.getDecisionFunction()));
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        PMMLResultResponse response = new PMMLResultResponse(false, 0.3);
        XContentBuilder builder = jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(
            JsonDeserializer.getTextValue(json, PMMLResultResponse.OUTLIER_JSON_KEY),
            String.valueOf(response.getOutlier()),
            "false"
        );
        assertEquals(
            JsonDeserializer.getDoubleValue(json, PMMLResultResponse.DECISION_FUNCTION_JSON_KEY),
            response.getDecisionFunction(),
            0.001
        );
    }

    public void testEmptyADID() {
        ActionRequestValidationException e = new PMMLResultRequest("", mlModelId, featureNames, features).validate();
        assertThat(e.validationErrors(), Matchers.hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testNullMLModelID() {
        ActionRequestValidationException e = new PMMLResultRequest(adId, null, featureNames, features).validate();
        assertThat(e.validationErrors(), hasItem(CommonErrorMessages.ML_MODEL_ID_MISSING_MSG));
    }

    public void testFeatureNameIsEmpty() {
        ActionRequestValidationException e = new PMMLResultRequest(adId, mlModelId, new String[0], features).validate();
        assertThat(e.validationErrors(), hasItem(PMMLResultRequest.INVALID_FEATURE_NAME_MSG));
    }

    public void testFeatureIsNull() {
        ActionRequestValidationException e = new PMMLResultRequest("123", "1", featureNames, null).validate();
        assertThat(e.validationErrors(), hasItem(PMMLResultRequest.INVALID_FEATURE_MSG));
    }

    public void testInvalidLengths() {
        ActionRequestValidationException e = new PMMLResultRequest(adId, mlModelId, new String[2], features).validate();
        assertThat(e.validationErrors(), hasItem(PMMLResultRequest.INVALID_LENGTH_MSG));
    }

    public void testSerializationRequest() throws IOException {
        PMMLResultRequest request = new PMMLResultRequest(adId, mlModelId, featureNames, features);
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        PMMLResultRequest readRequest = new PMMLResultRequest(streamInput);
        assertThat(request.getAdID(), equalTo(readRequest.getAdID()));
        assertThat(request.getFeatureValues(), equalTo(readRequest.getFeatureValues()));
    }

    public void testJsonRequest() throws IOException, JsonPathNotFoundException {
        PMMLResultRequest request = new PMMLResultRequest(adId, mlModelId, featureNames, features);
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, CommonName.ID_JSON_KEY), request.getAdID());
        assertEquals(JsonDeserializer.getTextValue(json, CommonName.ML_MODEL_ID_KEY), request.getMlModelID());
        assertArrayEquals(JsonDeserializer.getDoubleArrayValue(json, CommonName.FEATURE_JSON_KEY), request.getFeatureValues(), 0.001);
    }

    @SuppressWarnings("unchecked")
    public void testCircuitBreaker() {
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
        ADCircuitBreakerService breakerService = mock(ADCircuitBreakerService.class);
        PMMLResultTransportAction action = new PMMLResultTransportAction(
            mock(ActionFilters.class),
            transportService,
            breakerService,
            mock(NodeClient.class)
        );
        when(breakerService.isOpen()).thenReturn(true);

        final PlainActionFuture<PMMLResultResponse> future = new PlainActionFuture<>();
        PMMLResultRequest request = new PMMLResultRequest(adId, mlModelId, featureNames, features);
        action.doExecute(mock(Task.class), request, future);

        expectThrows(LimitExceededException.class, () -> future.actionGet());
    }
}
