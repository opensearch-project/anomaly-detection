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
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.stats.ADStat;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.suppliers.CounterSupplier;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.JsonDeserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class RCFResultTests extends OpenSearchTestCase {
    Gson gson = new GsonBuilder().create();

    private double[] attribution = new double[] { 1. };
    private HashRing hashRing;
    private DiscoveryNode node;
    private long totalUpdates = 32;
    private double grade = 0.5;
    private double[] pastValues = new double[] { 123, 456 };
    private double[][] expectedValuesList = new double[][] { new double[] { 789, 12 } };
    private double[] likelihood = new double[] { randomDouble() };
    private double threshold = 1.1d;
    private ADStats adStats;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hashRing = mock(HashRing.class);
        node = mock(DiscoveryNode.class);
        doReturn(Optional.of(node)).when(hashRing).getNodeByAddress(any());
        Map<String, ADStat<?>> statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.MODEL_CORRUTPION_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
            }
        };

        adStats = new ADStats(statsMap);
    }

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
        ADCircuitBreakerService adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        RCFResultTransportAction action = new RCFResultTransportAction(
            mock(ActionFilters.class),
            transportService,
            manager,
            adCircuitBreakerService,
            hashRing,
            adStats
        );

        double rcfScore = 0.5;
        int forestSize = 25;
        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener
                .onResponse(
                    new ThresholdingResult(
                        grade,
                        0d,
                        rcfScore,
                        totalUpdates,
                        0,
                        attribution,
                        pastValues,
                        expectedValuesList,
                        likelihood,
                        threshold,
                        forestSize
                    )
                );
            return null;
        }).when(manager).getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        final PlainActionFuture<RCFResultResponse> future = new PlainActionFuture<>();
        RCFResultRequest request = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        action.doExecute(mock(Task.class), request, future);

        RCFResultResponse response = future.actionGet();
        assertEquals(rcfScore, response.getRCFScore(), 0.001);
        assertEquals(forestSize, response.getForestSize(), 0.001);
        assertTrue(Arrays.equals(attribution, response.getAttribution()));
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
        ADCircuitBreakerService adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        RCFResultTransportAction action = new RCFResultTransportAction(
            mock(ActionFilters.class),
            transportService,
            manager,
            adCircuitBreakerService,
            hashRing,
            adStats
        );
        doThrow(NullPointerException.class)
            .when(manager)
            .getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        final PlainActionFuture<RCFResultResponse> future = new PlainActionFuture<>();
        RCFResultRequest request = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        action.doExecute(mock(Task.class), request, future);

        expectThrows(NullPointerException.class, () -> future.actionGet());
    }

    public void testSerialzationResponse() throws IOException {
        RCFResultResponse response = new RCFResultResponse(
            0.3,
            0,
            26,
            attribution,
            totalUpdates,
            grade,
            Version.CURRENT,
            0,
            null,
            null,
            null,
            1.1
        );
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        RCFResultResponse readResponse = RCFResultAction.INSTANCE.getResponseReader().read(streamInput);
        assertThat(response.getForestSize(), equalTo(readResponse.getForestSize()));
        assertThat(response.getRCFScore(), equalTo(readResponse.getRCFScore()));
        assertArrayEquals(response.getAttribution(), readResponse.getAttribution(), 1e-6);
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        RCFResultResponse response = new RCFResultResponse(
            0.3,
            0,
            26,
            attribution,
            totalUpdates,
            grade,
            Version.CURRENT,
            0,
            null,
            null,
            null,
            1.1
        );
        XContentBuilder builder = jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getDoubleValue(json, RCFResultResponse.RCF_SCORE_JSON_KEY), response.getRCFScore(), 0.001);
        assertEquals(JsonDeserializer.getDoubleValue(json, RCFResultResponse.FOREST_SIZE_JSON_KEY), response.getForestSize(), 0.001);
        assertTrue(
            Arrays.equals(JsonDeserializer.getDoubleArrayValue(json, RCFResultResponse.ATTRIBUTION_JSON_KEY), response.getAttribution())
        );
    }

    public void testEmptyID() {
        ActionRequestValidationException e = new RCFResultRequest(null, "123-rcf-1", new double[] { 0 }).validate();
        assertThat(e.validationErrors(), Matchers.hasItem(ADCommonMessages.AD_ID_MISSING_MSG));
    }

    public void testFeatureIsNull() {
        ActionRequestValidationException e = new RCFResultRequest("123", "123-rcf-1", null).validate();
        assertThat(e.validationErrors(), hasItem(RCFResultRequest.INVALID_FEATURE_MSG));
    }

    public void testSerialzationRequest() throws IOException {
        RCFResultRequest response = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        RCFResultRequest readResponse = new RCFResultRequest(streamInput);
        assertThat(response.getAdID(), equalTo(readResponse.getAdID()));
        assertThat(response.getFeatures(), equalTo(readResponse.getFeatures()));
    }

    public void testJsonRequest() throws IOException, JsonPathNotFoundException {
        RCFResultRequest request = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, ADCommonName.ID_JSON_KEY), request.getAdID());
        assertArrayEquals(JsonDeserializer.getDoubleArrayValue(json, ADCommonName.FEATURE_JSON_KEY), request.getFeatures(), 0.001);
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
        RCFResultTransportAction action = new RCFResultTransportAction(
            mock(ActionFilters.class),
            transportService,
            manager,
            breakerService,
            hashRing,
            adStats
        );
        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener
                .onResponse(
                    new ThresholdingResult(
                        grade,
                        0d,
                        0.5,
                        totalUpdates,
                        0,
                        attribution,
                        pastValues,
                        expectedValuesList,
                        likelihood,
                        threshold,
                        30
                    )
                );
            return null;
        }).when(manager).getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));
        when(breakerService.isOpen()).thenReturn(true);

        final PlainActionFuture<RCFResultResponse> future = new PlainActionFuture<>();
        RCFResultRequest request = new RCFResultRequest("123", "123-rcf-1", new double[] { 0 });
        action.doExecute(mock(Task.class), request, future);

        expectThrows(LimitExceededException.class, () -> future.actionGet());
    }

    @SuppressWarnings("unchecked")
    public void testCorruptModel() {
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
        ADCircuitBreakerService adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        RCFResultTransportAction action = new RCFResultTransportAction(
            mock(ActionFilters.class),
            transportService,
            manager,
            adCircuitBreakerService,
            hashRing,
            adStats
        );
        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalArgumentException());
            return null;
        }).when(manager).getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        final PlainActionFuture<RCFResultResponse> future = new PlainActionFuture<>();
        String detectorId = "123";
        RCFResultRequest request = new RCFResultRequest(detectorId, "123-rcf-1", new double[] { 0 });
        action.doExecute(mock(Task.class), request, future);

        expectThrows(IllegalArgumentException.class, () -> future.actionGet());
        Object val = adStats.getStat(StatNames.MODEL_CORRUTPION_COUNT.getName()).getValue();
        assertEquals(1L, ((Long) val).longValue());
        verify(manager, times(1)).clear(eq(detectorId), any());
    }
}
