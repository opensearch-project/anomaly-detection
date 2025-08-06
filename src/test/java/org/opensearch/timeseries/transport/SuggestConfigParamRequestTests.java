/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;

public class SuggestConfigParamRequestTests extends OpenSearchTestCase {
    private NamedWriteableRegistry registry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, BoolQueryBuilder.NAME, BoolQueryBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, RangeQueryBuilder.NAME, RangeQueryBuilder::new));
        namedWriteables
            .add(
                new NamedWriteableRegistry.Entry(
                    AggregationBuilder.class,
                    ValueCountAggregationBuilder.NAME,
                    ValueCountAggregationBuilder::new
                )
            );
        registry = new NamedWriteableRegistry(namedWriteables);
    }

    /**
     * Test serialization and deserialization of SuggestConfigParamRequest with AD context.
     */
    public void testSerializationDeserialization_ADContext() throws IOException {
        // Create an AnomalyDetector instance
        AnomalyDetector detector = createTestAnomalyDetector();

        AnalysisType context = AnalysisType.AD;
        String param = "test-param";
        TimeValue requestTimeout = TimeValue.timeValueSeconds(30);

        SuggestConfigParamRequest originalRequest = new SuggestConfigParamRequest(context, detector, param, requestTimeout);

        // Serialize the request
        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        // Deserialize the request
        StreamInput in = out.bytes().streamInput();

        StreamInput input = new NamedWriteableAwareStreamInput(in, registry);

        SuggestConfigParamRequest deserializedRequest = new SuggestConfigParamRequest(input);

        // Verify the deserialized object
        assertEquals(context, deserializedRequest.getContext());
        assertTrue(deserializedRequest.getConfig() instanceof AnomalyDetector);
        AnomalyDetector deserializedDetector = (AnomalyDetector) deserializedRequest.getConfig();
        assertEquals(detector, deserializedDetector);
        assertEquals(param, deserializedRequest.getParam());
        assertEquals(requestTimeout, deserializedRequest.getRequestTimeout());

        assertEquals(ADIndex.CONFIG.getIndexName(), deserializedRequest.index());
    }

    /**
     * Test serialization and deserialization of SuggestConfigParamRequest with Forecast context.
     */
    public void testSerializationDeserialization_ForecastContext() throws IOException {
        // Create a Forecaster instance using TestHelpers.ForecasterBuilder
        Forecaster forecaster = createTestForecaster();

        AnalysisType context = AnalysisType.FORECAST;
        String param = "test-param";
        TimeValue requestTimeout = TimeValue.timeValueSeconds(30);

        SuggestConfigParamRequest originalRequest = new SuggestConfigParamRequest(context, forecaster, param, requestTimeout);

        // Serialize the request
        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        // Deserialize the request
        StreamInput in = out.bytes().streamInput();
        StreamInput input = new NamedWriteableAwareStreamInput(in, registry);

        SuggestConfigParamRequest deserializedRequest = new SuggestConfigParamRequest(input);

        // Verify the deserialized object
        assertEquals(context, deserializedRequest.getContext());
        assertTrue(deserializedRequest.getConfig() instanceof Forecaster);
        Forecaster deserializedForecaster = (Forecaster) deserializedRequest.getConfig();
        assertEquals(forecaster, deserializedForecaster);
        assertEquals(param, deserializedRequest.getParam());
        assertEquals(requestTimeout, deserializedRequest.getRequestTimeout());

        assertEquals(ForecastIndex.CONFIG.getIndexName(), deserializedRequest.index());
    }

    // Helper methods to create test instances of AnomalyDetector and Forecaster

    private AnomalyDetector createTestAnomalyDetector() {
        // Use TestHelpers.AnomalyDetectorBuilder to create a test AnomalyDetector instance
        try {
            return TestHelpers.AnomalyDetectorBuilder.newInstance(1).build();
        } catch (IOException e) {
            fail("Failed to create test AnomalyDetector: " + e.getMessage());
            return null;
        }
    }

    private Forecaster createTestForecaster() {
        // Use TestHelpers.ForecasterBuilder to create a Forecaster instance
        try {
            return TestHelpers.ForecasterBuilder.newInstance().build();
        } catch (IOException e) {
            fail("Failed to create test Forecaster: " + e.getMessage());
            return null;
        }
    }
}
