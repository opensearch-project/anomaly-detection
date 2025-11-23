/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.test.OpenSearchTestCase;

public class InsightsJobResponseTests extends OpenSearchTestCase {

    public void testMessageConstructor() {
        String message = "Insights job started successfully";
        InsightsJobResponse response = new InsightsJobResponse(message);

        assertEquals(message, response.getMessage());
        assertNotNull(response.getResults());
        assertTrue(response.getResults().isEmpty());
        assertEquals(0L, response.getTotalHits());
    }

    public void testResultsConstructor() {
        List<String> results = Arrays
            .asList("{\"doc_id\":\"1\",\"generated_at\":1730505600000}", "{\"doc_id\":\"2\",\"generated_at\":1730505660000}");
        long totalHits = 2L;

        InsightsJobResponse response = new InsightsJobResponse(results, totalHits);

        assertNull(response.getMessage());
        assertEquals(results, response.getResults());
        assertEquals(totalHits, response.getTotalHits());
    }

    public void testStatusConstructor() {
        String jobName = "insights-job";
        Boolean isEnabled = true;
        Instant enabledTime = Instant.now();
        Instant disabledTime = null;
        Instant lastUpdateTime = Instant.now();
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 24, ChronoUnit.HOURS);

        InsightsJobResponse response = new InsightsJobResponse(jobName, isEnabled, enabledTime, disabledTime, lastUpdateTime, schedule);

        assertNull(response.getMessage());
        assertNotNull(response.getResults());
        assertTrue(response.getResults().isEmpty());
        assertEquals(0L, response.getTotalHits());
    }

    public void testStatusConstructorWithDisabledJob() {
        String jobName = "insights-job";
        Boolean isEnabled = false;
        Instant enabledTime = Instant.now().minus(1, ChronoUnit.DAYS);
        Instant disabledTime = Instant.now();
        Instant lastUpdateTime = Instant.now();
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 24, ChronoUnit.HOURS);

        InsightsJobResponse response = new InsightsJobResponse(jobName, isEnabled, enabledTime, disabledTime, lastUpdateTime, schedule);

        assertNull(response.getMessage());
        assertNotNull(response.getResults());
        assertTrue(response.getResults().isEmpty());
    }

    public void testStatusConstructorWithNullSchedule() {
        String jobName = "insights-job";
        Boolean isEnabled = false;
        Instant enabledTime = null;
        Instant disabledTime = null;
        Instant lastUpdateTime = null;

        InsightsJobResponse response = new InsightsJobResponse(jobName, isEnabled, enabledTime, disabledTime, lastUpdateTime, null);

        assertNull(response.getMessage());
        assertNotNull(response.getResults());
        assertTrue(response.getResults().isEmpty());
    }

    public void testSerializationWithMessage() throws IOException {
        String message = "Test message";
        InsightsJobResponse original = new InsightsJobResponse(message);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        InsightsJobResponse deserialized = new InsightsJobResponse(input);

        assertEquals(original.getMessage(), deserialized.getMessage());
        assertEquals(original.getResults().size(), deserialized.getResults().size());
        assertEquals(original.getTotalHits(), deserialized.getTotalHits());
    }

    public void testSerializationWithResults() throws IOException {
        List<String> results = Arrays.asList("{\"id\":\"1\"}", "{\"id\":\"2\"}");
        long totalHits = 2L;
        InsightsJobResponse original = new InsightsJobResponse(results, totalHits);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        InsightsJobResponse deserialized = new InsightsJobResponse(input);

        assertNull(deserialized.getMessage());
        assertEquals(original.getResults().size(), deserialized.getResults().size());
        assertEquals(original.getTotalHits(), deserialized.getTotalHits());
    }

    public void testSerializationWithStatus() throws IOException {
        String jobName = "insights-job";
        Boolean isEnabled = true;
        Instant enabledTime = Instant.now();
        Instant disabledTime = null;
        Instant lastUpdateTime = Instant.now();
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES);

        InsightsJobResponse original = new InsightsJobResponse(jobName, isEnabled, enabledTime, disabledTime, lastUpdateTime, schedule);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        InsightsJobResponse deserialized = new InsightsJobResponse(input);

        assertNull(deserialized.getMessage());
        assertEquals(0, deserialized.getResults().size());
        assertEquals(0L, deserialized.getTotalHits());
    }

    public void testToXContentWithMessage() throws IOException {
        String message = "Job started";
        InsightsJobResponse response = new InsightsJobResponse(message);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"message\""));
        assertTrue(json.contains(message));
        assertFalse(json.contains("\"job_name\""));
        assertFalse(json.contains("\"total_hits\""));
    }

    public void testToXContentWithResults() throws IOException {
        List<String> results = Arrays.asList("{\"id\":\"1\"}");
        long totalHits = 1L;
        InsightsJobResponse response = new InsightsJobResponse(results, totalHits);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"total_hits\""));
        assertTrue(json.contains("\"results\""));
        assertFalse(json.contains("\"message\""));
        assertFalse(json.contains("\"job_name\""));
    }

    public void testToXContentWithStatus() throws IOException {
        String jobName = "insights-job";
        Boolean isEnabled = true;
        Instant enabledTime = Instant.ofEpochMilli(1730505600000L);
        Instant disabledTime = null;
        Instant lastUpdateTime = Instant.ofEpochMilli(1730509200000L);
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 24, ChronoUnit.HOURS);

        InsightsJobResponse response = new InsightsJobResponse(jobName, isEnabled, enabledTime, disabledTime, lastUpdateTime, schedule);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"job_name\""));
        assertTrue(json.contains(jobName));
        assertTrue(json.contains("\"enabled\""));
        assertTrue(json.contains("true"));
        assertTrue(json.contains("\"enabled_time\""));
        assertTrue(json.contains("1730505600000"));
        assertTrue(json.contains("\"last_update_time\""));
        assertTrue(json.contains("\"schedule\""));
        assertFalse(json.contains("\"message\""));
        assertFalse(json.contains("\"total_hits\""));
    }

    public void testToXContentWithStatusDisabled() throws IOException {
        String jobName = "insights-job";
        Boolean isEnabled = false;
        Instant enabledTime = Instant.ofEpochMilli(1730505600000L);
        Instant disabledTime = Instant.ofEpochMilli(1730509200000L);
        Instant lastUpdateTime = Instant.ofEpochMilli(1730509200000L);
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 24, ChronoUnit.HOURS);

        InsightsJobResponse response = new InsightsJobResponse(jobName, isEnabled, enabledTime, disabledTime, lastUpdateTime, schedule);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"job_name\""));
        assertTrue(json.contains("\"enabled\""));
        assertTrue(json.contains("false"));
        assertTrue(json.contains("\"disabled_time\""));
        assertTrue(json.contains("1730509200000"));
    }

    public void testToXContentWithStatusNullFields() throws IOException {
        String jobName = "insights-job";
        Boolean isEnabled = false;

        InsightsJobResponse response = new InsightsJobResponse(jobName, isEnabled, null, null, null, null);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"job_name\""));
        assertTrue(json.contains("\"enabled\""));
        assertTrue(json.contains("false"));
        assertFalse(json.contains("\"enabled_time\""));
        assertFalse(json.contains("\"disabled_time\""));
        assertFalse(json.contains("\"last_update_time\""));
        assertFalse(json.contains("\"schedule\""));
    }

    public void testEmptyResults() {
        List<String> emptyResults = Arrays.asList();
        InsightsJobResponse response = new InsightsJobResponse(emptyResults, 0L);

        assertEquals(0, response.getResults().size());
        assertEquals(0L, response.getTotalHits());
    }

    public void testLargeResultSet() {
        StringBuilder largeResult = new StringBuilder("{\"data\":[");
        for (int i = 0; i < 100; i++) {
            largeResult.append("\"item").append(i).append("\"");
            if (i < 99)
                largeResult.append(",");
        }
        largeResult.append("]}");

        List<String> results = Arrays.asList(largeResult.toString());
        InsightsJobResponse response = new InsightsJobResponse(results, 100L);

        assertEquals(1, response.getResults().size());
        assertEquals(100L, response.getTotalHits());
    }

    public void testRoundTripSerialization() throws IOException {
        // Test with status response (most complex case)
        String jobName = "insights-job";
        Boolean isEnabled = true;
        Instant enabledTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Instant disabledTime = null;
        Instant lastUpdateTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 1, ChronoUnit.HOURS);

        InsightsJobResponse original = new InsightsJobResponse(jobName, isEnabled, enabledTime, disabledTime, lastUpdateTime, schedule);

        // Serialize
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        // Deserialize
        StreamInput input = output.bytes().streamInput();
        InsightsJobResponse deserialized = new InsightsJobResponse(input);

        // Serialize again
        BytesStreamOutput output2 = new BytesStreamOutput();
        deserialized.writeTo(output2);

        // Compare byte arrays
        assertArrayEquals(output.bytes().toBytesRef().bytes, output2.bytes().toBytesRef().bytes);
    }
}
