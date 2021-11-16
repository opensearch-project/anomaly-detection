/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Assert;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class SearchTopAnomalyResultRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        SearchTopAnomalyResultRequest originalRequest = new SearchTopAnomalyResultRequest(
            "test-detector-id",
            "test-task-id",
            false,
            1,
            Arrays.asList("test-field"),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(10, ChronoUnit.DAYS),
            Instant.now().minus(2, ChronoUnit.DAYS)
        );

        BytesStreamOutput output = new BytesStreamOutput();
        originalRequest.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        SearchTopAnomalyResultRequest parsedRequest = new SearchTopAnomalyResultRequest(input);
        assertEquals(originalRequest.getDetectorId(), parsedRequest.getDetectorId());
        assertEquals(originalRequest.getTaskId(), parsedRequest.getTaskId());
        assertEquals(originalRequest.getHistorical(), parsedRequest.getHistorical());
        assertEquals(originalRequest.getSize(), parsedRequest.getSize());
        assertEquals(originalRequest.getCategoryFields(), parsedRequest.getCategoryFields());
        assertEquals(originalRequest.getOrder(), parsedRequest.getOrder());
        assertEquals(originalRequest.getStartTime(), parsedRequest.getStartTime());
        assertEquals(originalRequest.getEndTime(), parsedRequest.getEndTime());
    }

    public void testNullTaskIdIsValid() {
        SearchTopAnomalyResultRequest request = new SearchTopAnomalyResultRequest(
            "test-detector-id",
            null,
            false,
            1,
            Arrays.asList("test-field"),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(10, ChronoUnit.DAYS),
            Instant.now().minus(2, ChronoUnit.DAYS)
        );
        ActionRequestValidationException exception = request.validate();
        Assert.assertNull(exception);
    }

    public void testNullSizeIsValid() {
        SearchTopAnomalyResultRequest request = new SearchTopAnomalyResultRequest(
            "test-detector-id",
            "",
            false,
            null,
            Arrays.asList("test-field"),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(10, ChronoUnit.DAYS),
            Instant.now().minus(2, ChronoUnit.DAYS)
        );
        ActionRequestValidationException exception = request.validate();
        Assert.assertNull(exception);
    }

    public void testNullCategoryFieldIsValid() {
        SearchTopAnomalyResultRequest request = new SearchTopAnomalyResultRequest(
            "test-detector-id",
            "",
            false,
            1,
            null,
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(10, ChronoUnit.DAYS),
            Instant.now().minus(2, ChronoUnit.DAYS)
        );
        ActionRequestValidationException exception = request.validate();
        Assert.assertNull(exception);
    }

    public void testEmptyCategoryFieldIsValid() {
        SearchTopAnomalyResultRequest request = new SearchTopAnomalyResultRequest(
            "test-detector-id",
            "",
            false,
            1,
            new ArrayList<>(),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(10, ChronoUnit.DAYS),
            Instant.now().minus(2, ChronoUnit.DAYS)
        );
        ActionRequestValidationException exception = request.validate();
        Assert.assertNull(exception);
    }

    public void testEmptyStartTimeIsInvalid() {
        SearchTopAnomalyResultRequest request = new SearchTopAnomalyResultRequest(
            "test-detector-id",
            "",
            false,
            1,
            new ArrayList<>(),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            null,
            Instant.now().minus(2, ChronoUnit.DAYS)
        );
        ActionRequestValidationException exception = request.validate();
        Assert.assertNotNull(exception);
    }

    public void testEmptyEndTimeIsInvalid() {
        SearchTopAnomalyResultRequest request = new SearchTopAnomalyResultRequest(
            "test-detector-id",
            "",
            false,
            1,
            new ArrayList<>(),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(10, ChronoUnit.DAYS),
            null
        );
        ActionRequestValidationException exception = request.validate();
        Assert.assertNotNull(exception);
    }

    public void testEndTimeBeforeStartTimeIsInvalid() {
        SearchTopAnomalyResultRequest request = new SearchTopAnomalyResultRequest(
            "test-detector-id",
            "",
            false,
            1,
            new ArrayList<>(),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(2, ChronoUnit.DAYS),
            Instant.now().minus(10, ChronoUnit.DAYS)
        );
        ActionRequestValidationException exception = request.validate();
        Assert.assertNotNull(exception);
    }
}
