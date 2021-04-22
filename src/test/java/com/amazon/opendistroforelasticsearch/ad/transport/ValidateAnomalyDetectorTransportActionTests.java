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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static com.amazon.opendistroforelasticsearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.FEATURE_WITH_EMPTY_DATA_MSG;
import static com.amazon.opendistroforelasticsearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.FEATURE_WITH_INVALID_QUERY_MSG;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Test;
import org.opensearch.common.unit.TimeValue;

import com.amazon.opendistroforelasticsearch.ad.ADIntegTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorValidationIssueType;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.ValidationAspect;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ValidateAnomalyDetectorTransportActionTests extends ADIntegTestCase {

    @Test
    public void testValidateAnomalyDetectorWithNoIssue() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestData(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertNull(response.getIssue());
    }

    @Test
    public void testValidateAnomalyDetectorWithDuplicateName() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestData(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        createDetectorIndex();
        createDetector(anomalyDetector);
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertNotNull(response.getIssue());
        assertEquals(DetectorValidationIssueType.NAME, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
    }

    @Test
    public void testValidateAnomalyDetectorWithNonExistingFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature("non_existing_field");
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableList.of(maxFeature), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestData(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertNotNull(response.getIssue());
        assertEquals(DetectorValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(FEATURE_WITH_EMPTY_DATA_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(maxFeature.getName()));
        assertTrue(FEATURE_WITH_EMPTY_DATA_MSG.contains(response.getIssue().getSubIssues().get(maxFeature.getName())));
    }

    @Test
    public void testValidateAnomalyDetectorWithInvalidFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature(categoryField);
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableList.of(maxFeature), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestData(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertNotNull(response.getIssue());
        assertEquals(DetectorValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(FEATURE_WITH_INVALID_QUERY_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(maxFeature.getName()));
        assertTrue(FEATURE_WITH_INVALID_QUERY_MSG.contains(response.getIssue().getSubIssues().get(maxFeature.getName())));
    }
}
