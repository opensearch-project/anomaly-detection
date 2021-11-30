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

import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.FEATURE_WITH_EMPTY_DATA_MSG;
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.FEATURE_WITH_INVALID_QUERY_MSG;
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG;

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Test;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssueType;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.aggregations.AggregationBuilder;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

public class ValidateAnomalyDetectorTransportActionTests extends ADIntegTestCase {

    @Test
    public void testValidateAnomalyDetectorWithNoIssue() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
    public void testValidateAnomalyDetectorWithNoIndexFound() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        System.out.println("response: " + response.getIssue());
        assertNotNull(response.getIssue());
        assertEquals(DetectorValidationIssueType.INDICES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(CommonErrorMessages.INDEX_NOT_FOUND));
    }

    @Test
    public void testValidateAnomalyDetectorWithDuplicateName() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
        Feature maxFeature = maxValueFeature(nameField, "non_existing_field", nameField);
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableList.of(maxFeature), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
    public void testValidateAnomalyDetectorWithDuplicateFeatureAggregationNames() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, "test-1");
        Feature maxFeatureTwo = maxValueFeature(nameField, categoryField, "test-2");
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(ImmutableList.of(maxFeature, maxFeatureTwo), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
        assertTrue(response.getIssue().getMessage().contains("Detector has duplicate feature aggregation query names:"));
        assertEquals(DetectorValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
    }

    @Test
    public void testValidateAnomalyDetectorWithDuplicateFeatureNamesAndDuplicateAggregationNames() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, nameField);
        Feature maxFeatureTwo = maxValueFeature(nameField, categoryField, nameField);
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(ImmutableList.of(maxFeature, maxFeatureTwo), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
        assertTrue(response.getIssue().getMessage().contains("Detector has duplicate feature aggregation query names:"));
        assertTrue(response.getIssue().getMessage().contains("Detector has duplicate feature names:"));
        assertEquals(DetectorValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
    }

    @Test
    public void testValidateAnomalyDetectorWithDuplicateFeatureNames() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, nameField);
        Feature maxFeatureTwo = maxValueFeature("test_1", categoryField, nameField);
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(ImmutableList.of(maxFeature, maxFeatureTwo), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
        assertTrue(response.getIssue().getMessage().contains("Detector has duplicate feature names:"));
        assertEquals(DetectorValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
    }

    @Test
    public void testValidateAnomalyDetectorWithInvalidFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, nameField);
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableList.of(maxFeature), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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

    @Test
    public void testValidateAnomalyDetectorWithUnknownFeatureField() throws IOException {
        AggregationBuilder aggregationBuilder = TestHelpers.parseAggregation("{\"test\":{\"terms\":{\"field\":\"type\"}}}");
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(new Feature(randomAlphaOfLength(5), nameField, true, aggregationBuilder)),
                ImmutableMap.of(),
                Instant.now()
            );
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
        assertTrue(response.getIssue().getMessage().contains(UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(nameField));
    }

    @Test
    public void testValidateAnomalyDetectorWithMultipleInvalidFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, nameField);
        Feature maxFeatureTwo = maxValueFeature("test_two", categoryField, "test_two");
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(ImmutableList.of(maxFeature, maxFeatureTwo), ImmutableMap.of(), Instant.now());
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
        assertEquals(response.getIssue().getSubIssues().keySet().size(), 2);
        assertEquals(DetectorValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(FEATURE_WITH_INVALID_QUERY_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(maxFeature.getName()));
        assertTrue(FEATURE_WITH_INVALID_QUERY_MSG.contains(response.getIssue().getSubIssues().get(maxFeature.getName())));
    }

    @Test
    public void testValidateAnomalyDetectorWithCustomResultIndex() throws IOException {
        String resultIndex = CommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        createCustomADResultIndex(resultIndex);
        AnomalyDetector anomalyDetector = TestHelpers
            .randomDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                randomAlphaOfLength(5).toLowerCase(),
                randomIntBetween(1, 5),
                randomAlphaOfLength(5),
                null,
                resultIndex
            );
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
    public void testValidateAnomalyDetectorWithCustomResultIndexCreated() throws IOException {
        testValidateAnomalyDetectorWithCustomResultIndex(true);
    }

    @Test
    public void testValidateAnomalyDetectorWithCustomResultIndexPresentButNotCreated() throws IOException {
        testValidateAnomalyDetectorWithCustomResultIndex(false);

    }

    @Test
    public void testValidateAnomalyDetectorWithCustomResultIndexWithInvalidMapping() throws IOException {
        String resultIndex = CommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource("mappings/checkpoint.json");
        createIndex(resultIndex, Resources.toString(url, Charsets.UTF_8));
        AnomalyDetector anomalyDetector = TestHelpers
            .randomDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                randomAlphaOfLength(5).toLowerCase(),
                randomIntBetween(1, 5),
                randomAlphaOfLength(5),
                null,
                resultIndex
            );
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertEquals(DetectorValidationIssueType.RESULT_INDEX, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(CommonErrorMessages.INVALID_RESULT_INDEX_MAPPING));
    }

    private void testValidateAnomalyDetectorWithCustomResultIndex(boolean resultIndexCreated) throws IOException {
        String resultIndex = CommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        if (resultIndexCreated) {
            createCustomADResultIndex(resultIndex);
        }
        AnomalyDetector anomalyDetector = TestHelpers
            .randomDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                randomAlphaOfLength(5).toLowerCase(),
                randomIntBetween(1, 5),
                randomAlphaOfLength(5),
                null,
                resultIndex
            );
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
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
    public void testValidateAnomalyDetectorWithInvalidDetectorName() throws IOException {
        AnomalyDetector anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            "#$32",
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5).toLowerCase()),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null
        );
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertEquals(DetectorValidationIssueType.NAME, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertEquals(CommonErrorMessages.INVALID_DETECTOR_NAME, response.getIssue().getMessage());
    }

    @Test
    public void testValidateAnomalyDetectorWithDetectorNameTooLong() throws IOException {
        AnomalyDetector anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            "abababababababababababababababababababababababababababababababababababababababababababababababab",
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5).toLowerCase()),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null
        );
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), startTime, 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertEquals(DetectorValidationIssueType.NAME, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains("Name should be shortened. The maximum limit is"));
    }
}
