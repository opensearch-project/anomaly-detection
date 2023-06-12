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

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.junit.Test;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

public class ValidateAnomalyDetectorTransportActionTests extends ADIntegTestCase {

    @Test
    public void testValidateAnomalyDetectorWithNoIssue() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(timeField, "test-index", ImmutableList.of(sumValueFeature(nameField, ipField + ".is_error", "test-2")));
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        assertNotNull(response.getIssue());
        assertEquals(ValidationIssueType.INDICES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(ADCommonMessages.INDEX_NOT_FOUND));
    }

    @Test
    public void testValidateAnomalyDetectorWithDuplicateName() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(timeField, "index-test");
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        assertEquals(ValidationIssueType.NAME, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
    }

    @Test
    public void testValidateAnomalyDetectorWithNonExistingFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, "non_existing_field", nameField);
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(timeField, "test-index", ImmutableList.of(maxFeature));
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        assertEquals(ValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(CommonMessages.FEATURE_WITH_EMPTY_DATA_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(maxFeature.getName()));
        assertTrue(CommonMessages.FEATURE_WITH_EMPTY_DATA_MSG.contains(response.getIssue().getSubIssues().get(maxFeature.getName())));
    }

    @Test
    public void testValidateAnomalyDetectorWithDuplicateFeatureAggregationNames() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, "test-1");
        Feature maxFeatureTwo = maxValueFeature(nameField, categoryField, "test-2");
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(timeField, "test-index", ImmutableList.of(maxFeature, maxFeatureTwo));
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        assertTrue(response.getIssue().getMessage().contains("Config has duplicate feature aggregation query names:"));
        assertEquals(ValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
    }

    @Test
    public void testValidateAnomalyDetectorWithDuplicateFeatureNamesAndDuplicateAggregationNames() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, nameField);
        Feature maxFeatureTwo = maxValueFeature(nameField, categoryField, nameField);
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(timeField, "test-index", ImmutableList.of(maxFeature, maxFeatureTwo));
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        assertTrue(response.getIssue().getMessage().contains("Config has duplicate feature aggregation query names:"));
        assertTrue(response.getIssue().getMessage().contains("There are duplicate feature names:"));
        assertEquals(ValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
    }

    @Test
    public void testValidateAnomalyDetectorWithDuplicateFeatureNames() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, nameField);
        Feature maxFeatureTwo = maxValueFeature("test_1", categoryField, nameField);
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(timeField, "test-index", ImmutableList.of(maxFeature, maxFeatureTwo));
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        assertTrue(
            "actual: " + response.getIssue().getMessage(),
            response.getIssue().getMessage().contains("There are duplicate feature names:")
        );
        assertEquals(ValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
    }

    @Test
    public void testValidateAnomalyDetectorWithInvalidFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, nameField);
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(timeField, "test-index", ImmutableList.of(maxFeature));
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        assertEquals(ValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(CommonMessages.FEATURE_WITH_INVALID_QUERY_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(maxFeature.getName()));
        assertTrue(CommonMessages.FEATURE_WITH_INVALID_QUERY_MSG.contains(response.getIssue().getSubIssues().get(maxFeature.getName())));
    }

    @Test
    public void testValidateAnomalyDetectorWithUnknownFeatureField() throws IOException {
        AggregationBuilder aggregationBuilder = TestHelpers.parseAggregation("{\"test\":{\"terms\":{\"field\":\"type\"}}}");
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(
                timeField,
                "test-index",
                ImmutableList.of(new Feature(randomAlphaOfLength(5), nameField, true, aggregationBuilder))
            );
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        assertEquals(ValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(CommonMessages.UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(nameField));
    }

    @Test
    public void testValidateAnomalyDetectorWithMultipleInvalidFeatureField() throws IOException {
        Feature maxFeature = maxValueFeature(nameField, categoryField, nameField);
        Feature maxFeatureTwo = maxValueFeature("test_two", categoryField, "test_two");
        AnomalyDetector anomalyDetector = TestHelpers
            .randomAnomalyDetector(timeField, "test-index", ImmutableList.of(maxFeature, maxFeatureTwo));
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        assertEquals(ValidationIssueType.FEATURE_ATTRIBUTES, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(CommonMessages.FEATURE_WITH_INVALID_QUERY_MSG));
        assertTrue(response.getIssue().getSubIssues().containsKey(maxFeature.getName()));
        assertTrue(CommonMessages.FEATURE_WITH_INVALID_QUERY_MSG.contains(response.getIssue().getSubIssues().get(maxFeature.getName())));
    }

    @Test
    public void testValidateAnomalyDetectorWithCustomResultIndex() throws IOException {
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        createCustomADResultIndex(resultIndex);
        AnomalyDetector anomalyDetector = TestHelpers
            .randomDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                randomAlphaOfLength(5).toLowerCase(Locale.ROOT),
                randomIntBetween(1, 5),
                timeField,
                null,
                resultIndex
            );
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource("mappings/anomaly-checkpoint.json");
        createIndex(resultIndex, Resources.toString(url, Charsets.UTF_8));
        AnomalyDetector anomalyDetector = TestHelpers
            .randomDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                randomAlphaOfLength(5).toLowerCase(Locale.ROOT),
                randomIntBetween(1, 5),
                timeField,
                null,
                resultIndex
            );
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertEquals(ValidationIssueType.RESULT_INDEX, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains(CommonMessages.INVALID_RESULT_INDEX_MAPPING));
    }

    private void testValidateAnomalyDetectorWithCustomResultIndex(boolean resultIndexCreated) throws IOException {
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        if (resultIndexCreated) {
            createCustomADResultIndex(resultIndex);
        }
        AnomalyDetector anomalyDetector = TestHelpers
            .randomDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                randomAlphaOfLength(5).toLowerCase(Locale.ROOT),
                randomIntBetween(1, 5),
                timeField,
                null,
                resultIndex
            );
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
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
            timeField,
            ImmutableList.of(randomAlphaOfLength(5).toLowerCase(Locale.ROOT)),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TimeSeriesSettings.DEFAULT_SHINGLE_SIZE,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null,
            TestHelpers.randomImputationOption()
        );
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertEquals(ValidationIssueType.NAME, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertEquals(CommonMessages.INVALID_NAME, response.getIssue().getMessage());
    }

    @Test
    public void testValidateAnomalyDetectorWithDetectorNameTooLong() throws IOException {
        AnomalyDetector anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            "abababababababababababababababababababababababababababababababababababababababababababababababab",
            randomAlphaOfLength(5),
            timeField,
            ImmutableList.of(randomAlphaOfLength(5).toLowerCase(Locale.ROOT)),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TimeSeriesSettings.DEFAULT_SHINGLE_SIZE,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null,
            TestHelpers.randomImputationOption()
        );
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertEquals(ValidationIssueType.NAME, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertTrue(response.getIssue().getMessage().contains("Name should be shortened. The maximum limit is"));
    }

    @Test
    public void testValidateAnomalyDetectorWithNonExistentTimefield() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertEquals(ValidationIssueType.TIMEFIELD_FIELD, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertEquals(
            String.format(Locale.ROOT, CommonMessages.NON_EXISTENT_TIMESTAMP, anomalyDetector.getTimeField()),
            response.getIssue().getMessage()
        );
    }

    @Test
    public void testValidateAnomalyDetectorWithNonDateTimeField() throws IOException {
        AnomalyDetector anomalyDetector = TestHelpers.randomAnomalyDetector(categoryField, "index-test");
        ingestTestDataValidate(anomalyDetector.getIndices().get(0), Instant.now().minus(1, ChronoUnit.DAYS), 1, "error");
        ValidateAnomalyDetectorRequest request = new ValidateAnomalyDetectorRequest(
            anomalyDetector,
            ValidationAspect.DETECTOR.getName(),
            5,
            5,
            5,
            new TimeValue(5_000L)
        );
        ValidateAnomalyDetectorResponse response = client().execute(ValidateAnomalyDetectorAction.INSTANCE, request).actionGet(5_000);
        assertEquals(ValidationIssueType.TIMEFIELD_FIELD, response.getIssue().getType());
        assertEquals(ValidationAspect.DETECTOR, response.getIssue().getAspect());
        assertEquals(
            String.format(Locale.ROOT, CommonMessages.INVALID_TIMESTAMP, anomalyDetector.getTimeField()),
            response.getIssue().getMessage()
        );
    }
}
