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

package org.opensearch.ad.model;

import static org.opensearch.ad.constant.CommonErrorMessages.INVALID_CHAR_IN_RESULT_INDEX_NAME;
import static org.opensearch.ad.constant.CommonErrorMessages.INVALID_RESULT_INDEX_NAME_SIZE;
import static org.opensearch.ad.constant.CommonErrorMessages.INVALID_RESULT_INDEX_PREFIX;
import static org.opensearch.ad.constant.CommonName.CUSTOM_RESULT_INDEX_PREFIX;
import static org.opensearch.ad.model.AnomalyDetector.MAX_RESULT_INDEX_NAME_SIZE;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.common.exception.ADValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.index.query.MatchAllQueryBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AnomalyDetectorTests extends AbstractADTest {

    public void testParseAnomalyDetector() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseAnomalyDetectorWithCustomIndex() throws IOException {
        String resultIndex = CommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                randomAlphaOfLength(5),
                randomIntBetween(1, 5),
                randomAlphaOfLength(5),
                ImmutableList.of(randomAlphaOfLength(5)),
                resultIndex
            );
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing result index doesn't work", resultIndex, parsedDetector.getResultIndex());
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testAnomalyDetectorWithInvalidCustomIndex() throws Exception {
        String resultIndex = CommonName.CUSTOM_RESULT_INDEX_PREFIX + "test@@";
        TestHelpers
            .assertFailWith(
                ADValidationException.class,
                () -> (TestHelpers
                    .randomDetector(
                        ImmutableList.of(TestHelpers.randomFeature()),
                        randomAlphaOfLength(5),
                        randomIntBetween(1, 5),
                        randomAlphaOfLength(5),
                        ImmutableList.of(randomAlphaOfLength(5)),
                        resultIndex
                    ))
            );
    }

    public void testParseAnomalyDetectorWithoutParams() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder()));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseAnomalyDetectorWithCustomDetectionDelay() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder()));
        LOG.info(detectorString);
        TimeValue detectionInterval = new TimeValue(1, TimeUnit.MINUTES);
        TimeValue detectionWindowDelay = new TimeValue(10, TimeUnit.MINUTES);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyDetector parsedDetector = AnomalyDetector
            .parse(
                TestHelpers.parser(detectorString),
                detector.getDetectorId(),
                detector.getVersion(),
                detectionInterval,
                detectionWindowDelay
            );
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseSingleEntityAnomalyDetector() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                TestHelpers.randomUiMetadata(),
                Instant.now(),
                AnomalyDetectorType.SINGLE_ENTITY.name()
            );
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseHistoricalAnomalyDetectorWithoutUser() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(TestHelpers.randomFeature()),
                TestHelpers.randomUiMetadata(),
                Instant.now(),
                false,
                null
            );
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(detectorString);
        detectorString = detectorString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testParseAnomalyDetectorWithNullFilterQuery() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertTrue(parsedDetector.getFilterQuery() instanceof MatchAllQueryBuilder);
    }

    public void testParseAnomalyDetectorWithEmptyFilterQuery() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\":"
            + "true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"filter_query\":{},"
            + "\"detection_interval\":{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":"
            + "{\"period\":{\"interval\":973,\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},"
            + "\"last_update_time\":1568396089028}";
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertTrue(parsedDetector.getFilterQuery() instanceof MatchAllQueryBuilder);
    }

    public void testParseAnomalyDetectorWithWrongFilterQuery() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\":"
            + "true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"filter_query\":"
            + "{\"aa\":\"bb\"},\"detection_interval\":{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},"
            + "\"window_delay\":{\"period\":{\"interval\":973,\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":"
            + "-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\","
            + "\"feature_enabled\":false,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},"
            + "\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ADValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithoutOptionalParams() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString), "id", 1L, null, null);
        assertTrue(parsedDetector.getFilterQuery() instanceof MatchAllQueryBuilder);
        assertEquals((long) parsedDetector.getShingleSize(), (long) AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE);
    }

    public void testParseAnomalyDetectorWithInvalidShingleSize() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"shingle_size\":-1,\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ADValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithNegativeWindowDelay() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":-973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ADValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithNegativeDetectionInterval() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":-425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ADValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithIncorrectFeatureQuery() throws Exception {
        String detectorString = "{\"name\":\"todagdpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":\"bb\"}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ADValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
    }

    public void testParseAnomalyDetectorWithInvalidDetectorIntervalUnits() {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Millis\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> AnomalyDetector.parse(TestHelpers.parser(detectorString))
        );
        assertEquals(
            String.format(Locale.ROOT, CommonErrorMessages.INVALID_TIME_CONFIGURATION_UNITS, ChronoUnit.MILLIS),
            exception.getMessage()
        );
    }

    public void testParseAnomalyDetectorInvalidWindowDelayUnits() {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Millis\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> AnomalyDetector.parse(TestHelpers.parser(detectorString))
        );
        assertEquals(
            String.format(Locale.ROOT, CommonErrorMessages.INVALID_TIME_CONFIGURATION_UNITS, ChronoUnit.MILLIS),
            exception.getMessage()
        );
    }

    public void testParseAnomalyDetectorWithNullUiMetadata() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
        assertNull(parsedDetector.getUiMetadata());
    }

    public void testParseAnomalyDetectorWithEmptyUiMetadata() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testInvalidShingleSize() throws Exception {
        TestHelpers
            .assertFailWith(
                ADValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    0,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null
                )
            );
    }

    public void testNullDetectorName() throws Exception {
        TestHelpers
            .assertFailWith(
                ADValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    null,
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
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
                )
            );
    }

    public void testBlankDetectorName() throws Exception {
        TestHelpers
            .assertFailWith(
                ADValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    "",
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
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
                )
            );
    }

    public void testNullTimeField() throws Exception {
        TestHelpers
            .assertFailWith(
                ADValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    null,
                    ImmutableList.of(randomAlphaOfLength(5)),
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
                )
            );
    }

    public void testNullIndices() throws Exception {
        TestHelpers
            .assertFailWith(
                ADValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    null,
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
                )
            );
    }

    public void testEmptyIndices() throws Exception {
        TestHelpers
            .assertFailWith(
                ADValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(),
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
                )
            );
    }

    public void testNullDetectionInterval() throws Exception {
        TestHelpers
            .assertFailWith(
                ADValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(TestHelpers.randomFeature()),
                    TestHelpers.randomQuery(),
                    null,
                    TestHelpers.randomIntervalTimeConfiguration(),
                    AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null
                )
            );
    }

    public void testInvalidDetectionInterval() {
        ADValidationException exception = expectThrows(
            ADValidationException.class,
            () -> new AnomalyDetector(
                randomAlphaOfLength(10),
                randomLong(),
                randomAlphaOfLength(20),
                randomAlphaOfLength(30),
                randomAlphaOfLength(5),
                ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
                ImmutableList.of(TestHelpers.randomFeature()),
                TestHelpers.randomQuery(),
                new IntervalTimeConfiguration(0, ChronoUnit.MINUTES),
                TestHelpers.randomIntervalTimeConfiguration(),
                randomIntBetween(1, 20),
                null,
                randomInt(),
                Instant.now(),
                null,
                null,
                null
            )
        );
        assertEquals("Detection interval must be a positive integer", exception.getMessage());
    }

    public void testInvalidWindowDelay() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new AnomalyDetector(
                randomAlphaOfLength(10),
                randomLong(),
                randomAlphaOfLength(20),
                randomAlphaOfLength(30),
                randomAlphaOfLength(5),
                ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
                ImmutableList.of(TestHelpers.randomFeature()),
                TestHelpers.randomQuery(),
                new IntervalTimeConfiguration(1, ChronoUnit.MINUTES),
                new IntervalTimeConfiguration(-1, ChronoUnit.MINUTES),
                randomIntBetween(1, 20),
                null,
                randomInt(),
                Instant.now(),
                null,
                null,
                null
            )
        );
        assertEquals("Interval -1 should be non-negative", exception.getMessage());
    }

    public void testNullFeatures() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, null, Instant.now().truncatedTo(ChronoUnit.SECONDS));
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals(0, parsedDetector.getFeatureAttributes().size());
    }

    public void testEmptyFeatures() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(ImmutableList.of(), null, Instant.now().truncatedTo(ChronoUnit.SECONDS));
        String detectorString = TestHelpers.xContentBuilderToString(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        assertEquals(0, parsedDetector.getFeatureAttributes().size());
    }

    public void testGetShingleSize() throws IOException {
        AnomalyDetector anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            5,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null
        );
        assertEquals((int) anomalyDetector.getShingleSize(), 5);
    }

    public void testGetShingleSizeReturnsDefaultValue() throws IOException {
        AnomalyDetector anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(TestHelpers.randomFeature()),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            null,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null
        );
        assertEquals((int) anomalyDetector.getShingleSize(), AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE);
    }

    public void testNullFeatureAttributes() throws IOException {
        AnomalyDetector anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            null,
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            null,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null
        );
        assertNotNull(anomalyDetector.getFeatureAttributes());
        assertEquals(0, anomalyDetector.getFeatureAttributes().size());
    }

    public void testValidateResultIndex() {
        String errorMessage = AnomalyDetector.validateResultIndex("abc");
        assertEquals(INVALID_RESULT_INDEX_PREFIX, errorMessage);

        StringBuilder resultIndexNameBuilder = new StringBuilder(CUSTOM_RESULT_INDEX_PREFIX);
        for (int i = 0; i < MAX_RESULT_INDEX_NAME_SIZE - CUSTOM_RESULT_INDEX_PREFIX.length(); i++) {
            resultIndexNameBuilder.append("a");
        }
        assertNull(AnomalyDetector.validateResultIndex(resultIndexNameBuilder.toString()));
        resultIndexNameBuilder.append("a");

        errorMessage = AnomalyDetector.validateResultIndex(resultIndexNameBuilder.toString());
        assertEquals(INVALID_RESULT_INDEX_NAME_SIZE, errorMessage);

        errorMessage = AnomalyDetector.validateResultIndex(CUSTOM_RESULT_INDEX_PREFIX + "abc#");
        assertEquals(INVALID_CHAR_IN_RESULT_INDEX_NAME, errorMessage);
    }
}
