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

import static org.opensearch.ad.constant.ADCommonName.CUSTOM_RESULT_INDEX_PREFIX;
import static org.opensearch.timeseries.model.Config.MAX_RESULT_INDEX_NAME_SIZE;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AnomalyDetectorTests extends AbstractTimeSeriesTest {

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
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
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
        assertEquals("Parsing result index doesn't work", resultIndex, parsedDetector.getCustomResultIndex());
        assertEquals("Parsing anomaly detector doesn't work", detector, parsedDetector);
    }

    public void testAnomalyDetectorWithInvalidCustomIndex() throws Exception {
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test@@";
        TestHelpers
            .assertFailWith(
                ValidationException.class,
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
            .parse(TestHelpers.parser(detectorString), detector.getId(), detector.getVersion(), detectionInterval, detectionWindowDelay);
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
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
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
        assertEquals((long) parsedDetector.getShingleSize(), (long) TimeSeriesSettings.DEFAULT_SHINGLE_SIZE);
    }

    public void testParseAnomalyDetectorWithInvalidShingleSize() throws Exception {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"description\":"
            + "\"ClrcaMpuLfeDSlVduRcKlqPZyqWDBf\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"shingle_size\":-1,\"schema_version\":-1203962153,\"ui_metadata\":"
            + "{\"JbAaV\":{\"feature_id\":\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,"
            + "\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
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
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
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
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
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
        TestHelpers.assertFailWith(ValidationException.class, () -> AnomalyDetector.parse(TestHelpers.parser(detectorString)));
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
            String.format(Locale.ROOT, ADCommonMessages.INVALID_TIME_CONFIGURATION_UNITS, ChronoUnit.MILLIS),
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
            String.format(Locale.ROOT, ADCommonMessages.INVALID_TIME_CONFIGURATION_UNITS, ChronoUnit.MILLIS),
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
        Feature feature = TestHelpers.randomFeature();
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(feature),
                    TestHelpers.randomQuery(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TestHelpers.randomIntervalTimeConfiguration(),
                    0,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null,
                    TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                    randomIntBetween(1, 10000),
                    randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
                    randomIntBetween(1, 1000),
                    null,
                    null,
                    null,
                    null
                )
            );
    }

    public void testNullDetectorName() throws Exception {
        Feature feature = TestHelpers.randomFeature();
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    null,
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(feature),
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
                    TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                    randomIntBetween(1, 10000),
                    randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
                    randomIntBetween(1, 1000),
                    null,
                    null,
                    null,
                    null
                )
            );
    }

    public void testBlankDetectorName() throws Exception {
        Feature feature = TestHelpers.randomFeature();
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    "",
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(feature),
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
                    TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                    randomIntBetween(1, 10000),
                    randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
                    randomIntBetween(1, 1000),
                    null,
                    null,
                    null,
                    null
                )
            );
    }

    public void testNullTimeField() throws Exception {
        Feature feature = TestHelpers.randomFeature();
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    null,
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(feature),
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
                    TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                    randomIntBetween(1, 10000),
                    randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
                    randomIntBetween(1, 1000),
                    null,
                    null,
                    null,
                    null
                )
            );
    }

    public void testNullIndices() throws Exception {
        Feature feature = TestHelpers.randomFeature();
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    null,
                    ImmutableList.of(feature),
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
                    TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                    randomIntBetween(1, 10000),
                    randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
                    randomIntBetween(1, 1000),
                    null,
                    null,
                    null,
                    null
                )
            );
    }

    public void testEmptyIndices() throws Exception {
        Feature feature = TestHelpers.randomFeature();
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(),
                    ImmutableList.of(feature),
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
                    TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                    randomIntBetween(1, 10000),
                    randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
                    randomIntBetween(1, 1000),
                    null,
                    null,
                    null,
                    null
                )
            );
    }

    public void testNullDetectionInterval() throws Exception {
        Feature feature = TestHelpers.randomFeature();
        TestHelpers
            .assertFailWith(
                ValidationException.class,
                () -> new AnomalyDetector(
                    randomAlphaOfLength(5),
                    randomLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5)),
                    ImmutableList.of(feature),
                    TestHelpers.randomQuery(),
                    null,
                    TestHelpers.randomIntervalTimeConfiguration(),
                    TimeSeriesSettings.DEFAULT_SHINGLE_SIZE,
                    null,
                    1,
                    Instant.now(),
                    null,
                    TestHelpers.randomUser(),
                    null,
                    TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                    randomIntBetween(1, 10000),
                    randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
                    randomIntBetween(1, 1000),
                    null,
                    null,
                    null,
                    null
                )
            );
    }

    public void testInvalidRecency() {
        Feature feature = TestHelpers.randomFeature();
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> new AnomalyDetector(
                randomAlphaOfLength(10),
                randomLong(),
                randomAlphaOfLength(20),
                randomAlphaOfLength(30),
                randomAlphaOfLength(5),
                ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
                ImmutableList.of(feature),
                TestHelpers.randomQuery(),
                new IntervalTimeConfiguration(0, ChronoUnit.MINUTES),
                TestHelpers.randomIntervalTimeConfiguration(),
                randomIntBetween(1, 20),
                null,
                randomInt(),
                Instant.now(),
                null,
                null,
                null,
                TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                -1,
                randomIntBetween(1, 256),
                randomIntBetween(1, 1000),
                null,
                null,
                null,
                null
            )
        );
        assertEquals("recency emphasis has to be a positive integer", exception.getMessage());
    }

    public void testInvalidDetectionInterval() {
        Feature feature = TestHelpers.randomFeature();
        ValidationException exception = expectThrows(
            ValidationException.class,
            () -> new AnomalyDetector(
                randomAlphaOfLength(10),
                randomLong(),
                randomAlphaOfLength(20),
                randomAlphaOfLength(30),
                randomAlphaOfLength(5),
                ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
                ImmutableList.of(feature),
                TestHelpers.randomQuery(),
                new IntervalTimeConfiguration(0, ChronoUnit.MINUTES),
                TestHelpers.randomIntervalTimeConfiguration(),
                randomIntBetween(1, 20),
                null,
                randomInt(),
                Instant.now(),
                null,
                null,
                null,
                TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                null, // emphasis is not customized
                randomIntBetween(1, 256),
                randomIntBetween(1, 1000),
                null,
                null,
                null,
                null
            )
        );
        assertEquals("Detection interval must be a positive integer", exception.getMessage());
    }

    public void testInvalidWindowDelay() {
        Feature feature = TestHelpers.randomFeature();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new AnomalyDetector(
                randomAlphaOfLength(10),
                randomLong(),
                randomAlphaOfLength(20),
                randomAlphaOfLength(30),
                randomAlphaOfLength(5),
                ImmutableList.of(randomAlphaOfLength(10).toLowerCase(Locale.ROOT)),
                ImmutableList.of(feature),
                TestHelpers.randomQuery(),
                new IntervalTimeConfiguration(1, ChronoUnit.MINUTES),
                new IntervalTimeConfiguration(-1, ChronoUnit.MINUTES),
                randomIntBetween(1, 20),
                null,
                randomInt(),
                Instant.now(),
                null,
                null,
                null,
                TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
                null, // emphasis is not customized
                randomIntBetween(1, 256),
                randomIntBetween(1, 1000),
                null,
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
        Feature feature = TestHelpers.randomFeature();
        Config anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(feature),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            5,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null,
            TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null
        );
        assertEquals((int) anomalyDetector.getShingleSize(), 5);
    }

    public void testGetShingleSizeReturnsDefaultValue() throws IOException {
        int seasonalityIntervals = randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2);
        Feature feature = TestHelpers.randomFeature();
        Config anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(feature),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            null,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null,
            TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
            randomIntBetween(1, 10000),
            seasonalityIntervals,
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null
        );
        // seasonalityIntervals is not null and custom shingle size is null, use seasonalityIntervals to deterine shingle size
        assertEquals(seasonalityIntervals / TimeSeriesSettings.SEASONALITY_TO_SHINGLE_RATIO, (int) anomalyDetector.getShingleSize());

        anomalyDetector = new AnomalyDetector(
            randomAlphaOfLength(5),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(5)),
            ImmutableList.of(feature),
            TestHelpers.randomQuery(),
            TestHelpers.randomIntervalTimeConfiguration(),
            TestHelpers.randomIntervalTimeConfiguration(),
            null,
            null,
            1,
            Instant.now(),
            null,
            TestHelpers.randomUser(),
            null,
            TestHelpers.randomImputationOption(feature.getEnabled() ? 1 : 0),
            null,
            null,
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null
        );
        // seasonalityIntervals is null and custom shingle size is null, use default shingle size
        assertEquals(TimeSeriesSettings.DEFAULT_SHINGLE_SIZE, (int) anomalyDetector.getShingleSize());
    }

    public void testNullFeatureAttributes() throws IOException {
        Config anomalyDetector = new AnomalyDetector(
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
            null,
            TestHelpers.randomImputationOption(0),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null
        );
        assertNotNull(anomalyDetector.getFeatureAttributes());
        assertEquals(0, anomalyDetector.getFeatureAttributes().size());
    }

    public void testValidateResultIndex() throws IOException {
        Config anomalyDetector = new AnomalyDetector(
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
            null,
            TestHelpers.randomImputationOption(0),
            randomIntBetween(1, 10000),
            randomIntBetween(1, TimeSeriesSettings.MAX_SHINGLE_SIZE * TimeSeriesSettings.SEASONALITY_TO_SHINGLE_RATIO),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null
        );
        String errorMessage = anomalyDetector.validateCustomResultIndex("abc");
        assertEquals(ADCommonMessages.INVALID_RESULT_INDEX_PREFIX, errorMessage);

        StringBuilder resultIndexNameBuilder = new StringBuilder(CUSTOM_RESULT_INDEX_PREFIX);
        for (int i = 0; i < MAX_RESULT_INDEX_NAME_SIZE - CUSTOM_RESULT_INDEX_PREFIX.length(); i++) {
            resultIndexNameBuilder.append("a");
        }
        assertNull(anomalyDetector.validateCustomResultIndex(resultIndexNameBuilder.toString()));
        resultIndexNameBuilder.append("a");

        errorMessage = anomalyDetector.validateCustomResultIndex(resultIndexNameBuilder.toString());
        assertEquals(Config.INVALID_RESULT_INDEX_NAME_SIZE, errorMessage);

        errorMessage = anomalyDetector.validateCustomResultIndex(CUSTOM_RESULT_INDEX_PREFIX + "abc#");
        assertEquals(CommonMessages.INVALID_CHAR_IN_RESULT_INDEX_NAME, errorMessage);
    }

    public void testParseAnomalyDetectorWithNoDescription() throws IOException {
        String detectorString = "{\"name\":\"todagtCMkwpcaedpyYUM\",\"time_field\":\"dJRwh\",\"indices\":[\"eIrgWMqAED\"],"
            + "\"feature_attributes\":[{\"feature_id\":\"lxYRN\",\"feature_name\":\"eqSeU\",\"feature_enabled\""
            + ":true,\"aggregation_query\":{\"aa\":{\"value_count\":{\"field\":\"ok\"}}}}],\"detection_interval\":"
            + "{\"period\":{\"interval\":425,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":{\"interval\":973,"
            + "\"unit\":\"Minutes\"}},\"shingle_size\":4,\"schema_version\":-1203962153,\"ui_metadata\":{\"JbAaV\":{\"feature_id\":"
            + "\"rIFjS\",\"feature_name\":\"QXCmS\",\"feature_enabled\":false,\"aggregation_query\":{\"aa\":"
            + "{\"value_count\":{\"field\":\"ok\"}}}}},\"last_update_time\":1568396089028}";
        AnomalyDetector parsedDetector = AnomalyDetector.parse(TestHelpers.parser(detectorString), "id", 1L, null, null);
        assertEquals(parsedDetector.getDescription(), "");
    }
}
