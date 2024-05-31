/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;

public class ForecasterTests extends AbstractTimeSeriesTest {
    TimeConfiguration forecastInterval = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
    TimeConfiguration windowDelay = new IntervalTimeConfiguration(1, ChronoUnit.MINUTES);
    String forecasterId = "testId";
    Long version = 1L;
    String name = "testName";
    String description = "testDescription";
    String timeField = "testTimeField";
    List<String> indices = Collections.singletonList("testIndex");
    List<Feature> features = Collections.emptyList(); // Assuming no features for simplicity
    MatchAllQueryBuilder filterQuery = QueryBuilders.matchAllQuery();
    Integer shingleSize = 8;
    Map<String, Object> uiMetadata = new HashMap<>();
    Integer schemaVersion = 1;
    Instant lastUpdateTime = Instant.now();
    List<String> categoryFields = Arrays.asList("field1", "field2");
    User user = new User("testUser", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    String resultIndex = null;
    Integer horizon = 1;
    int recencyEmphasis = 20;
    int seasonality = 20;
    Integer customResultIndexMinSize = null;
    Integer customResultIndexMinAge = null;
    Integer customResultIndexTTL = null;

    public void testForecasterConstructor() {
        ImputationOption imputationOption = TestHelpers.randomImputationOption(0);

        Forecaster forecaster = new Forecaster(
            forecasterId,
            version,
            name,
            description,
            timeField,
            indices,
            features,
            filterQuery,
            forecastInterval,
            windowDelay,
            shingleSize,
            uiMetadata,
            schemaVersion,
            lastUpdateTime,
            categoryFields,
            user,
            resultIndex,
            horizon,
            imputationOption,
            recencyEmphasis,
            seasonality,
            randomIntBetween(1, 1000),
            customResultIndexMinSize,
            customResultIndexMinAge,
            customResultIndexTTL
        );

        assertEquals(forecasterId, forecaster.getId());
        assertEquals(version, forecaster.getVersion());
        assertEquals(name, forecaster.getName());
        assertEquals(description, forecaster.getDescription());
        assertEquals(timeField, forecaster.getTimeField());
        assertEquals(indices, forecaster.getIndices());
        assertEquals(features, forecaster.getFeatureAttributes());
        assertEquals(filterQuery, forecaster.getFilterQuery());
        assertEquals(forecastInterval, forecaster.getInterval());
        assertEquals(windowDelay, forecaster.getWindowDelay());
        assertEquals(shingleSize, forecaster.getShingleSize());
        assertEquals(uiMetadata, forecaster.getUiMetadata());
        assertEquals(schemaVersion, forecaster.getSchemaVersion());
        assertEquals(lastUpdateTime, forecaster.getLastUpdateTime());
        assertEquals(categoryFields, forecaster.getCategoryFields());
        assertEquals(user, forecaster.getUser());
        assertEquals(resultIndex, forecaster.getCustomResultIndex());
        assertEquals(horizon, forecaster.getHorizon());
        assertEquals(imputationOption, forecaster.getImputationOption());
    }

    public void testForecasterConstructorWithNullForecastInterval() {
        TimeConfiguration forecastInterval = null;

        ValidationException ex = expectThrows(ValidationException.class, () -> {
            new Forecaster(
                forecasterId,
                version,
                name,
                description,
                timeField,
                indices,
                features,
                filterQuery,
                forecastInterval,
                windowDelay,
                shingleSize,
                uiMetadata,
                schemaVersion,
                lastUpdateTime,
                categoryFields,
                user,
                resultIndex,
                horizon,
                TestHelpers.randomImputationOption(0),
                recencyEmphasis,
                seasonality,
                randomIntBetween(1, 1000),
                customResultIndexMinSize,
                customResultIndexMinAge,
                customResultIndexTTL
            );
        });

        MatcherAssert.assertThat(ex.getMessage(), containsString(ForecastCommonMessages.NULL_FORECAST_INTERVAL));
        MatcherAssert.assertThat(ex.getType(), is(ValidationIssueType.FORECAST_INTERVAL));
        MatcherAssert.assertThat(ex.getAspect(), is(ValidationAspect.FORECASTER));
    }

    public void testNegativeInterval() {
        var forecastInterval = new IntervalTimeConfiguration(0, ChronoUnit.MINUTES); // An interval less than or equal to zero

        ValidationException ex = expectThrows(ValidationException.class, () -> {
            new Forecaster(
                forecasterId,
                version,
                name,
                description,
                timeField,
                indices,
                features,
                filterQuery,
                forecastInterval,
                windowDelay,
                shingleSize,
                uiMetadata,
                schemaVersion,
                lastUpdateTime,
                categoryFields,
                user,
                resultIndex,
                horizon,
                TestHelpers.randomImputationOption(0),
                recencyEmphasis,
                seasonality,
                randomIntBetween(1, 1000),
                customResultIndexMinSize,
                customResultIndexMinAge,
                customResultIndexTTL
            );
        });

        MatcherAssert.assertThat(ex.getMessage(), containsString(ForecastCommonMessages.INVALID_FORECAST_INTERVAL));
        MatcherAssert.assertThat(ex.getType(), is(ValidationIssueType.FORECAST_INTERVAL));
        MatcherAssert.assertThat(ex.getAspect(), is(ValidationAspect.FORECASTER));
    }

    public void testMaxCategoryFieldsLimits() {
        List<String> categoryFields = Arrays.asList("field1", "field2", "field3");

        ValidationException ex = expectThrows(ValidationException.class, () -> {
            new Forecaster(
                forecasterId,
                version,
                name,
                description,
                timeField,
                indices,
                features,
                filterQuery,
                forecastInterval,
                windowDelay,
                shingleSize,
                uiMetadata,
                schemaVersion,
                lastUpdateTime,
                categoryFields,
                user,
                resultIndex,
                horizon,
                TestHelpers.randomImputationOption(0),
                recencyEmphasis,
                seasonality,
                randomIntBetween(1, 1000),
                customResultIndexMinSize,
                customResultIndexMinAge,
                customResultIndexTTL
            );
        });

        MatcherAssert.assertThat(ex.getMessage(), containsString(CommonMessages.getTooManyCategoricalFieldErr(2)));
        MatcherAssert.assertThat(ex.getType(), is(ValidationIssueType.CATEGORY));
        MatcherAssert.assertThat(ex.getAspect(), is(ValidationAspect.FORECASTER));
    }

    public void testBlankName() {
        String name = "";

        ValidationException ex = expectThrows(ValidationException.class, () -> {
            new Forecaster(
                forecasterId,
                version,
                name,
                description,
                timeField,
                indices,
                features,
                filterQuery,
                forecastInterval,
                windowDelay,
                shingleSize,
                uiMetadata,
                schemaVersion,
                lastUpdateTime,
                categoryFields,
                user,
                resultIndex,
                horizon,
                TestHelpers.randomImputationOption(0),
                recencyEmphasis,
                seasonality,
                randomIntBetween(1, 1000),
                customResultIndexMinSize,
                customResultIndexMinAge,
                customResultIndexTTL
            );
        });

        MatcherAssert.assertThat(ex.getMessage(), containsString(CommonMessages.EMPTY_NAME));
        MatcherAssert.assertThat(ex.getType(), is(ValidationIssueType.NAME));
        MatcherAssert.assertThat(ex.getAspect(), is(ValidationAspect.FORECASTER));
    }

    public void testInvalidCustomResultIndex() {
        String resultIndex = "test";

        ValidationException ex = expectThrows(ValidationException.class, () -> {
            new Forecaster(
                forecasterId,
                version,
                name,
                description,
                timeField,
                indices,
                features,
                filterQuery,
                forecastInterval,
                windowDelay,
                shingleSize,
                uiMetadata,
                schemaVersion,
                lastUpdateTime,
                categoryFields,
                user,
                resultIndex,
                horizon,
                TestHelpers.randomImputationOption(0),
                recencyEmphasis,
                seasonality,
                randomIntBetween(1, 1000),
                customResultIndexMinSize,
                customResultIndexMinAge,
                customResultIndexTTL
            );
        });

        MatcherAssert.assertThat(ex.getMessage(), containsString(ForecastCommonMessages.INVALID_RESULT_INDEX_PREFIX));
        MatcherAssert.assertThat(ex.getType(), is(ValidationIssueType.RESULT_INDEX));
        MatcherAssert.assertThat(ex.getAspect(), is(ValidationAspect.FORECASTER));
    }

    public void testValidCustomResultIndex() {
        String resultIndex = ForecastCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";

        var forecaster = new Forecaster(
            forecasterId,
            version,
            name,
            description,
            timeField,
            indices,
            features,
            filterQuery,
            forecastInterval,
            windowDelay,
            shingleSize,
            uiMetadata,
            schemaVersion,
            lastUpdateTime,
            categoryFields,
            user,
            resultIndex,
            horizon,
            TestHelpers.randomImputationOption(0),
            recencyEmphasis,
            seasonality,
            randomIntBetween(1, 1000),
            customResultIndexMinSize,
            customResultIndexMinAge,
            customResultIndexTTL
        );

        assertEquals(resultIndex, forecaster.getCustomResultIndex());
    }

    public void testInvalidHorizon() {
        int horizon = 0;

        ValidationException ex = expectThrows(ValidationException.class, () -> {
            new Forecaster(
                forecasterId,
                version,
                name,
                description,
                timeField,
                indices,
                features,
                filterQuery,
                forecastInterval,
                windowDelay,
                shingleSize,
                uiMetadata,
                schemaVersion,
                lastUpdateTime,
                categoryFields,
                user,
                resultIndex,
                horizon,
                TestHelpers.randomImputationOption(0),
                recencyEmphasis,
                seasonality,
                randomIntBetween(1, 1000),
                customResultIndexMinSize,
                customResultIndexMinAge,
                customResultIndexTTL
            );
        });

        MatcherAssert.assertThat(ex.getMessage(), containsString("Horizon size must be a positive integer no larger than"));
        MatcherAssert.assertThat(ex.getType(), is(ValidationIssueType.HORIZON_SIZE));
        MatcherAssert.assertThat(ex.getAspect(), is(ValidationAspect.FORECASTER));
    }

    public void testParse() throws IOException {
        Forecaster forecaster = TestHelpers.randomForecaster();
        String forecasterString = TestHelpers
            .xContentBuilderToString(forecaster.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(forecasterString);
        Forecaster parsedForecaster = Forecaster.parse(TestHelpers.parser(forecasterString));
        assertEquals("Parsing forecaster doesn't work", forecaster, parsedForecaster);
    }

    public void testParseEmptyMetaData() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setUiMetadata(null).build();
        String forecasterString = TestHelpers
            .xContentBuilderToString(forecaster.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(forecasterString);
        Forecaster parsedForecaster = Forecaster.parse(TestHelpers.parser(forecasterString));
        assertEquals("Parsing forecaster doesn't work", forecaster, parsedForecaster);
    }

    public void testParseNullLastUpdateTime() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setLastUpdateTime(null).build();
        String forecasterString = TestHelpers
            .xContentBuilderToString(forecaster.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(forecasterString);
        Forecaster parsedForecaster = Forecaster.parse(TestHelpers.parser(forecasterString));
        assertEquals("Parsing forecaster doesn't work", forecaster, parsedForecaster);
    }

    public void testParseNullCategoryFields() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setCategoryFields(null).build();
        String forecasterString = TestHelpers
            .xContentBuilderToString(forecaster.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(forecasterString);
        Forecaster parsedForecaster = Forecaster.parse(TestHelpers.parser(forecasterString));
        assertEquals("Parsing forecaster doesn't work", forecaster, parsedForecaster);
    }

    public void testParseNullUser() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setUser(null).build();
        String forecasterString = TestHelpers
            .xContentBuilderToString(forecaster.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(forecasterString);
        Forecaster parsedForecaster = Forecaster.parse(TestHelpers.parser(forecasterString));
        assertEquals("Parsing forecaster doesn't work", forecaster, parsedForecaster);
    }

    public void testParseNullCustomResultIndex() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setCustomResultIndex(null).build();
        String forecasterString = TestHelpers
            .xContentBuilderToString(forecaster.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(forecasterString);
        Forecaster parsedForecaster = Forecaster.parse(TestHelpers.parser(forecasterString));
        assertEquals("Parsing forecaster doesn't work", forecaster, parsedForecaster);
    }

    public void testParseNullImpute() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setNullImputationOption().build();
        String forecasterString = TestHelpers
            .xContentBuilderToString(forecaster.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        LOG.info(forecasterString);
        Forecaster parsedForecaster = Forecaster.parse(TestHelpers.parser(forecasterString));
        assertEquals("Parsing forecaster doesn't work", forecaster, parsedForecaster);
    }
}
