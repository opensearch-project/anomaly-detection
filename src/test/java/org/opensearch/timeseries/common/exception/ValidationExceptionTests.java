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

package org.opensearch.timeseries.common.exception;

import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;

public class ValidationExceptionTests extends OpenSearchTestCase {
    public void testConstructorDetector() {
        String message = randomAlphaOfLength(5);
        ValidationException exception = new ValidationException(message, ValidationIssueType.NAME, ValidationAspect.DETECTOR);
        assertEquals(ValidationIssueType.NAME, exception.getType());
        assertEquals(ValidationAspect.DETECTOR, exception.getAspect());
    }

    public void testConstructorModel() {
        String message = randomAlphaOfLength(5);
        ValidationException exception = new ValidationException(message, ValidationIssueType.CATEGORY, ValidationAspect.MODEL);
        assertEquals(ValidationIssueType.CATEGORY, exception.getType());
        assertEquals(ValidationAspect.getName(CommonName.MODEL_ASPECT), exception.getAspect());
    }

    public void testToString() {
        String message = randomAlphaOfLength(5);
        ValidationException exception = new ValidationException(message, ValidationIssueType.NAME, ValidationAspect.DETECTOR);
        String exceptionString = exception.toString();
        logger.info("exception string: " + exceptionString);
        ValidationException exceptionNoType = new ValidationException(message, ValidationIssueType.NAME, null);
        String exceptionStringNoType = exceptionNoType.toString();
        logger.info("exception string no type: " + exceptionStringNoType);
    }

    public void testForecasterAspect() {
        String message = randomAlphaOfLength(5);
        ValidationException exception = new ValidationException(message, ValidationIssueType.CATEGORY, ValidationAspect.FORECASTER);
        assertEquals(ValidationIssueType.CATEGORY, exception.getType());
        assertEquals(ValidationAspect.getName(ForecastCommonName.FORECASTER_ASPECT), exception.getAspect());
    }
}
