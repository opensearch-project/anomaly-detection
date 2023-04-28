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

package org.opensearch.ad.common.exception;

import org.opensearch.ad.model.DetectorValidationIssueType;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.constant.CommonName;

public class ADValidationExceptionTests extends OpenSearchTestCase {
    public void testConstructorDetector() {
        String message = randomAlphaOfLength(5);
        ADValidationException exception = new ADValidationException(message, DetectorValidationIssueType.NAME, ValidationAspect.DETECTOR);
        assertEquals(DetectorValidationIssueType.NAME, exception.getType());
        assertEquals(ValidationAspect.DETECTOR, exception.getAspect());
    }

    public void testConstructorModel() {
        String message = randomAlphaOfLength(5);
        ADValidationException exception = new ADValidationException(message, DetectorValidationIssueType.CATEGORY, ValidationAspect.MODEL);
        assertEquals(DetectorValidationIssueType.CATEGORY, exception.getType());
        assertEquals(ValidationAspect.getName(CommonName.MODEL_ASPECT), exception.getAspect());
    }

    public void testToString() {
        String message = randomAlphaOfLength(5);
        ADValidationException exception = new ADValidationException(message, DetectorValidationIssueType.NAME, ValidationAspect.DETECTOR);
        String exceptionString = exception.toString();
        logger.info("exception string: " + exceptionString);
        ADValidationException exceptionNoType = new ADValidationException(message, DetectorValidationIssueType.NAME, null);
        String exceptionStringNoType = exceptionNoType.toString();
        logger.info("exception string no type: " + exceptionStringNoType);
    }
}
