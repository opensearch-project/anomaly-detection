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

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.common.exception;

import org.opensearch.ad.model.DetectorValidationIssueType;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.test.OpenSearchTestCase;

public class ADValidationExceptionTests extends OpenSearchTestCase {
    public void testConstructor() {
        String message = randomAlphaOfLength(5);
        ADValidationException exception = new ADValidationException(message, DetectorValidationIssueType.NAME, ValidationAspect.DETECTOR);
        assertEquals(DetectorValidationIssueType.NAME, exception.getType());
        assertEquals(ValidationAspect.DETECTOR, exception.getAspect());
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
