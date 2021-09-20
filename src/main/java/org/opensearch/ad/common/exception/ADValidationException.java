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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorValidationIssueType;
import org.opensearch.ad.model.ValidationAspect;

public class ADValidationException extends AnomalyDetectionException {
    private final DetectorValidationIssueType type;
    private final ValidationAspect aspect;

    public DetectorValidationIssueType getType() {
        return type;
    }

    public ValidationAspect getAspect() {
        return aspect;
    }

    public ADValidationException(String message, DetectorValidationIssueType type, ValidationAspect aspect) {
        this(message, null, type, aspect);
    }

    public ADValidationException(String message, Throwable cause, DetectorValidationIssueType type, ValidationAspect aspect) {
        super(AnomalyDetector.NO_ID, message, cause);
        this.type = type;
        this.aspect = aspect;
    }

    @Override
    public String toString() {
        String superString = super.toString();
        StringBuilder sb = new StringBuilder(superString);
        if (type != null) {
            sb.append(" type: ");
            sb.append(type.getName());
        }

        if (aspect != null) {
            sb.append(" aspect: ");
            sb.append(aspect.getName());
        }

        return sb.toString();
    }
}
