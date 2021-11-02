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
