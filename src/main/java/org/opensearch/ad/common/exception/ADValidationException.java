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
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.model.ValidationAspect;

public class ADValidationException extends AnomalyDetectionException {
    private final DetectorValidationIssueType type;
    private final ValidationAspect aspect;
    private final IntervalTimeConfiguration intervalSuggestion;

    public DetectorValidationIssueType getType() {
        return type;
    }

    public ValidationAspect getAspect() {
        return aspect;
    }

    public IntervalTimeConfiguration getIntervalSuggestion() {
        return intervalSuggestion;
    }

    public ADValidationException(String message, DetectorValidationIssueType type, ValidationAspect aspect) {
        this(message, null, type, aspect, null);
    }

    public ADValidationException(
        String message,
        DetectorValidationIssueType type,
        ValidationAspect aspect,
        IntervalTimeConfiguration intervalSuggestion
    ) {
        this(message, null, type, aspect, intervalSuggestion);
    }

    public ADValidationException(
        String message,
        Throwable cause,
        DetectorValidationIssueType type,
        ValidationAspect aspect,
        IntervalTimeConfiguration intervalSuggestion
    ) {
        super(AnomalyDetector.NO_ID, message, cause);
        this.type = type;
        this.aspect = aspect;
        this.intervalSuggestion = intervalSuggestion;
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

        if (intervalSuggestion != null) {
            sb.append("interval Suggestion: ");
            sb.append(intervalSuggestion.getInterval());
            sb.append(intervalSuggestion.getUnit());
        }

        return sb.toString();
    }
}
