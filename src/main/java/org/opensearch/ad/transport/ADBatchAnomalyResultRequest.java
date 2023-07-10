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

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.common.Strings;

public class ADBatchAnomalyResultRequest extends ActionRequest {
    private ADTask adTask;

    public ADBatchAnomalyResultRequest(StreamInput in) throws IOException {
        super(in);
        adTask = new ADTask(in);
    }

    public ADBatchAnomalyResultRequest(ADTask adTask) {
        super();
        this.adTask = adTask;
    }

    public ADTask getAdTask() {
        return adTask;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        adTask.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(adTask.getTaskId())) {
            validationException = addValidationError("Task id can't be null", validationException);
        }
        if (adTask.getDetectionDateRange() == null) {
            validationException = addValidationError("Detection date range can't be null for batch task", validationException);
        }
        AnomalyDetector detector = adTask.getDetector();
        if (detector == null) {
            validationException = addValidationError("Detector can't be null", validationException);
        }
        return validationException;
    }

}
