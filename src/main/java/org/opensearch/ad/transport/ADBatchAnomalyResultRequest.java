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

package org.opensearch.ad.transport;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

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
        AnomalyDetector detector = adTask.getDetector();
        if (detector == null) {
            validationException = addValidationError("Detector can't be null", validationException);
        } else if (detector.isRealTimeDetector()) {
            validationException = addValidationError("Can't run batch task for realtime detector", validationException);
        }
        return validationException;
    }

}
