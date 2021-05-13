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
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;

public class ForwardADTaskRequest extends ActionRequest {
    private AnomalyDetector detector;
    private User user;
    private ADTaskAction adTaskAction;

    public ForwardADTaskRequest(AnomalyDetector detector, User user, ADTaskAction adTaskAction) {
        this.detector = detector;
        this.user = user;
        this.adTaskAction = adTaskAction;
    }

    public ForwardADTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.detector = new AnomalyDetector(in);
        if (in.readBoolean()) {
            this.user = new User(in);
        }
        this.adTaskAction = in.readEnum(ADTaskAction.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        detector.writeTo(out);
        if (user != null) {
            out.writeBoolean(true);
            user.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeEnum(adTaskAction);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (detector == null) {
            validationException = addValidationError(CommonErrorMessages.DETECTOR_MISSING, validationException);
        } else if (detector.getDetectorId() == null) {
            validationException = addValidationError(CommonErrorMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (adTaskAction == null) {
            validationException = addValidationError(CommonErrorMessages.AD_TASK_ACTION_MISSING, validationException);
        }
        return validationException;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public User getUser() {
        return user;
    }

    public ADTaskAction getAdTaskAction() {
        return adTaskAction;
    }
}
