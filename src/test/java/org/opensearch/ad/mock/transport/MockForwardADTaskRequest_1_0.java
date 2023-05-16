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

package org.opensearch.ad.mock.transport;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.commons.authuser.User;

public class MockForwardADTaskRequest_1_0 extends ActionRequest {
    private AnomalyDetector detector;
    private User user;
    private MockADTaskAction_1_0 adTaskAction;

    public MockForwardADTaskRequest_1_0(AnomalyDetector detector, User user, MockADTaskAction_1_0 adTaskAction) {
        this.detector = detector;
        this.user = user;
        this.adTaskAction = adTaskAction;
    }

    public MockForwardADTaskRequest_1_0(StreamInput in) throws IOException {
        super(in);
        this.detector = new AnomalyDetector(in);
        if (in.readBoolean()) {
            this.user = new User(in);
        }
        this.adTaskAction = in.readEnum(MockADTaskAction_1_0.class);
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
            validationException = addValidationError(ADCommonMessages.DETECTOR_MISSING, validationException);
        } else if (detector.getDetectorId() == null) {
            validationException = addValidationError(ADCommonMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (adTaskAction == null) {
            validationException = addValidationError(ADCommonMessages.AD_TASK_ACTION_MISSING, validationException);
        }
        return validationException;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public User getUser() {
        return user;
    }

    public MockADTaskAction_1_0 getAdTaskAction() {
        return adTaskAction;
    }
}
