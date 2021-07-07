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
import java.util.List;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.commons.authuser.User;

public class ForwardADTaskRequest extends ActionRequest {
    private AnomalyDetector detector;
    private ADTask adTask;
    private DetectionDateRange detectionDateRange;
    private List<String> staleRunningEntities;
    private User user;
    private ADTaskAction adTaskAction;

    public ForwardADTaskRequest(AnomalyDetector detector, DetectionDateRange detectionDateRange, User user, ADTaskAction adTaskAction) {
        this.detector = detector;
        this.detectionDateRange = detectionDateRange;
        this.user = user;
        this.adTaskAction = adTaskAction;
    }

    public ForwardADTaskRequest(ADTask adTask, ADTaskAction adTaskAction) {
        this(adTask, adTaskAction, null);
    }

    public ForwardADTaskRequest(ADTask adTask, ADTaskAction adTaskAction, List<String> staleRunningEntities) {
        this.adTask = adTask;
        this.adTaskAction = adTaskAction;
        if (adTask != null) {
            this.detector = adTask.getDetector();
        }
        this.staleRunningEntities = staleRunningEntities;
    }

    public ForwardADTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.detector = new AnomalyDetector(in);
        if (in.readBoolean()) {
            this.adTask = new ADTask(in);
        }
        if (in.readBoolean()) {
            this.detectionDateRange = new DetectionDateRange(in);
        }
        if (in.readBoolean()) {
            this.user = new User(in);
        }
        this.adTaskAction = in.readEnum(ADTaskAction.class);
        this.staleRunningEntities = in.readOptionalStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        detector.writeTo(out);
        if (adTask != null) {
            out.writeBoolean(true);
            adTask.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        if (detectionDateRange != null) {
            out.writeBoolean(true);
            detectionDateRange.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (user != null) {
            out.writeBoolean(true);
            user.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeEnum(adTaskAction);
        out.writeOptionalStringCollection(staleRunningEntities);
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
        if (adTaskAction == ADTaskAction.CLEAN_STALE_RUNNING_ENTITIES && (staleRunningEntities == null || staleRunningEntities.isEmpty())) {
            validationException = addValidationError(CommonErrorMessages.EMPTY_STALE_RUNNING_ENTITIES, validationException);
        }
        return validationException;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public ADTask getAdTask() {
        return adTask;
    }

    public DetectionDateRange getDetectionDateRange() {
        return detectionDateRange;
    }

    public User getUser() {
        return user;
    }

    public ADTaskAction getAdTaskAction() {
        return adTaskAction;
    }

    public List<String> getStaleRunningEntities() {
        return staleRunningEntities;
    }
}
