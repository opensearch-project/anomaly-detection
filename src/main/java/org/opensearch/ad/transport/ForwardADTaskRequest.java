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

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.cluster.ADVersionUtil;
import org.opensearch.ad.common.exception.ADVersionException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.commons.authuser.User;
import org.opensearch.transport.TransportService;

public class ForwardADTaskRequest extends ActionRequest {
    private AnomalyDetector detector;
    private ADTask adTask;
    private DetectionDateRange detectionDateRange;
    private List<String> staleRunningEntities;
    private User user;
    private Integer availableTaskSlots;
    private ADTaskAction adTaskAction;

    /**
     * Constructor function.
     * For most task actions, we only send ForwardADTaskRequest to node with same local AD version.
     * But it's possible that we need to clean up detector cache by sending FINISHED task action to
     * an old coordinating node when no task running for the detector.
     * Check {@link org.opensearch.ad.task.ADTaskManager#cleanDetectorCache(ADTask, TransportService, AnomalyDetectorFunction)}.
     *
     * @param detector detector
     * @param detectionDateRange detection date range
     * @param user user
     * @param adTaskAction AD task action
     * @param availableTaskSlots available task slots
     * @param remoteAdVersion AD version of remote node
     */
    public ForwardADTaskRequest(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ADTaskAction adTaskAction,
        Integer availableTaskSlots,
        Version remoteAdVersion
    ) {
        if (!ADVersionUtil.compatibleWithVersionOnOrAfter1_1(remoteAdVersion)) {
            throw new ADVersionException("Can't forward AD task request to node running AD version " + remoteAdVersion);
        }
        this.detector = detector;
        this.detectionDateRange = detectionDateRange;
        this.user = user;
        this.availableTaskSlots = availableTaskSlots;
        this.adTaskAction = adTaskAction;
    }

    public ForwardADTaskRequest(AnomalyDetector detector, DetectionDateRange detectionDateRange, User user, ADTaskAction adTaskAction) {
        this.detector = detector;
        this.detectionDateRange = detectionDateRange;
        this.user = user;
        this.adTaskAction = adTaskAction;
    }

    public ForwardADTaskRequest(ADTask adTask, ADTaskAction adTaskAction) {
        this(adTask, adTaskAction, null);
    }

    public ForwardADTaskRequest(ADTask adTask, Integer availableTaskSLots, ADTaskAction adTaskAction) {
        this(adTask, adTaskAction, null);
        this.availableTaskSlots = availableTaskSLots;
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
            this.user = new User(in);
        }
        this.adTaskAction = in.readEnum(ADTaskAction.class);
        if (in.available() == 0) { // Old version on or before 1.0 will send less fields.
            throw new ADVersionException("Can't process ForwardADTaskRequest of old version");
        }
        if (in.readBoolean()) {
            this.adTask = new ADTask(in);
        }
        if (in.readBoolean()) {
            this.detectionDateRange = new DetectionDateRange(in);
        }
        this.staleRunningEntities = in.readOptionalStringList();
        availableTaskSlots = in.readOptionalInt();
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
        // From AD 1.1, only forward AD task request to nodes with same local AD version
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
        out.writeOptionalStringCollection(staleRunningEntities);
        out.writeOptionalInt(availableTaskSlots);
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

    public Integer getAvailableTaskSLots() {
        return availableTaskSlots;
    }
}
