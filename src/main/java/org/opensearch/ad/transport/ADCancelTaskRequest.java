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

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADCancelTaskRequest extends BaseNodesRequest<ADCancelTaskRequest> {

    private String detectorId;
    private String detectorTaskId;
    private String userName;
    private String reason;

    public ADCancelTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readOptionalString();
        this.userName = in.readOptionalString();
        if (in.available() > 0) {
            this.detectorTaskId = in.readOptionalString();
            this.reason = in.readOptionalString();
        }
    }

    public ADCancelTaskRequest(String detectorId, String detectorTaskId, String userName, DiscoveryNode... nodes) {
        this(detectorId, detectorTaskId, userName, null, nodes);
    }

    public ADCancelTaskRequest(String detectorId, String detectorTaskId, String userName, String reason, DiscoveryNode... nodes) {
        super(nodes);
        this.detectorId = detectorId;
        this.detectorTaskId = detectorTaskId;
        this.userName = userName;
        this.reason = reason;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(detectorId)) {
            validationException = addValidationError(CommonErrorMessages.AD_ID_MISSING_MSG, validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(detectorId);
        out.writeOptionalString(userName);
        out.writeOptionalString(detectorTaskId);
        out.writeOptionalString(reason);
    }

    public String getDetectorId() {
        return detectorId;
    }

    public String getDetectorTaskId() {
        return detectorTaskId;
    }

    public String getUserName() {
        return userName;
    }

    public String getReason() {
        return reason;
    }
}
