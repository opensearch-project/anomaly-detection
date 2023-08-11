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

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ADCancelTaskNodeRequest extends BaseNodeRequest {
    private String detectorId;
    private String detectorTaskId;
    private String userName;
    private String reason;

    public ADCancelTaskNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readOptionalString();
        this.userName = in.readOptionalString();
        if (in.available() > 0) {
            this.detectorTaskId = in.readOptionalString();
            this.reason = in.readOptionalString();
        }
    }

    public ADCancelTaskNodeRequest(ADCancelTaskRequest request) {
        this.detectorId = request.getDetectorId();
        this.detectorTaskId = request.getDetectorTaskId();
        this.userName = request.getUserName();
        this.reason = request.getReason();
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
