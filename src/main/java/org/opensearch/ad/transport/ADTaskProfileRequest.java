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
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.common.Strings;

public class ADTaskProfileRequest extends BaseNodesRequest<ADTaskProfileRequest> {

    private String detectorId;

    public ADTaskProfileRequest(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readString();
    }

    public ADTaskProfileRequest(String detectorId, DiscoveryNode... nodes) {
        super(nodes);
        this.detectorId = detectorId;
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
        out.writeString(detectorId);
    }

    public String getDetectorId() {
        return detectorId;
    }
}
