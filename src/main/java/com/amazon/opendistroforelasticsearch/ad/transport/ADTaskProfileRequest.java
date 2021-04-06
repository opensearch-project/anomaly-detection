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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;

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
