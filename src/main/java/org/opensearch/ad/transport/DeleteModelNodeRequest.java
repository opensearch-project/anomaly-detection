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
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

/**
 *  Delete model represents the request to an individual node
 */
public class DeleteModelNodeRequest extends BaseNodeRequest {

    private String adID;

    DeleteModelNodeRequest() {}

    DeleteModelNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.adID = in.readString();
    }

    DeleteModelNodeRequest(DeleteModelRequest request) {
        this.adID = request.getAdID();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(adID);
    }

    public String getAdID() {
        return adID;
    }
}
