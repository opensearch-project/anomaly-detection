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

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

public class MockADCancelTaskNodeRequest_1_0 extends TransportRequest {
    private String detectorId;
    private String userName;

    public MockADCancelTaskNodeRequest_1_0(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readOptionalString();
        this.userName = in.readOptionalString();
    }

    public MockADCancelTaskNodeRequest_1_0(String detectorId, String userName) {
        this.detectorId = detectorId;
        this.userName = userName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(detectorId);
        out.writeOptionalString(userName);
    }

    public String getDetectorId() {
        return detectorId;
    }

    public String getUserName() {
        return userName;
    }

}
