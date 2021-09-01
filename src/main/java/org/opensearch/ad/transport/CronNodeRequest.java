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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.ad.util.Bwc;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

/**
 *  Delete model represents the request to an individual node
 */
public class CronNodeRequest extends BaseNodeRequest {

    private String requestId;

    public CronNodeRequest() {}

    public CronNodeRequest(String requestId) {
        this.requestId = requestId;
    }

    public CronNodeRequest(StreamInput in) throws IOException {
        super(in);
        if (Bwc.supportMultiCategoryFields(in.getVersion())) {
            requestId = in.readString();
        }
    }

    public String getRequestId() {
        return this.requestId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (Bwc.supportMultiCategoryFields(out.getVersion())) {
            out.writeString(requestId);
        }
    }
}
