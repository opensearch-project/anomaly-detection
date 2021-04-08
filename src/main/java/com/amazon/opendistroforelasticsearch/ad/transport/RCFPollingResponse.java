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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class RCFPollingResponse extends ActionResponse implements ToXContentObject {
    public static final String TOTAL_UPDATES_KEY = "totalUpdates";

    private final long totalUpdates;

    public RCFPollingResponse(long totalUpdates) {
        this.totalUpdates = totalUpdates;
    }

    public RCFPollingResponse(StreamInput in) throws IOException {
        super(in);
        totalUpdates = in.readVLong();
    }

    public long getTotalUpdates() {
        return totalUpdates;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalUpdates);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOTAL_UPDATES_KEY, totalUpdates);
        builder.endObject();
        return builder;
    }
}
