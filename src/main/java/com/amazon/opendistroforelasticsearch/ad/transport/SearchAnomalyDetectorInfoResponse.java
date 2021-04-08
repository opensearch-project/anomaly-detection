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

import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;

public class SearchAnomalyDetectorInfoResponse extends ActionResponse implements ToXContentObject {
    private long count;
    private boolean nameExists;

    public SearchAnomalyDetectorInfoResponse(StreamInput in) throws IOException {
        super(in);
        count = in.readLong();
        nameExists = in.readBoolean();
    }

    public SearchAnomalyDetectorInfoResponse(long count, boolean nameExists) {
        this.count = count;
        this.nameExists = nameExists;
    }

    public long getCount() {
        return count;
    }

    public boolean isNameExists() {
        return nameExists;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(count);
        out.writeBoolean(nameExists);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RestHandlerUtils.COUNT, count);
        builder.field(RestHandlerUtils.MATCH, nameExists);
        builder.endObject();
        return builder;
    }
}
