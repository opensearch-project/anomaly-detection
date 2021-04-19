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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class GetAnomalyDetectorRequest extends ActionRequest {

    private String detectorID;
    private long version;
    private boolean returnJob;
    private boolean returnTask;
    private String typeStr;
    private String rawPath;
    private boolean all;
    private String entityValue;

    public GetAnomalyDetectorRequest(StreamInput in) throws IOException {
        super(in);
        detectorID = in.readString();
        version = in.readLong();
        returnJob = in.readBoolean();
        returnTask = in.readBoolean();
        typeStr = in.readString();
        rawPath = in.readString();
        all = in.readBoolean();
        if (in.readBoolean()) {
            entityValue = in.readString();
        }
    }

    public GetAnomalyDetectorRequest(
        String detectorID,
        long version,
        boolean returnJob,
        boolean returnTask,
        String typeStr,
        String rawPath,
        boolean all,
        String entityValue
    ) {
        super();
        this.detectorID = detectorID;
        this.version = version;
        this.returnJob = returnJob;
        this.returnTask = returnTask;
        this.typeStr = typeStr;
        this.rawPath = rawPath;
        this.all = all;
        this.entityValue = entityValue;
    }

    public String getDetectorID() {
        return detectorID;
    }

    public long getVersion() {
        return version;
    }

    public boolean isReturnJob() {
        return returnJob;
    }

    public boolean isReturnTask() {
        return returnTask;
    }

    public String getTypeStr() {
        return typeStr;
    }

    public String getRawPath() {
        return rawPath;
    }

    public boolean isAll() {
        return all;
    }

    public String getEntityValue() {
        return entityValue;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(detectorID);
        out.writeLong(version);
        out.writeBoolean(returnJob);
        out.writeBoolean(returnTask);
        out.writeString(typeStr);
        out.writeString(rawPath);
        out.writeBoolean(all);
        if (this.entityValue != null) {
            out.writeBoolean(true);
            out.writeString(entityValue);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
