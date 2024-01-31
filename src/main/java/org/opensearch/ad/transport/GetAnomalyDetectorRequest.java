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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.model.Entity;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class GetAnomalyDetectorRequest extends ActionRequest {

    private String detectorID;
    private long version;
    private boolean returnJob;
    private boolean returnTask;
    private String typeStr;
    private String rawPath;
    private boolean all;
    private Entity entity;

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
            entity = new Entity(in);
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
        Entity entity
    ) {
        super();
        this.detectorID = detectorID;
        this.version = version;
        this.returnJob = returnJob;
        this.returnTask = returnTask;
        this.typeStr = typeStr;
        this.rawPath = rawPath;
        this.all = all;
        this.entity = entity;
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

    public Entity getEntity() {
        return entity;
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
        if (this.entity != null) {
            out.writeBoolean(true);
            entity.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public static GetAnomalyDetectorRequest fromActionRequest(final ActionRequest actionRequest) {
        if (actionRequest instanceof GetAnomalyDetectorRequest) {
            return (GetAnomalyDetectorRequest) actionRequest;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new GetAnomalyDetectorRequest(input);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into GetAnomalyDetectorRequest", e);
        }
    }
}
