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

package org.opensearch.forecast.transport;

import java.io.IOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class IndexForecasterResponse extends ActionResponse implements ToXContentObject {
    private final String id;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;
    private final Forecaster forecaster;
    private final RestStatus restStatus;

    public IndexForecasterResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        version = in.readLong();
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        forecaster = new Forecaster(in);
        restStatus = in.readEnum(RestStatus.class);
    }

    public IndexForecasterResponse(String id, long version, long seqNo, long primaryTerm, Forecaster forecaster, RestStatus restStatus) {
        this.id = id;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.forecaster = forecaster;
        this.restStatus = restStatus;
    }

    public String getId() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        forecaster.writeTo(out);
        out.writeEnum(restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(RestHandlerUtils._ID, id)
            .field(RestHandlerUtils._VERSION, version)
            .field(RestHandlerUtils._SEQ_NO, seqNo)
            .field(RestHandlerUtils.FORECASTER, forecaster)
            .field(RestHandlerUtils._PRIMARY_TERM, primaryTerm)
            .endObject();
    }
}
