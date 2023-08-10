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

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class IndexAnomalyDetectorResponse extends ActionResponse implements ToXContentObject {
    private final String id;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;
    private final AnomalyDetector detector;
    private final RestStatus restStatus;

    public IndexAnomalyDetectorResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        version = in.readLong();
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        detector = new AnomalyDetector(in);
        restStatus = in.readEnum(RestStatus.class);
    }

    public IndexAnomalyDetectorResponse(
        String id,
        long version,
        long seqNo,
        long primaryTerm,
        AnomalyDetector detector,
        RestStatus restStatus
    ) {
        this.id = id;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.detector = detector;
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
        detector.writeTo(out);
        out.writeEnum(restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(RestHandlerUtils._ID, id)
            .field(RestHandlerUtils._VERSION, version)
            .field(RestHandlerUtils._SEQ_NO, seqNo)
            .field(RestHandlerUtils.ANOMALY_DETECTOR, detector)
            .field(RestHandlerUtils._PRIMARY_TERM, primaryTerm)
            .endObject();
    }
}
