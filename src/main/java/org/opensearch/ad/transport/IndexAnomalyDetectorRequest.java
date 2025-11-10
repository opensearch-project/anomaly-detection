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

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.DocRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.RestRequest;

public class IndexAnomalyDetectorRequest extends ActionRequest implements DocRequest {

    private String detectorID;
    private long seqNo;
    private long primaryTerm;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private AnomalyDetector detector;
    private RestRequest.Method method;
    private TimeValue requestTimeout;
    private Integer maxSingleEntityAnomalyDetectors;
    private Integer maxMultiEntityAnomalyDetectors;
    private Integer maxAnomalyFeatures;
    // added during refactoring for forecasting. It is fine we add a new field
    // since the request is handled by the same node.
    private Integer maxCategoricalFields;

    public IndexAnomalyDetectorRequest(StreamInput in) throws IOException {
        super(in);
        detectorID = in.readString();
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        refreshPolicy = in.readEnum(WriteRequest.RefreshPolicy.class);
        detector = new AnomalyDetector(in);
        method = in.readEnum(RestRequest.Method.class);
        requestTimeout = in.readTimeValue();
        maxSingleEntityAnomalyDetectors = in.readOptionalInt();
        maxMultiEntityAnomalyDetectors = in.readOptionalInt();
        maxAnomalyFeatures = in.readOptionalInt();
        maxCategoricalFields = in.readOptionalInt();
    }

    public IndexAnomalyDetectorRequest(
        String detectorID,
        long seqNo,
        long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector detector,
        RestRequest.Method method,
        TimeValue requestTimeout,
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures,
        Integer maxCategoricalFields
    ) {
        super();
        this.detectorID = detectorID;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.detector = detector;
        this.method = method;
        this.requestTimeout = requestTimeout;
        this.maxSingleEntityAnomalyDetectors = maxSingleEntityAnomalyDetectors;
        this.maxMultiEntityAnomalyDetectors = maxMultiEntityAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.maxCategoricalFields = maxCategoricalFields;
    }

    public IndexAnomalyDetectorRequest(String detectorID, AnomalyDetector detector, RestRequest.Method method) {
        this(
            detectorID,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            detector,
            method,
            TimeValue.timeValueSeconds(60),
            null,
            null,
            null,
            null
        );
    }

    public String getDetectorID() {
        return detectorID;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public RestRequest.Method getMethod() {
        return method;
    }

    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }

    public Integer getMaxSingleEntityAnomalyDetectors() {
        return maxSingleEntityAnomalyDetectors;
    }

    public Integer getMaxMultiEntityAnomalyDetectors() {
        return maxMultiEntityAnomalyDetectors;
    }

    public Integer getMaxAnomalyFeatures() {
        return maxAnomalyFeatures;
    }

    public Integer getMaxCategoricalFields() {
        return maxCategoricalFields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(detectorID);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        out.writeEnum(refreshPolicy);
        detector.writeTo(out);
        out.writeEnum(method);
        out.writeTimeValue(requestTimeout);
        out.writeOptionalInt(maxSingleEntityAnomalyDetectors);
        out.writeOptionalInt(maxMultiEntityAnomalyDetectors);
        out.writeOptionalInt(maxAnomalyFeatures);
        out.writeOptionalInt(maxCategoricalFields);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String type() {
        return ADCommonName.AD_RESOURCE_TYPE;
    }

    @Override
    public String index() {
        return ADIndex.CONFIG.getIndexName();
    }

    @Override
    public String id() {
        return detectorID;
    }

    public static IndexAnomalyDetectorRequest fromActionRequest(
        final ActionRequest actionRequest,
        org.opensearch.core.common.io.stream.NamedWriteableRegistry namedWriteableRegistry
    ) {
        if (actionRequest instanceof IndexAnomalyDetectorRequest) {
            return (IndexAnomalyDetectorRequest) actionRequest;
        }

        try (
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            org.opensearch.core.common.io.stream.OutputStreamStreamOutput osso =
                new org.opensearch.core.common.io.stream.OutputStreamStreamOutput(baos)
        ) {
            actionRequest.writeTo(osso);
            try (
                org.opensearch.core.common.io.stream.StreamInput input = new org.opensearch.core.common.io.stream.InputStreamStreamInput(
                    new java.io.ByteArrayInputStream(baos.toByteArray())
                );
                org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput namedInput =
                    new org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput(input, namedWriteableRegistry)
            ) {
                return new IndexAnomalyDetectorRequest(namedInput);
            }
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into IndexAnomalyDetectorRequest", e);
        }
    }
}
