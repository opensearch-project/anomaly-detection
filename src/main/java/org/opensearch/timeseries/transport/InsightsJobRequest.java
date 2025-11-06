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

package org.opensearch.timeseries.transport;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class InsightsJobRequest extends ActionRequest {

    private String frequency;
    private String detectorId;
    private String index;
    private int from;
    private int size;
    private String rawPath;

    /**
     * Constructor for start operation
     * @param frequency
     * @param rawPath 
     */
    public InsightsJobRequest(String frequency, String rawPath) {
        super();
        this.frequency = frequency;
        this.rawPath = rawPath;
        this.from = 0;
        this.size = 20;
    }

    /**
     * Constructor for get results operation with filters
     * @param detectorId
     * @param index
     * @param from
     * @param size
     * @param rawPath
     */
    public InsightsJobRequest(String detectorId, String index, int from, int size, String rawPath) {
        super();
        this.detectorId = detectorId;
        this.index = index;
        this.from = from;
        this.size = size;
        this.rawPath = rawPath;
    }

    /**
     * Constructor for stop operation
     * @param rawPath
     */
    public InsightsJobRequest(String rawPath) {
        super();
        this.rawPath = rawPath;
        this.from = 0;
        this.size = 20;
    }

    public InsightsJobRequest(StreamInput in) throws IOException {
        super(in);
        this.frequency = in.readOptionalString();
        this.detectorId = in.readOptionalString();
        this.index = in.readOptionalString();
        this.from = in.readInt();
        this.size = in.readInt();
        this.rawPath = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(frequency);
        out.writeOptionalString(detectorId);
        out.writeOptionalString(index);
        out.writeInt(from);
        out.writeInt(size);
        out.writeString(rawPath);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (rawPath != null && rawPath.contains("_results")) {
            if (from < 0) {
                validationException = new ActionRequestValidationException();
                validationException.addValidationError("from parameter must be non-negative");
            }
            if (size <= 0) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationError("size parameter must be positive");
            }
        }
        return validationException;
    }

    public String getFrequency() {
        return frequency;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public String getIndex() {
        return index;
    }

    public int getFrom() {
        return from;
    }

    public int getSize() {
        return size;
    }

    public String getRawPath() {
        return rawPath;
    }

    public boolean isStartOperation() {
        return rawPath != null && rawPath.contains("_start");
    }

    public boolean isStatusOperation() {
        return rawPath != null && rawPath.contains("_status");
    }

    public boolean isStopOperation() {
        return rawPath != null && rawPath.contains("_stop");
    }

    public boolean isResultsOperation() {
        return rawPath != null && rawPath.contains("_results");
    }
}
