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
import org.opensearch.timeseries.model.DateRange;

public class JobRequest extends ActionRequest {

    private String configID;
    // data start/end time. See ADBatchTaskRunner.getDateRangeOfSourceData.
    private DateRange dateRange;
    private boolean historical;
    private String rawPath;

    public JobRequest(StreamInput in) throws IOException {
        super(in);
        configID = in.readString();
        rawPath = in.readString();
        if (in.readBoolean()) {
            dateRange = new DateRange(in);
        }
        historical = in.readBoolean();
    }

    public JobRequest(String detectorID, String rawPath) {
        this(detectorID, null, false, rawPath);
    }

    /**
     * Constructor function.
     *
     * The dateRange and historical boolean can be passed in individually.
     * The historical flag is for stopping analysis, the dateRange is for
     * starting analysis. It's ok if historical is true but dateRange is
     * null.
     *
     * @param configID config identifier
     * @param dateRange analysis date range
     * @param historical historical analysis or not
     * @param rawPath raw request path
     */
    public JobRequest(String configID, DateRange dateRange, boolean historical, String rawPath) {
        super();
        this.configID = configID;
        this.dateRange = dateRange;
        this.historical = historical;
        this.rawPath = rawPath;
    }

    public String getConfigID() {
        return configID;
    }

    public DateRange getDateRange() {
        return dateRange;
    }

    public String getRawPath() {
        return rawPath;
    }

    public boolean isHistorical() {
        return historical;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(configID);
        out.writeString(rawPath);
        if (dateRange != null) {
            out.writeBoolean(true);
            dateRange.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(historical);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
