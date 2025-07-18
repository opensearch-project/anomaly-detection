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

package org.opensearch.timeseries.model;

import java.io.IOException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Profile output for detector initialization progress.  When the new detector is created, it is possible that
 * there hasn’t been enough continuous data in the index.  We need to use live data to initialize.
 * During initialization, we need to tell users progress (using a percentage), how many more
 *  shingles to go, and approximately how many minutes before the detector becomes operational
 *  if they keep their data stream continuous.
 *
 */
public class InitProgressProfile implements Writeable, ToXContent {
    // field name in toXContent
    public static final String PERCENTAGE = "percentage";
    public static final String ESTIMATED_MINUTES_LEFT = "estimated_minutes_left";
    public static final String NEEDED_SHINGLES = "needed_shingles";

    private final String percentage;
    private final long estimatedMinutesLeft;
    private final int neededShingles;

    public InitProgressProfile(String percentage, long estimatedMinutesLeft, int neededDataPoints) {
        super();
        this.percentage = percentage;
        this.estimatedMinutesLeft = estimatedMinutesLeft;
        this.neededShingles = neededDataPoints;
    }

    public InitProgressProfile(StreamInput in) throws IOException {
        percentage = in.readString();
        estimatedMinutesLeft = in.readVLong();
        neededShingles = in.readVInt();
    }

    public String getPercentage() {
        return percentage;
    }

    public long getEstimatedMinutesLeft() {
        return estimatedMinutesLeft;
    }

    public int getNeededDataPoints() {
        return neededShingles;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PERCENTAGE, percentage);
        if (estimatedMinutesLeft > 0) {
            builder.field(ESTIMATED_MINUTES_LEFT, estimatedMinutesLeft);
        }
        if (neededShingles > 0) {
            builder.field(NEEDED_SHINGLES, neededShingles);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(percentage);
        out.writeVLong(estimatedMinutesLeft);
        out.writeVInt(neededShingles);
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append(PERCENTAGE, percentage);
        if (estimatedMinutesLeft > 0) {
            builder.append(ESTIMATED_MINUTES_LEFT, estimatedMinutesLeft);
        }
        if (neededShingles > 0) {
            builder.append(NEEDED_SHINGLES, neededShingles);
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof InitProgressProfile) {
            InitProgressProfile other = (InitProgressProfile) obj;

            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(percentage, other.percentage);
            equalsBuilder.append(estimatedMinutesLeft, other.estimatedMinutesLeft);
            equalsBuilder.append(neededShingles, other.neededShingles);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(percentage).append(estimatedMinutesLeft).append(neededShingles).toHashCode();
    }
}
