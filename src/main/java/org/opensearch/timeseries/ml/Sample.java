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

package org.opensearch.timeseries.ml;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.constant.CommonName;

import com.google.common.base.Objects;

public class Sample implements ToXContentObject {
    private final double[] data;
    private final Instant dataStartTime;
    private final Instant dataEndTime;

    public Sample(double[] data, Instant dataStartTime, Instant dataEndTime) {
        this.data = data;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
    }

    // Invalid sample
    public Sample() {
        this.data = new double[0];
        this.dataStartTime = this.dataEndTime = Instant.MIN;
    }

    public double[] getValueList() {
        return data;
    }

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (data != null) {
            xContentBuilder.array(CommonName.VALUE_LIST_FIELD, data);
        }
        if (dataStartTime != null && dataStartTime != Instant.MIN) {
            xContentBuilder.field(CommonName.DATA_START_TIME_FIELD, dataStartTime.toEpochMilli());
        }
        if (dataEndTime != null && dataEndTime != Instant.MIN) {
            xContentBuilder.field(CommonName.DATA_END_TIME_FIELD, dataEndTime.toEpochMilli());
        }
        return xContentBuilder.endObject();
    }

    /**
     * Extract Sample fields out of a serialized Map, which is what we get from a get checkpoint call.
     * @param map serialized sample.
     * Example input map:
     *   Key: last_processed_sample, Value type: java.util.HashMap
     *   Key: data_end_time, Value type: java.lang.Long
     *    Value: 1695825364700, Type: java.lang.Long
     *   Key: data_start_time, Value type: java.lang.Long
     *    Value: 1695825304700, Type: java.lang.Long
     *   Key: value_list, Value type: java.util.ArrayList
     *    Item type: java.lang.Double
     *    Value: 8840.0, Type: java.lang.Double
     *   Key: feature_imputed, Value type: java.util.ArrayList
     *    Item type: java.lang.Boolean
     *    Value: true, Type: java.lang.Boolean
     * @return a Sample.
     */
    public static Sample extractSample(Map<String, Object> map) {
        // Extract and convert values from the map
        Long dataEndTimeLong = (Long) map.get(CommonName.DATA_END_TIME_FIELD);
        Long dataStartTimeLong = (Long) map.get(CommonName.DATA_START_TIME_FIELD);
        List<Double> valueList = (List<Double>) map.get(CommonName.VALUE_LIST_FIELD);

        // Check if all required keys are present in the map
        if (dataEndTimeLong == null && dataStartTimeLong == null && valueList == null) {
            return null;
        }

        // Convert List<Double> to double[]
        double[] data = valueList.stream().mapToDouble(Double::doubleValue).toArray();

        // Convert long to Instant
        Instant dataEndTime = Instant.ofEpochMilli(dataEndTimeLong);
        Instant dataStartTime = Instant.ofEpochMilli(dataStartTimeLong);

        return new Sample(data, dataStartTime, dataEndTime);
    }

    public boolean isInvalid() {
        return dataStartTime.compareTo(Instant.MIN) == 0 || dataEndTime.compareTo(Instant.MIN) == 0;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("data", Arrays.toString(data))
            .append("dataStartTime", dataStartTime)
            .append("dataEndTime", dataEndTime)
            .toString();
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Sample sample = (Sample) o;
        return Arrays.equals(data, sample.data)
            && dataStartTime.truncatedTo(ChronoUnit.MILLIS).equals(sample.dataStartTime.truncatedTo(ChronoUnit.MILLIS))
            && dataEndTime.truncatedTo(ChronoUnit.MILLIS).equals(sample.dataEndTime.truncatedTo(ChronoUnit.MILLIS));
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(Arrays.hashCode(data), dataStartTime.truncatedTo(ChronoUnit.MILLIS), dataEndTime.truncatedTo(ChronoUnit.MILLIS));
    }
}
