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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.ad.mock.model;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class MockSimpleLog implements ToXContentObject, Writeable {

    public static final String TIME_FIELD = "timestamp";
    public static final String VALUE_FIELD = "value";
    public static final String CATEGORY_FIELD = "category";
    public static final String IS_ERROR_FIELD = "is_error";
    public static final String MESSAGE_FIELD = "message";

    public static final String INDEX_MAPPING = "{\"mappings\":{\"properties\":{"
        + "\""
        + TIME_FIELD
        + "\":{\"type\":\"date\",\"format\":\"strict_date_time||epoch_millis\"},"
        + "\""
        + VALUE_FIELD
        + "\":{\"type\":\"double\"},"
        + "\""
        + CATEGORY_FIELD
        + "\":{\"type\":\"keyword\"},"
        + "\""
        + IS_ERROR_FIELD
        + "\":{\"type\":\"boolean\"},"
        + "\""
        + MESSAGE_FIELD
        + "\":{\"type\":\"text\"}}}}";

    private Instant timestamp;
    private Double value;
    private String category;
    private Boolean isError;
    private String message;

    public MockSimpleLog(Instant timestamp, Double value, String category, Boolean isError, String message) {
        this.timestamp = timestamp;
        this.value = value;
        this.category = category;
        this.isError = isError;
        this.message = message;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInstant(timestamp);
        out.writeOptionalDouble(value);
        out.writeOptionalString(category);
        out.writeOptionalBoolean(isError);
        out.writeOptionalString(message);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (timestamp != null) {
            xContentBuilder.field(TIME_FIELD, timestamp.toEpochMilli());
        }
        if (value != null) {
            xContentBuilder.field(VALUE_FIELD, value);
        }
        if (category != null) {
            xContentBuilder.field(CATEGORY_FIELD, category);
        }
        if (isError != null) {
            xContentBuilder.field(IS_ERROR_FIELD, isError);
        }
        if (message != null) {
            xContentBuilder.field(MESSAGE_FIELD, message);
        }
        return xContentBuilder.endObject();
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Boolean getError() {
        return isError;
    }

    public void setError(Boolean error) {
        isError = error;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
