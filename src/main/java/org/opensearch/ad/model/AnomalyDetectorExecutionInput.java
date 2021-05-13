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

package org.opensearch.ad.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.ad.annotation.Generated;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import com.google.common.base.Objects;

/**
 * Input data needed to trigger anomaly detector.
 */
public class AnomalyDetectorExecutionInput implements ToXContentObject {

    private static final String DETECTOR_ID_FIELD = "detector_id";
    private static final String PERIOD_START_FIELD = "period_start";
    private static final String PERIOD_END_FIELD = "period_end";
    private static final String DETECTOR_FIELD = "detector";
    private Instant periodStart;
    private Instant periodEnd;
    private String detectorId;
    private AnomalyDetector detector;

    public AnomalyDetectorExecutionInput(String detectorId, Instant periodStart, Instant periodEnd, AnomalyDetector detector) {
        this.periodStart = periodStart;
        this.periodEnd = periodEnd;
        this.detectorId = detectorId;
        this.detector = detector;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(DETECTOR_ID_FIELD, detectorId)
            .field(PERIOD_START_FIELD, periodStart.toEpochMilli())
            .field(PERIOD_END_FIELD, periodEnd.toEpochMilli())
            .field(DETECTOR_FIELD, detector);
        return xContentBuilder.endObject();
    }

    public static AnomalyDetectorExecutionInput parse(XContentParser parser, String detectorId) throws IOException {
        Instant periodStart = null;
        Instant periodEnd = null;
        AnomalyDetector detector = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case PERIOD_START_FIELD:
                    periodStart = ParseUtils.toInstant(parser);
                    break;
                case PERIOD_END_FIELD:
                    periodEnd = ParseUtils.toInstant(parser);
                    break;
                case DETECTOR_FIELD:
                    XContentParser.Token token = parser.currentToken();
                    if (parser.currentToken().equals(XContentParser.Token.START_OBJECT)) {
                        detector = AnomalyDetector.parse(parser, detectorId);
                    }
                    break;
                default:
                    break;
            }
        }
        return new AnomalyDetectorExecutionInput(detectorId, periodStart, periodEnd, detector);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyDetectorExecutionInput that = (AnomalyDetectorExecutionInput) o;
        return Objects.equal(getPeriodStart(), that.getPeriodStart())
            && Objects.equal(getPeriodEnd(), that.getPeriodEnd())
            && Objects.equal(getDetectorId(), that.getDetectorId())
            && Objects.equal(getDetector(), that.getDetector());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(periodStart, periodEnd, detectorId);
    }

    public Instant getPeriodStart() {
        return periodStart;
    }

    public Instant getPeriodEnd() {
        return periodEnd;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public void setDetectorId(String detectorId) {
        this.detectorId = detectorId;
    }
}
