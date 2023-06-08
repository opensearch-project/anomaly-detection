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

package org.opensearch.ad.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.util.ParseUtils;

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

    public static AnomalyDetectorExecutionInput parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    public static AnomalyDetectorExecutionInput parse(XContentParser parser, String adId) throws IOException {
        Instant periodStart = null;
        Instant periodEnd = null;
        AnomalyDetector detector = null;
        String detectorId = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case DETECTOR_ID_FIELD:
                    detectorId = parser.text();
                    break;
                case PERIOD_START_FIELD:
                    periodStart = ParseUtils.toInstant(parser);
                    break;
                case PERIOD_END_FIELD:
                    periodEnd = ParseUtils.toInstant(parser);
                    break;
                case DETECTOR_FIELD:
                    if (parser.currentToken().equals(XContentParser.Token.START_OBJECT)) {
                        detector = AnomalyDetector.parse(parser, detectorId);
                    }
                    break;
                default:
                    break;
            }
        }
        if (!Strings.isNullOrEmpty(adId)) {
            detectorId = adId;
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
