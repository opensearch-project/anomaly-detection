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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentParser;

/**
 * TimeConfiguration represents the time configuration for a job which runs regularly.
 */
public abstract class TimeConfiguration implements Writeable, ToXContentObject {

    public static final String PERIOD_FIELD = "period";
    public static final String INTERVAL_FIELD = "interval";
    public static final String UNIT_FIELD = "unit";

    /**
     * Parse raw json content into schedule instance.
     *
     * @param parser json based content parser
     * @return schedule instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static TimeConfiguration parse(XContentParser parser) throws IOException {
        long interval = 0;
        ChronoUnit unit = null;
        String scheduleType = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            scheduleType = parser.currentName();
            parser.nextToken();
            switch (scheduleType) {
                case PERIOD_FIELD:
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String periodFieldName = parser.currentName();
                        parser.nextToken();
                        switch (periodFieldName) {
                            case INTERVAL_FIELD:
                                interval = parser.longValue();
                                break;
                            case UNIT_FIELD:
                                unit = ChronoUnit.valueOf(parser.text().toUpperCase(Locale.ROOT));
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                default:
                    break;
            }
        }
        if (PERIOD_FIELD.equals(scheduleType)) {
            return new IntervalTimeConfiguration(interval, unit);
        }
        throw new IllegalArgumentException("Find no schedule definition");
    }
}
