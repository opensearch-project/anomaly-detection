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
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentParser;

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
