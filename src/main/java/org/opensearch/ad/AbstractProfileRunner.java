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

package org.opensearch.ad;

import java.util.Locale;

import org.opensearch.ad.model.InitProgressProfile;

public abstract class AbstractProfileRunner {
    protected long requiredSamples;

    public AbstractProfileRunner(long requiredSamples) {
        this.requiredSamples = requiredSamples;
    }

    protected InitProgressProfile computeInitProgressProfile(long totalUpdates, long intervalMins) {
        float percent = Math.min((100.0f * totalUpdates) / requiredSamples, 100.0f);
        int neededPoints = (int) (requiredSamples - totalUpdates);
        return new InitProgressProfile(
            // rounding: 93.456 => 93%, 93.556 => 94%
            // Without Locale.ROOT, sometimes conversions use localized decimal digits
            // rather than the usual ASCII digits. See https://tinyurl.com/y5sdr5tp
            String.format(Locale.ROOT, "%.0f%%", percent),
            intervalMins * neededPoints,
            neededPoints
        );
    }
}
