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

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Locale;

import org.junit.Ignore;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.TestHelpers;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

@Ignore
public class DetectionDateRangeTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testParseDetectionDateRangeWithNullStartTime() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DetectionDateRange(null, Instant.now())
        );
        assertEquals("Detection data range's start time must not be null", exception.getMessage());
    }

    public void testParseDetectionDateRangeWithNullEndTime() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DetectionDateRange(Instant.now(), null)
        );
        assertEquals("Detection data range's end time must not be null", exception.getMessage());
    }

    public void testInvalidDateRange() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DetectionDateRange(Instant.now(), Instant.now().minus(10, ChronoUnit.MINUTES))
        );
        assertEquals("Detection data range's end time must be after start time", exception.getMessage());
    }

    public void testSerializeDetectoinDateRange() throws IOException {
        DetectionDateRange dateRange = TestHelpers.randomDetectionDateRange();
        BytesStreamOutput output = new BytesStreamOutput();
        dateRange.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        DetectionDateRange parsedDateRange = new DetectionDateRange(input);
        assertTrue(parsedDateRange.equals(dateRange));
    }

    public void testParseDetectionDateRange() throws IOException {
        DetectionDateRange dateRange = TestHelpers.randomDetectionDateRange();
        String dateRangeString = TestHelpers.xContentBuilderToString(dateRange.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        dateRangeString = dateRangeString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));
        DetectionDateRange parsedDateRange = DetectionDateRange.parse(TestHelpers.parser(dateRangeString));
        assertEquals("Parsing detection range doesn't work", dateRange, parsedDateRange);
    }

}
