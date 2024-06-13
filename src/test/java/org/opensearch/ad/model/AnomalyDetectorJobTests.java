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
import java.util.Collection;
import java.util.Locale;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.Job;

public class AnomalyDetectorJobTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, TimeSeriesAnalyticsPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testParseAnomalyDetectorJob() throws IOException {
        Job anomalyDetectorJob = TestHelpers.randomJob();
        String anomalyDetectorJobString = TestHelpers
            .xContentBuilderToString(anomalyDetectorJob.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        anomalyDetectorJobString = anomalyDetectorJobString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));

        Job parsedAnomalyDetectorJob = Job.parse(TestHelpers.parser(anomalyDetectorJobString));
        assertEquals("Parsing anomaly detect result doesn't work", anomalyDetectorJob, parsedAnomalyDetectorJob);
    }

    public void testSerialization() throws IOException {
        Job anomalyDetectorJob = TestHelpers.randomJob();
        BytesStreamOutput output = new BytesStreamOutput();
        anomalyDetectorJob.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        Job parsedAnomalyDetectorJob = new Job(input);
        assertNotNull(parsedAnomalyDetectorJob);
    }
}
