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
package org.opensearch.ad.model;


public class AnomalyDetectorJobTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testParseAnomalyDetectorJob() throws IOException {
        AnomalyDetectorJob anomalyDetectorJob = TestHelpers.randomAnomalyDetectorJob();
        String anomalyDetectorJobString = TestHelpers
            .xContentBuilderToString(anomalyDetectorJob.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        anomalyDetectorJobString = anomalyDetectorJobString
            .replaceFirst("\\{", String.format(Locale.ROOT, "{\"%s\":\"%s\",", randomAlphaOfLength(5), randomAlphaOfLength(5)));

        AnomalyDetectorJob parsedAnomalyDetectorJob = AnomalyDetectorJob.parse(TestHelpers.parser(anomalyDetectorJobString));
        assertEquals("Parsing anomaly detect result doesn't work", anomalyDetectorJob, parsedAnomalyDetectorJob);
    }

    public void testSerialization() throws IOException {
        AnomalyDetectorJob anomalyDetectorJob = TestHelpers.randomAnomalyDetectorJob();
        BytesStreamOutput output = new BytesStreamOutput();
        anomalyDetectorJob.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetectorJob parsedAnomalyDetectorJob = new AnomalyDetectorJob(input);
        assertNotNull(parsedAnomalyDetectorJob);
    }
}
*/
