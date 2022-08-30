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


public class AnomalyDetectorSerializationTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testDetectorWithUiMetadata() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }

    */
/*public void testDetectorWithoutUiMetadata() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }*//*
        
        
        */
/*public void testHCDetector() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields("testId", ImmutableList.of("category_field"));
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }*//*
        
        
        */
/* public void testWithoutUser() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields("testId", ImmutableList.of("category_field"));
        detector.setUser(null);
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }*//*
        
        
        }
        */
