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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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

    public void testDetectorWithoutUiMetadata() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }

    public void testHCDetector() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields("testId", ImmutableList.of("category_field"));
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }

    public void testWithoutUser() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields("testId", ImmutableList.of("category_field"));
        detector.setUser(null);
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }

}
