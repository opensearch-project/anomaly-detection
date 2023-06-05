/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import java.io.IOException;
import java.util.Collection;

import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TestHelpers;

public class ForecastSerializationTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testStreamConstructor() throws IOException {
        Forecaster forecaster = TestHelpers.randomForecaster();

        BytesStreamOutput output = new BytesStreamOutput();

        forecaster.writeTo(output);
        NamedWriteableAwareStreamInput streamInput = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        Forecaster parsedForecaster = new Forecaster(streamInput);
        assertTrue(parsedForecaster.equals(forecaster));
    }

    public void testStreamConstructorNullUser() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setUser(null).build();

        BytesStreamOutput output = new BytesStreamOutput();

        forecaster.writeTo(output);
        NamedWriteableAwareStreamInput streamInput = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        Forecaster parsedForecaster = new Forecaster(streamInput);
        assertTrue(parsedForecaster.equals(forecaster));
    }

    public void testStreamConstructorNullUiMeta() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setUiMetadata(null).build();

        BytesStreamOutput output = new BytesStreamOutput();

        forecaster.writeTo(output);
        NamedWriteableAwareStreamInput streamInput = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        Forecaster parsedForecaster = new Forecaster(streamInput);
        assertTrue(parsedForecaster.equals(forecaster));
    }

    public void testStreamConstructorNullCustomResult() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setCustomResultIndex(null).build();

        BytesStreamOutput output = new BytesStreamOutput();

        forecaster.writeTo(output);
        NamedWriteableAwareStreamInput streamInput = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        Forecaster parsedForecaster = new Forecaster(streamInput);
        assertTrue(parsedForecaster.equals(forecaster));
    }

    public void testStreamConstructorNullImputationOption() throws IOException {
        Forecaster forecaster = TestHelpers.ForecasterBuilder.newInstance().setNullImputationOption().build();

        BytesStreamOutput output = new BytesStreamOutput();

        forecaster.writeTo(output);
        NamedWriteableAwareStreamInput streamInput = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        Forecaster parsedForecaster = new Forecaster(streamInput);
        assertTrue(parsedForecaster.equals(forecaster));
    }
}
