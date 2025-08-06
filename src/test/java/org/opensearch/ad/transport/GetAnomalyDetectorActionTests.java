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

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.Collection;

import org.junit.Assert;
import org.mockito.Mockito;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.GetConfigRequest;

import com.google.common.collect.ImmutableList;

/**
 * Need to extend from OpenSearchSingleNodeTestCase and override getPlugins and writeableRegistry
 * for testGetResponse. Without it, we will have exception "can't read named writeable from StreamInput"
 * when deserializing AnomalyDetector.
 *
 */
public class GetAnomalyDetectorActionTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, TimeSeriesAnalyticsPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testGetRequest() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        GetConfigRequest request = new GetConfigRequest(
            "1234",
            ADIndex.CONFIG.getIndexName(),
            4321,
            false,
            false,
            "nonempty",
            "",
            false,
            null
        );
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        GetConfigRequest newRequest = new GetConfigRequest(input);
        Assert.assertEquals(request.getConfigID(), newRequest.getConfigID());

    }

    public void testGetResponse() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        Feature feature = TestHelpers.randomFeature(true);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of(feature)); // Mockito.mock(AnomalyDetector.class);
        Job detectorJob = Mockito.mock(Job.class);
        // Mockito.doNothing().when(detector).writeTo(out);
        GetAnomalyDetectorResponse response = new GetAnomalyDetectorResponse(
            1234,
            "4567",
            9876,
            2345,
            detector,
            detectorJob,
            false,
            Mockito.mock(ADTask.class),
            Mockito.mock(ADTask.class),
            false,
            RestStatus.OK,
            Mockito.mock(DetectorProfile.class),
            null,
            false
        );
        response.writeTo(out);
        // StreamInput input = out.bytes().streamInput();
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        // PowerMockito.whenNew(AnomalyDetector.class).withAnyArguments().thenReturn(detector);
        GetAnomalyDetectorResponse newResponse = new GetAnomalyDetectorResponse(input);
        Assert.assertNotNull(newResponse);
    }
}
