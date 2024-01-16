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
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.model.Feature;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * Need to extend from OpenSearchSingleNodeTestCase and override getPlugins and writeableRegistry
 * for testGetResponse. Without it, we will have exception "can't read named writeable from StreamInput"
 * when deserializing AnomalyDetector.
 *
 */
public class GetAnomalyDetectorActionTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testGetRequest() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        GetAnomalyDetectorRequest request = new GetAnomalyDetectorRequest("1234", 4321, false, false, "nonempty", "", false, null);
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        GetAnomalyDetectorRequest newRequest = new GetAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());

    }

    @Test
    public void testGetResponse() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        Feature feature = TestHelpers.randomFeature(true);
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(List.of(feature));
        AnomalyDetectorJob detectorJob = Mockito.mock(AnomalyDetectorJob.class);
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

        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        GetAnomalyDetectorResponse newResponse = new GetAnomalyDetectorResponse(input);
        Assert.assertNotNull(newResponse);
    }
}
