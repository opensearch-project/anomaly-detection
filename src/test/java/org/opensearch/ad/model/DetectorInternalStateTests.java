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
import java.util.Collection;

import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.TestHelpers;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class DetectorInternalStateTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testToXContentDetectorInternalState() throws IOException {
        DetectorInternalState internalState = new DetectorInternalState.Builder()
            .lastUpdateTime(Instant.ofEpochMilli(100L))
            .error("error-test")
            .build();
        String internalStateString = TestHelpers
            .xContentBuilderToString(internalState.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        DetectorInternalState parsedInternalState = DetectorInternalState.parse(TestHelpers.parser(internalStateString));
        assertEquals(internalState, parsedInternalState);
    }
}
