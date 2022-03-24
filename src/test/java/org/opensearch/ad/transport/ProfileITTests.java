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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

public class ProfileITTests extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    public void testNormalProfile() throws ExecutionException, InterruptedException {
        ProfileRequest profileRequest = new ProfileRequest("123", new HashSet<DetectorProfileName>(), false);

        ProfileResponse response = client().execute(ProfileAction.INSTANCE, profileRequest).get();
        assertTrue("getting profile failed", !response.hasFailures());
    }
}
