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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfileName;

@OpenSearchIntegTestCase.ClusterScope(transportClientRatio = 0.9)
public class ProfileIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    public void testNormalProfile() throws ExecutionException, InterruptedException {
        ProfileRequest profileRequest = new ProfileRequest("123", new HashSet<DetectorProfileName>(), false);

        ProfileResponse response = client().execute(ProfileAction.INSTANCE, profileRequest).get();
        assertTrue("getting profile failed", !response.hasFailures());
    }
}
