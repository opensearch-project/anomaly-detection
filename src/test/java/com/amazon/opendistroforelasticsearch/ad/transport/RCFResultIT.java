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
import java.util.concurrent.ExecutionException;

import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;

@OpenSearchIntegTestCase.ClusterScope(transportClientRatio = 0.9)
public class RCFResultIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    public void testEmptyFeature() throws ExecutionException, InterruptedException {
        RCFResultRequest request = new RCFResultRequest("123", "123-rcfmodel-1", new double[] {});

        ActionFuture<RCFResultResponse> future = client().execute(RCFResultAction.INSTANCE, request);

        expectThrows(ActionRequestValidationException.class, () -> future.actionGet());
    }

    public void testIDIsNull() throws ExecutionException, InterruptedException {
        RCFResultRequest request = new RCFResultRequest(null, "123-rcfmodel-1", new double[] { 0 });

        ActionFuture<RCFResultResponse> future = client().execute(RCFResultAction.INSTANCE, request);

        expectThrows(ActionRequestValidationException.class, () -> future.actionGet());
    }
}
