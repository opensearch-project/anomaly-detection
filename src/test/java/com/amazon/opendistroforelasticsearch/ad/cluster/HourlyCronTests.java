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

package com.amazon.opendistroforelasticsearch.ad.cluster;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;

import test.com.amazon.opendistroforelasticsearch.ad.util.ClusterCreation;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.transport.CronAction;
import com.amazon.opendistroforelasticsearch.ad.transport.CronNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.CronResponse;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class HourlyCronTests extends AbstractADTest {

    enum HourlyCronTestExecutionMode {
        NORMAL,
        NODE_FAIL,
        ALL_FAIL
    }

    @SuppressWarnings("unchecked")
    public void templateHourlyCron(HourlyCronTestExecutionMode mode) {
        super.setUpLog4jForJUnit(HourlyCron.class);

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = ClusterCreation.state(1);
        when(clusterService.state()).thenReturn(state);
        HashMap<String, String> ignoredAttributes = new HashMap<String, String>();
        ignoredAttributes.put(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE);
        DiscoveryNodeFilterer nodeFilter = new DiscoveryNodeFilterer(clusterService);

        Client client = mock(Client.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 3);
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<CronResponse> listener = (ActionListener<CronResponse>) args[2];

            if (mode == HourlyCronTestExecutionMode.NODE_FAIL) {
                listener
                    .onResponse(
                        new CronResponse(
                            new ClusterName("test"),
                            Collections.singletonList(new CronNodeResponse(state.nodes().getLocalNode())),
                            Collections.singletonList(new FailedNodeException("foo0", "blah", new OpenSearchException("bar")))
                        )
                    );
            } else if (mode == HourlyCronTestExecutionMode.ALL_FAIL) {
                listener.onFailure(new OpenSearchException("bar"));
            } else {
                CronNodeResponse nodeResponse = new CronNodeResponse(state.nodes().getLocalNode());
                BytesStreamOutput nodeResponseOut = new BytesStreamOutput();
                nodeResponseOut.setVersion(Version.CURRENT);
                nodeResponse.writeTo(nodeResponseOut);
                StreamInput siNode = nodeResponseOut.bytes().streamInput();

                CronNodeResponse nodeResponseRead = new CronNodeResponse(siNode);

                CronResponse response = new CronResponse(
                    new ClusterName("test"),
                    Collections.singletonList(nodeResponseRead),
                    Collections.EMPTY_LIST
                );
                BytesStreamOutput out = new BytesStreamOutput();
                out.setVersion(Version.CURRENT);
                response.writeTo(out);
                StreamInput si = out.bytes().streamInput();
                CronResponse responseRead = new CronResponse(si);
                listener.onResponse(responseRead);
            }

            return null;
        }).when(client).execute(eq(CronAction.INSTANCE), any(), any());

        HourlyCron cron = new HourlyCron(client, nodeFilter);
        cron.run();

        Logger LOG = LogManager.getLogger(HourlyCron.class);
        LOG.info(testAppender.messages);
        if (mode == HourlyCronTestExecutionMode.NODE_FAIL) {
            assertTrue(testAppender.containsMessage(HourlyCron.NODE_EXCEPTION_LOG_MSG));
        } else if (mode == HourlyCronTestExecutionMode.ALL_FAIL) {
            assertTrue(testAppender.containsMessage(HourlyCron.EXCEPTION_LOG_MSG));
        } else {
            assertTrue(testAppender.containsMessage(HourlyCron.SUCCEEDS_LOG_MSG));
        }

        super.tearDownLog4jForJUnit();
    }

    public void testNormal() {
        templateHourlyCron(HourlyCronTestExecutionMode.NORMAL);
    }

    public void testAllFail() {
        templateHourlyCron(HourlyCronTestExecutionMode.ALL_FAIL);
    }

    public void testNodeFail() throws Exception {
        templateHourlyCron(HourlyCronTestExecutionMode.NODE_FAIL);
    }
}
