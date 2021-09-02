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

package org.opensearch.ad.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.ScrollableHitSource;

import com.amazon.randomcutforest.parkservices.threshold.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.threshold.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV2StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.amazon.randomcutforest.state.RandomCutForestState;
import com.google.gson.Gson;

import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;

/**
 * CheckpointDaoTests cannot extends basic ES test case and I cannot check logs
 * written during test running using functions in ADAbstractTest.  Create a new
 * class for tests requiring checking logs.
 *
 */
public class CheckpointDeleteTests extends AbstractADTest {
    private enum DeleteExecutionMode {
        NORMAL,
        INDEX_NOT_FOUND,
        FAILURE,
        PARTIAL_FAILURE
    }

    private CheckpointDao checkpointDao;
    private Client client;
    private ClientUtil clientUtil;
    private Gson gson;
    private AnomalyDetectionIndices indexUtil;
    private String detectorId;
    private int maxCheckpointBytes;
    private GenericObjectPool<LinkedBuffer> objectPool;

    @Mock
    private ThresholdedRandomCutForestMapper ercfMapper;

    @Mock
    private Schema<ThresholdedRandomCutForestState> ercfSchema;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(CheckpointDao.class);

        client = mock(Client.class);
        clientUtil = mock(ClientUtil.class);
        gson = null;
        indexUtil = mock(AnomalyDetectionIndices.class);
        detectorId = "123";
        maxCheckpointBytes = 1_000_000;

        RandomCutForestMapper mapper = mock(RandomCutForestMapper.class);
        Schema<RandomCutForestState> schema = mock(Schema.class);
        V1JsonToV2StateConverter converter = mock(V1JsonToV2StateConverter.class);

        objectPool = mock(GenericObjectPool.class);
        int deserializeRCFBufferSize = 512;
        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            CommonName.CHECKPOINT_INDEX_NAME,
            gson,
            mapper,
            schema,
            converter,
            ercfMapper,
            ercfSchema,
            HybridThresholdingModel.class,
            indexUtil,
            maxCheckpointBytes,
            objectPool,
            deserializeRCFBufferSize
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    @SuppressWarnings("unchecked")
    public void delete_by_detector_id_template(DeleteExecutionMode mode) {
        long deletedDocNum = 10L;
        BulkByScrollResponse deleteByQueryResponse = mock(BulkByScrollResponse.class);
        when(deleteByQueryResponse.getDeleted()).thenReturn(deletedDocNum);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 3);
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<BulkByScrollResponse> listener = (ActionListener<BulkByScrollResponse>) args[2];

            assertTrue(listener != null);
            if (mode == DeleteExecutionMode.INDEX_NOT_FOUND) {
                listener.onFailure(new IndexNotFoundException(CommonName.CHECKPOINT_INDEX_NAME));
            } else if (mode == DeleteExecutionMode.FAILURE) {
                listener.onFailure(new OpenSearchException(""));
            } else {
                if (mode == DeleteExecutionMode.PARTIAL_FAILURE) {
                    when(deleteByQueryResponse.getSearchFailures())
                        .thenReturn(
                            Collections
                                .singletonList(new ScrollableHitSource.SearchFailure(new OpenSearchException("foo"), "bar", 1, "blah"))
                        );
                }
                listener.onResponse(deleteByQueryResponse);
            }

            return null;
        }).when(client).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());

        checkpointDao.deleteModelCheckpointByDetectorId(detectorId);
    }

    public void testDeleteSingleNormal() throws Exception {
        delete_by_detector_id_template(DeleteExecutionMode.NORMAL);
        assertTrue(testAppender.containsMessage(CheckpointDao.DOC_GOT_DELETED_LOG_MSG));
    }

    public void testDeleteSingleIndexNotFound() throws Exception {
        delete_by_detector_id_template(DeleteExecutionMode.INDEX_NOT_FOUND);
        assertTrue(testAppender.containsMessage(CheckpointDao.INDEX_DELETED_LOG_MSG));
    }

    public void testDeleteSingleResultFailure() throws Exception {
        delete_by_detector_id_template(DeleteExecutionMode.FAILURE);
        assertTrue(testAppender.containsMessage(CheckpointDao.NOT_ABLE_TO_DELETE_LOG_MSG));
    }

    public void testDeleteSingleResultPartialFailure() throws Exception {
        delete_by_detector_id_template(DeleteExecutionMode.PARTIAL_FAILURE);
        assertTrue(testAppender.containsMessage(CheckpointDao.SEARCH_FAILURE_LOG_MSG));
        assertTrue(testAppender.containsMessage(CheckpointDao.DOC_GOT_DELETED_LOG_MSG));
    }
}
