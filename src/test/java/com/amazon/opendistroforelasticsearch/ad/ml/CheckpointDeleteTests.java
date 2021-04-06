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

package com.amazon.opendistroforelasticsearch.ad.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.ScrollableHitSource;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;
import com.google.gson.Gson;

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
    private RandomCutForestSerDe rcfSerde;
    private Clock clock;
    private AnomalyDetectionIndices indexUtil;
    private String detectorId;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(CheckpointDao.class);

        client = mock(Client.class);
        clientUtil = mock(ClientUtil.class);
        gson = null;
        rcfSerde = mock(RandomCutForestSerDe.class);
        clock = mock(Clock.class);
        indexUtil = mock(AnomalyDetectionIndices.class);
        detectorId = "123";

        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            CommonName.CHECKPOINT_INDEX_NAME,
            gson,
            rcfSerde,
            HybridThresholdingModel.class,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            indexUtil,
            AnomalyDetectorSettings.MAX_BULK_CHECKPOINT_SIZE,
            AnomalyDetectorSettings.CHECKPOINT_BULK_PER_SECOND
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
