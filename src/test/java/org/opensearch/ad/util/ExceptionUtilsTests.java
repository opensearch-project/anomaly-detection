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

package org.opensearch.ad.util;

import org.opensearch.OpenSearchException;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

public class ExceptionUtilsTests extends OpenSearchTestCase {

    public void testGetShardsFailure() {
        ShardId shardId = new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), 1);
        ReplicationResponse.ShardInfo.Failure failure = new ReplicationResponse.ShardInfo.Failure(
            shardId,
            randomAlphaOfLength(5),
            new RuntimeException("test"),
            RestStatus.BAD_REQUEST,
            false
        );
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(2, 1, failure);
        IndexResponse indexResponse = new IndexResponse(shardId, "id", randomLong(), randomLong(), randomLong(), randomBoolean());
        indexResponse.setShardInfo(shardInfo);
        String shardsFailure = ExceptionUtil.getShardsFailure(indexResponse);
        assertEquals("RuntimeException[test]", shardsFailure);
    }

    public void testGetShardsFailureWithoutError() {
        ShardId shardId = new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), 1);
        IndexResponse indexResponse = new IndexResponse(shardId, "id", randomLong(), randomLong(), randomLong(), randomBoolean());
        assertNull(ExceptionUtil.getShardsFailure(indexResponse));

        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(2, 1, ReplicationResponse.EMPTY);
        indexResponse.setShardInfo(shardInfo);
        assertNull(ExceptionUtil.getShardsFailure(indexResponse));
    }

    public void testCountInStats() {
        assertTrue(ExceptionUtil.countInStats(new AnomalyDetectionException("test")));
        assertFalse(ExceptionUtil.countInStats(new AnomalyDetectionException("test").countedInStats(false)));
        assertTrue(ExceptionUtil.countInStats(new RuntimeException("test")));
    }

    public void testGetErrorMessage() {
        assertEquals("test", ExceptionUtil.getErrorMessage(new AnomalyDetectionException("test")));
        assertEquals("test", ExceptionUtil.getErrorMessage(new IllegalArgumentException("test")));
        assertEquals("OpenSearchException[test]", ExceptionUtil.getErrorMessage(new OpenSearchException("test")));
        assertTrue(
            ExceptionUtil
                .getErrorMessage(new RuntimeException("test"))
                .contains("at org.opensearch.ad.util.ExceptionUtilsTests.testGetErrorMessage")
        );
    }
}
