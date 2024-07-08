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

import java.util.List;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkItemResponse.Failure;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.util.BulkUtil;

public class BulkUtilTests extends OpenSearchTestCase {
    public void testGetFailedIndexRequest() {
        BulkItemResponse[] itemResponses = new BulkItemResponse[2];
        String indexName = "index";
        String type = "_doc";
        String idPrefix = "id";
        String uuid = "uuid";
        int shardIntId = 0;
        ShardId shardId = new ShardId(new Index(indexName, uuid), shardIntId);
        itemResponses[0] = new BulkItemResponse(
            0,
            randomFrom(DocWriteRequest.OpType.values()),
            new Failure(indexName, idPrefix + 0, new VersionConflictEngineException(shardId, "", "blah"))
        );
        itemResponses[1] = new BulkItemResponse(
            1,
            randomFrom(DocWriteRequest.OpType.values()),
            new IndexResponse(shardId, idPrefix + 1, 1, 1, randomInt(), true)
        );
        BulkResponse response = new BulkResponse(itemResponses, 0);

        BulkRequest request = new BulkRequest();
        for (int i = 0; i < 2; i++) {
            request.add(new IndexRequest(indexName).id(idPrefix + i).source(XContentType.JSON, "field", "value"));
        }

        List<IndexRequest> retry = BulkUtil.getFailedIndexRequest(request, response);
        assertEquals(1, retry.size());
        assertEquals(idPrefix + 0, retry.get(0).id());
    }
}
