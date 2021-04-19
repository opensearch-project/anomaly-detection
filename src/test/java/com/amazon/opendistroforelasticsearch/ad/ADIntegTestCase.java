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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad;

import static com.amazon.opendistroforelasticsearch.ad.AbstractADTest.LOG;
import static com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.util.RestHandlerUtils;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public abstract class ADIntegTestCase extends OpenSearchIntegTestCase {

    private long timeout = 5_000;
    protected String timeField = "timestamp";
    protected String categoryField = "type";
    protected String valueField = "value";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AnomalyDetectorPlugin.class);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    public void createDetectors(List<AnomalyDetector> detectors, boolean createIndexFirst) throws IOException {
        if (createIndexFirst) {
            createIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX, AnomalyDetectionIndices.getAnomalyDetectorMappings());
        }

        for (AnomalyDetector detector : detectors) {
            indexDoc(AnomalyDetector.ANOMALY_DETECTORS_INDEX, detector.toXContent(jsonBuilder(), XCONTENT_WITH_TYPE));
        }
    }

    public String createDetector(AnomalyDetector detector) throws IOException {
        return indexDoc(AnomalyDetector.ANOMALY_DETECTORS_INDEX, detector.toXContent(jsonBuilder(), XCONTENT_WITH_TYPE));
    }

    public String createADResult(AnomalyResult adResult) throws IOException {
        return indexDoc(CommonName.ANOMALY_RESULT_INDEX_ALIAS, adResult.toXContent(jsonBuilder(), XCONTENT_WITH_TYPE));
    }

    public String createADTask(ADTask adTask) throws IOException {
        return indexDoc(CommonName.DETECTION_STATE_INDEX, adTask.toXContent(jsonBuilder(), XCONTENT_WITH_TYPE));
    }

    public void createDetectorIndex() throws IOException {
        createIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX, AnomalyDetectionIndices.getAnomalyDetectorMappings());
    }

    public void createADResultIndex() throws IOException {
        createIndex(CommonName.ANOMALY_RESULT_INDEX_ALIAS, AnomalyDetectionIndices.getAnomalyResultMappings());
    }

    public void createDetectionStateIndex() throws IOException {
        createIndex(CommonName.DETECTION_STATE_INDEX, AnomalyDetectionIndices.getDetectionStateMappings());
    }

    public void createTestDataIndex(String indexName) {
        String mappings = "{\"properties\":{\""
            + timeField
            + "\":{\"type\":\"date\",\"format\":\"strict_date_time||epoch_millis\"},"
            + "\"value\":{\"type\":\"double\"}, \""
            + categoryField
            + "\":{\"type\":\"keyword\"},"
            + "\"is_error\":{\"type\":\"boolean\"}, \"message\":{\"type\":\"text\"}}}";
        createIndex(indexName, mappings);
    }

    public void createIndex(String indexName, String mappings) {
        CreateIndexResponse createIndexResponse = TestHelpers.createIndex(admin(), indexName, mappings);
        assertEquals(true, createIndexResponse.isAcknowledged());
    }

    public AcknowledgedResponse deleteDetectorIndex() {
        return deleteIndex(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
    }

    public AcknowledgedResponse deleteIndex(String indexName) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        return admin().indices().delete(deleteIndexRequest).actionGet(timeout);
    }

    public void deleteIndexIfExists(String indexName) {
        if (indexExists(indexName)) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            admin().indices().delete(deleteIndexRequest).actionGet(timeout);
        }
    }

    public String indexDoc(String indexName, XContentBuilder source) {
        IndexRequest indexRequest = new IndexRequest(indexName).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source(source);
        IndexResponse indexResponse = client().index(indexRequest).actionGet(timeout);
        assertEquals(RestStatus.CREATED, indexResponse.status());
        return indexResponse.getId();
    }

    public String indexDoc(String indexName, Map<String, ?> source) {
        IndexRequest indexRequest = new IndexRequest(indexName).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).source(source);
        IndexResponse indexResponse = client().index(indexRequest).actionGet(timeout);
        assertEquals(RestStatus.CREATED, indexResponse.status());
        return indexResponse.getId();
    }

    public <T extends ToXContent> BulkResponse bulkIndexObjects(String indexName, List<T> objects) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        objects.forEach(obj -> {
            try (XContentBuilder builder = jsonBuilder()) {
                IndexRequest indexRequest = new IndexRequest(indexName)
                    .source(obj.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
                bulkRequestBuilder.add(indexRequest);
            } catch (Exception e) {
                String error = "Failed to prepare request to bulk index docs";
                LOG.error(error, e);
                throw new AnomalyDetectionException(error);
            }
        });
        return client().bulk(bulkRequestBuilder.request()).actionGet(timeout);
    }

    public BulkResponse bulkIndexDocs(String indexName, List<Map<String, ?>> docs, long timeout) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        docs.forEach(doc -> bulkRequestBuilder.add(new IndexRequest(indexName).source(doc)));
        return client().bulk(bulkRequestBuilder.request().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).actionGet(timeout);
    }

    public GetResponse getDoc(String indexName, String id) {
        GetRequest getRequest = new GetRequest(indexName).id(id);
        return client().get(getRequest).actionGet(timeout);
    }

    public long countDocs(String indexName) {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder()).size(0);
        request.indices(indexName).source(searchSourceBuilder);
        SearchResponse searchResponse = client().search(request).actionGet(timeout);
        return searchResponse.getHits().getTotalHits().value;
    }

    public ClusterUpdateSettingsResponse updateTransientSettings(Map<String, ?> settings) {
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(settings);
        return clusterAdmin().updateSettings(updateSettingsRequest).actionGet(timeout);
    }

    public ImmutableOpenMap<String, DiscoveryNode> getDataNodes() {
        DiscoveryNodes nodes = clusterService().state().getNodes();
        return nodes.getDataNodes();
    }

    public Client getDataNodeClient() {
        for (Client client : clients()) {
            if (client instanceof NodeClient) {
                return client;
            }
        }
        return null;
    }

    public DiscoveryNode[] getDataNodesArray() {
        DiscoveryNodes nodes = clusterService().state().getNodes();
        Iterator<ObjectObjectCursor<String, DiscoveryNode>> iterator = nodes.getDataNodes().iterator();
        List<DiscoveryNode> dataNodes = new ArrayList<>();
        while (iterator.hasNext()) {
            dataNodes.add(iterator.next().value);
        }
        return dataNodes.toArray(new DiscoveryNode[0]);
    }

}
