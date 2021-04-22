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

package org.opensearch.action.admin.indices.mapping.get;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.rest.RestRequest;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ADValidationException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.IndexAnomalyDetectorResponse;

/**
 *
 * we need to put the test in the same package of GetFieldMappingsResponse
 * (org.opensearch.action.admin.indices.mapping.get) since its constructor is
 * package private
 *
 */
public class IndexAnomalyDetectorActionHandlerTests extends AnomalyDetectorActionHandlerTestsBase<IndexAnomalyDetectorResponse> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock,
            transportService,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );
    }

    public void testTwoCategoricalFields() throws IOException {
        expectThrows(
            ADValidationException.class,
            () -> TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a", "b"))
        );
    }

    @SuppressWarnings("unchecked")
    public void testNoCategoricalField() throws IOException {
        SearchResponse mockResponse = mock(SearchResponse.class);
        int totalHits = 1001;
        when(mockResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);

            assertTrue(args[0] instanceof SearchRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            listener.onResponse(mockResponse);

            return null;
        }).when(clientMock).search(any(SearchRequest.class), any());

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock,
            transportService,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            // no categorical feature
            TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true),
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        handler.start();
        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        verify(clientMock, never()).execute(eq(GetMappingsAction.INSTANCE), any(), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof IllegalArgumentException);
        assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG));
    }

    @SuppressWarnings("unchecked")
    public void testTextField() throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse duplicateDetectorNameResponse = mock(SearchResponse.class);
        when(duplicateDetectorNameResponse.getHits()).thenReturn(TestHelpers.createSearchHits(0));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = getMockClient(
            detectorResponse,
            duplicateDetectorNameResponse,
            null,
            false,
            false,
            null,
            field,
            TEXT_FIELD_TYPE
        );

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            client,
            transportService,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);

        handler.start();

        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof Exception);
        assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.CATEGORICAL_FIELD_TYPE_ERR_MSG));
    }

    @SuppressWarnings("unchecked")
    private void testValidTypeTemplate(String filedTypeName) throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse duplicateDetectorNameResponse = mock(SearchResponse.class);
        when(duplicateDetectorNameResponse.getHits()).thenReturn(TestHelpers.createSearchHits(0));

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 0;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = getMockClient(
            detectorResponse,
            duplicateDetectorNameResponse,
            userIndexResponse,
            false,
            false,
            null,
            field,
            filedTypeName
        );

        NodeClient clientSpy = spy(client);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            transportService,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);

        handler.start();

        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof ADValidationException);
        assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG));
    }

    public void testIpField() throws IOException {
        testValidTypeTemplate(CommonName.IP_TYPE);
    }

    public void testKeywordField() throws IOException {
        testValidTypeTemplate(CommonName.KEYWORD_TYPE);
    }

    @SuppressWarnings("unchecked")
    private void testUpdateTemplate(String fieldTypeName) throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse duplicateDetectorNameResponse = mock(SearchResponse.class);
        when(duplicateDetectorNameResponse.getHits()).thenReturn(TestHelpers.createSearchHits(0));

        GetResponse getDetectorResponse = TestHelpers
            .createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 0;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = getMockClient(
            detectorResponse,
            duplicateDetectorNameResponse,
            userIndexResponse,
            false,
            false,
            getDetectorResponse,
            field,
            fieldTypeName
        );

        NodeClient clientSpy = spy(client);
        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            transportService,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);

        handler.start();

        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        if (fieldTypeName.equals(CommonName.IP_TYPE) || fieldTypeName.equals(CommonName.KEYWORD_TYPE)) {
            assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG));
        } else {
            assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.CATEGORICAL_FIELD_TYPE_ERR_MSG));
        }

    }

    public void testUpdateIpField() throws IOException {
        testUpdateTemplate(CommonName.IP_TYPE);
    }

    public void testUpdateKeywordField() throws IOException {
        testUpdateTemplate(CommonName.KEYWORD_TYPE);
    }

    public void testUpdateTextField() throws IOException {
        testUpdateTemplate(TEXT_FIELD_TYPE);
    }

    @SuppressWarnings("unchecked")
    public void testMoreThanTenMultiEntityDetectors() throws IOException {
        SearchResponse mockResponse = mock(SearchResponse.class);

        int totalHits = 11;

        when(mockResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);

            assertTrue(args[0] instanceof SearchRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            listener.onResponse(mockResponse);

            return null;
        }).when(clientMock).search(any(SearchRequest.class), any());

        handler.start();

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        verify(clientMock, times(1)).search(any(SearchRequest.class), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof IllegalArgumentException);
        assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG));
    }

    @SuppressWarnings("unchecked")
    public void testTenMultiEntityDetectorsUpdateSingleEntityAdToMulti() throws IOException {
        int totalHits = 10;
        AnomalyDetector existingDetector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, null);
        GetResponse getDetectorResponse = TestHelpers
            .createGetResponse(existingDetector, existingDetector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);

            assertTrue(args[0] instanceof SearchRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            listener.onResponse(searchResponse);

            return null;
        }).when(clientMock).search(any(SearchRequest.class), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);

            assertTrue(args[0] instanceof GetRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            listener.onResponse(getDetectorResponse);

            return null;
        }).when(clientMock).get(any(GetRequest.class), any());

        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock,
            transportService,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        handler.start();

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        verify(clientMock, times(1)).search(any(SearchRequest.class), any());
        verify(clientMock, times(1)).get(any(GetRequest.class), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof IllegalArgumentException);
        assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG));
    }

    @SuppressWarnings("unchecked")
    public void testTenMultiEntityDetectorsUpdateExistingMultiEntityAd() throws IOException {
        int totalHits = 10;
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a"));
        GetResponse getDetectorResponse = TestHelpers
            .createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);

            assertTrue(args[0] instanceof SearchRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            listener.onResponse(searchResponse);

            return null;
        }).when(clientMock).search(any(SearchRequest.class), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);

            assertTrue(args[0] instanceof GetRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            listener.onResponse(getDetectorResponse);

            return null;
        }).when(clientMock).get(any(GetRequest.class), any());

        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock,
            transportService,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        handler.start();

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        verify(clientMock, times(0)).search(any(SearchRequest.class), any());
        verify(clientMock, times(1)).get(any(GetRequest.class), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        // make sure execution passes all necessary checks
        assertTrue(value instanceof IllegalStateException);
        assertTrue(value.getMessage().contains("NodeClient has not been initialized"));
    }
}
