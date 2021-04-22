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

package org.opensearch.action.admin.indices.mapping.get;

import static com.amazon.opendistroforelasticsearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.FEATURE_WITH_EMPTY_DATA_MSG;
import static com.amazon.opendistroforelasticsearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.FEATURE_WITH_INVALID_QUERY_MSG;
import static com.amazon.opendistroforelasticsearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.RestRequest;

import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ADValidationException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.ValidationAspect;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.ValidateAnomalyDetectorActionHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.ValidateAnomalyDetectorResponse;
import com.google.common.collect.ImmutableMap;

public class ValidateAnomalyDetectorActionHandlerTests extends AnomalyDetectorActionHandlerTestsBase<ValidateAnomalyDetectorResponse> {

    @Test
    public void testValidateEvenOverThousandSingleEntityDetectorLimit() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = maxSingleEntityAnomalyDetectors + 1;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse duplicateDetectorNameResponse = mock(SearchResponse.class);
        when(duplicateDetectorNameResponse.getHits()).thenReturn(TestHelpers.createSearchHits(0));

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 1;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));

        double[] features = new double[1];
        features[0] = 1.0;
        when(searchFeatureDao.parseResponse(any(SearchResponse.class), anyList())).thenReturn(Optional.of(features));

        NodeClient client = getMockClient(
            detectorResponse,
            duplicateDetectorNameResponse,
            userIndexResponse,
            false,
            false,
            null,
            null,
            null
        );

        handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            client,
            channel,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            RestRequest.Method.POST,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName()
        );

        handler.start();

        ArgumentCaptor<ValidateAnomalyDetectorResponse> response = ArgumentCaptor.forClass(ValidateAnomalyDetectorResponse.class);

        verify(channel).onResponse(response.capture());
        Object value = response.getValue();
        assertNull(value);
    }

    @Test
    public void testValidateEvenOverTenMultiEntityDetectorLimit() throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = maxMultiEntityAnomalyDetectors + 1;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse duplicateDetectorNameResponse = mock(SearchResponse.class);
        when(duplicateDetectorNameResponse.getHits()).thenReturn(TestHelpers.createSearchHits(0));

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 1;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));

        double[] features = new double[1];
        features[0] = 1.0;
        when(searchFeatureDao.parseResponse(any(SearchResponse.class), anyList())).thenReturn(Optional.of(features));

        NodeClient client = getMockClient(
            detectorResponse,
            duplicateDetectorNameResponse,
            userIndexResponse,
            false,
            false,
            null,
            field,
            CommonName.IP_TYPE
        );

        handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            client,
            channel,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            RestRequest.Method.POST,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName()
        );

        handler.start();

        ArgumentCaptor<ValidateAnomalyDetectorResponse> response = ArgumentCaptor.forClass(ValidateAnomalyDetectorResponse.class);

        verify(channel).onResponse(response.capture());
        Object value = response.getValue();
        assertNull(value);
    }

    @Test
    public void testInvalidFeatureQueryCausingSearchFailure() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 1;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 1;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));

        NodeClient client = getMockClient(detectorResponse, null, userIndexResponse, true, true, null, null, null);

        handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            client,
            channel,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            RestRequest.Method.POST,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName()
        );

        handler.start();

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);

        verify(channel).onFailure(response.capture());
        Exception exceptionCaught = response.getValue();
        assertNotNull(exceptionCaught);
        assertTrue(exceptionCaught instanceof ADValidationException);
        assertTrue(exceptionCaught.getMessage().contains(FEATURE_WITH_INVALID_QUERY_MSG));
    }

    @Test
    public void testFeatureQueryCausingUnknownFailure() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 1;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 1;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));

        NodeClient client = getMockClient(detectorResponse, null, userIndexResponse, true, false, null, null, null);

        handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            client,
            channel,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            RestRequest.Method.POST,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName()
        );

        handler.start();

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);

        verify(channel).onFailure(response.capture());
        Exception exceptionCaught = response.getValue();
        assertNotNull(exceptionCaught);
        assertTrue(exceptionCaught instanceof ADValidationException);
        assertTrue(exceptionCaught.getMessage().contains(UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG));
    }

    @Test
    public void testInvalidFeatureWithEmptyData() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now());

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 1;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 1;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));

        when(searchFeatureDao.parseResponse(any(SearchResponse.class), anyList())).thenReturn(Optional.empty());

        NodeClient client = getMockClient(detectorResponse, null, userIndexResponse, false, false, null, null, null);

        handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            client,
            channel,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            RestRequest.Method.POST,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName()
        );

        handler.start();

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);

        verify(channel).onFailure(response.capture());
        Exception exceptionCaught = response.getValue();
        assertNotNull(exceptionCaught);
        assertTrue(exceptionCaught instanceof ADValidationException);
        assertTrue(exceptionCaught.getMessage().contains(FEATURE_WITH_EMPTY_DATA_MSG));
    }
}
