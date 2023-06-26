/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.indices;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionListener;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.function.ExecutorFunction;

public abstract class IndexManagementIntegTestCase<IndexType extends Enum<IndexType> & TimeSeriesIndex, ISMType extends IndexManagement<IndexType>>
    extends OpenSearchIntegTestCase {

    public void validateCustomIndexForBackendJob(ISMType indices, String resultMapping) throws IOException, InterruptedException {

        Map<String, Object> asMap = XContentHelper.convertToMap(new BytesArray(resultMapping), false, XContentType.JSON).v2();
        String resultIndex = "test_index";

        client()
            .admin()
            .indices()
            .prepareCreate(resultIndex)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .setMapping(asMap)
            .get();
        ensureGreen(resultIndex);

        String securityLogId = "logId";
        String user = "testUser";
        List<String> roles = Arrays.asList("role1", "role2");
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(function).execute();
        latch.await(20, TimeUnit.SECONDS);
        indices.validateCustomIndexForBackendJob(resultIndex, securityLogId, user, roles, function, listener);
        verify(listener, never()).onFailure(any(Exception.class));
    }

    public void validateCustomIndexForBackendJobInvalidMapping(ISMType indices) {
        String resultIndex = "test_index";

        client()
            .admin()
            .indices()
            .prepareCreate(resultIndex)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .setMapping("ip", "type=ip")
            .get();
        ensureGreen(resultIndex);

        String securityLogId = "logId";
        String user = "testUser";
        List<String> roles = Arrays.asList("role1", "role2");
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        indices.validateCustomIndexForBackendJob(resultIndex, securityLogId, user, roles, function, listener);

        ArgumentCaptor<EndRunException> exceptionCaptor = ArgumentCaptor.forClass(EndRunException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertEquals("Result index mapping is not correct", exceptionCaptor.getValue().getMessage());
    }

    public void validateCustomIndexForBackendJobNoIndex(ISMType indices) {
        String resultIndex = "testIndex";
        String securityLogId = "logId";
        String user = "testUser";
        List<String> roles = Arrays.asList("role1", "role2");
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<Void> listener = mock(ActionListener.class);

        indices.validateCustomIndexForBackendJob(resultIndex, securityLogId, user, roles, function, listener);

        ArgumentCaptor<EndRunException> exceptionCaptor = ArgumentCaptor.forClass(EndRunException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertEquals(CommonMessages.CAN_NOT_FIND_RESULT_INDEX + resultIndex, exceptionCaptor.getValue().getMessage());
    }
}
