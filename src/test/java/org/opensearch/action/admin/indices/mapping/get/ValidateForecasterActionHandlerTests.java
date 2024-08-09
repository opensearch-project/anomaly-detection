/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.action.admin.indices.mapping.get;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.forecast.rest.handler.ValidateForecasterActionHandler;
import org.opensearch.timeseries.model.ValidationAspect;

public class ValidateForecasterActionHandlerTests extends AbstractForecasterActionHandlerTestCase {
    protected ValidateForecasterActionHandler handler;

    public void testCreateOrUpdateConfigException() throws InterruptedException {
        doThrow(IllegalArgumentException.class).when(forecastISM).doesConfigIndexExist();
        handler = new ValidateForecasterActionHandler(
            clusterService,
            clientMock,
            clientUtil,
            forecastISM,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.FORECASTER.getName(),
            clock,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof IllegalArgumentException);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testValidateTimeFieldException() throws InterruptedException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    listener.onFailure(new IllegalArgumentException());
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        handler = new ValidateForecasterActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            forecastISM,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.FORECASTER.getName(),
            clock,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof IllegalArgumentException);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
    }
}
