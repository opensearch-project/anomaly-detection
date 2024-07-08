/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.core.action.ActionListener;

public class ActionListenerExecutor {
    /*
     * Private constructor to avoid Jacoco complaining about public constructor
     * not covered: https://tinyurl.com/yetc7tra
     */
    private ActionListenerExecutor() {}

    /**
     * Wraps the provided response and failure handlers in an ActionListener that executes the
     * response handler asynchronously using the provided ExecutorService.
     *
     * @param <Response> the type of the response
     * @param onResponse a CheckedConsumer that handles the response; it can throw an exception
     * @param onFailure a Consumer that handles any exceptions thrown by the onResponse handler or the onFailure method
     * @param executorService the ExecutorService used to execute the onResponse handler asynchronously
     * @return an ActionListener that handles the response and failure cases
     */
    public static <Response> ActionListener<Response> wrap(
        CheckedConsumer<Response, ? extends Exception> onResponse,
        Consumer<Exception> onFailure,
        ExecutorService executorService
    ) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                executorService.execute(() -> {
                    try {
                        onResponse.accept(response);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(e);
            }
        };
    }
}
