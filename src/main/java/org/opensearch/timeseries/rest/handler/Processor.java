/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;

/**
 * Represents a processor capable of initiating a certain process
 * and then notifying a listener upon completion.
 *
 * @param <T> the type of response expected after processing, which must be a subtype of ActionResponse.
 */
public interface Processor<T extends ActionResponse> {

    /**
     * Starts the processing action. Once the processing is completed,
     * the provided listener is notified with the outcome.
     *
     * @param listener the listener to be notified upon the completion of the processing action.
     */
    public void start(ActionListener<T> listener);
}
