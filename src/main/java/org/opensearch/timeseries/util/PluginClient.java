/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.timeseries.util;

import org.opensearch.transport.client.Client;

/**
 * The time series analytics plugin's client, which runs transport actions as the plugin's system subject.
 */
public final class PluginClient extends org.opensearch.commons.client.PluginClient {

    /**
     * Wraps a client so its actions run as the plugin subject, which is assigned later.
     *
     * @param delegate the client whose actions this wraps
     */
    public PluginClient(Client delegate) {
        super(delegate);
    }
}
