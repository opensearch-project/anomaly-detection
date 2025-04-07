/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

import java.io.IOException;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.security.spi.resources.ShareableResourceParser;

/**
 * ConfigParser is used to parse Config object from XContentParser.
 *
 */
public class ConfigParser implements ShareableResourceParser<Config> {

    private final Class<? extends Config> clazz;

    public ConfigParser(Class<? extends Config> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Config parseXContent(XContentParser xContentParser) throws IOException {
        return Config.parseConfig(clazz, xContentParser);
    }
}
