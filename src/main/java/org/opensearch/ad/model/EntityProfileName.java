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

package org.opensearch.ad.model;

import java.util.Collection;
import java.util.Set;

import org.opensearch.ad.Name;
import org.opensearch.ad.constant.CommonName;

public enum EntityProfileName implements Name {
    INIT_PROGRESS(CommonName.INIT_PROGRESS),
    ENTITY_INFO(CommonName.ENTITY_INFO),
    STATE(CommonName.STATE),
    MODELS(CommonName.MODELS);

    private String name;

    EntityProfileName(String name) {
        this.name = name;
    }

    /**
     * Get profile name
     *
     * @return name
     */
    @Override
    public String getName() {
        return name;
    }

    public static EntityProfileName getName(String name) {
        switch (name) {
            case CommonName.INIT_PROGRESS:
                return INIT_PROGRESS;
            case CommonName.ENTITY_INFO:
                return ENTITY_INFO;
            case CommonName.STATE:
                return STATE;
            case CommonName.MODELS:
                return MODELS;
            default:
                throw new IllegalArgumentException("Unsupported profile types");
        }
    }

    public static Set<EntityProfileName> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, EntityProfileName::getName);
    }
}
