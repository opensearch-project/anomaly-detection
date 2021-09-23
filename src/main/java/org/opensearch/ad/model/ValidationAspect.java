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

public enum ValidationAspect implements Name {
    DETECTOR(CommonName.DETECTOR),
    MODEL(CommonName.MODEL);

    private String name;

    ValidationAspect(String name) {
        this.name = name;
    }

    /**
     * Get validation aspect
     *
     * @return name
     */
    @Override
    public String getName() {
        return name;
    }

    public static ValidationAspect getName(String name) {
        switch (name) {
            case CommonName.DETECTOR:
                return DETECTOR;
            case CommonName.MODEL:
                return MODEL;
            default:
                throw new IllegalArgumentException("Unsupported validation aspects");
        }
    }

    public static Set<ValidationAspect> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, ValidationAspect::getName);
    }
}
