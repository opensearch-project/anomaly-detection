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

package org.opensearch.ad;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * A super type for enum types returning names
 *
 */
public interface Name {
    String getName();

    static <T extends Name> Set<T> getNameFromCollection(Collection<String> names, Function<String, T> getName) {
        Set<T> res = new HashSet<>();
        for (String name : names) {
            res.add(getName.apply(name));
        }
        return res;
    }
}
