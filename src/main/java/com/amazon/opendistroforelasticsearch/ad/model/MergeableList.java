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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.util.List;

public class MergeableList<T> implements Mergeable {

    private final List<T> elements;

    public List<T> getElements() {
        return elements;
    }

    public MergeableList(List<T> elements) {
        this.elements = elements;
    }

    @Override
    public void merge(Mergeable other) {
        if (this == other || other == null || getClass() != other.getClass()) {
            return;
        }
        MergeableList otherList = (MergeableList) other;
        if (otherList.getElements() != null) {
            this.elements.addAll(otherList.getElements());
        }
    }
}
