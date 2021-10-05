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

package org.opensearch.ad.model;

import org.opensearch.ad.AbstractADTest;

import java.util.ArrayList;
import java.util.List;

public class MergeableListTests extends AbstractADTest {

    public void testMergeableListGetElements() {
        List<String> ls1 = new ArrayList<String>();
        ls1.add("item1");
        ls1.add("item2");
        MergeableList<String> mergeList = new MergeableList<>(ls1);
        assertEquals(ls1, mergeList.getElements());
    }

    public void testMergeableListMerge() {
        List<String> ls1 = new ArrayList<String>();
        ls1.add("item1");
        ls1.add("item2");
        List<String> ls2 = new ArrayList<String>();
        ls1.add("item3");
        ls1.add("item4");
        MergeableList<String> mergeListOne = new MergeableList<>(ls1);
        MergeableList<String> mergeListTwo = new MergeableList<>(ls2);
        mergeListOne.merge(mergeListTwo);
        assertEquals(4, mergeListOne.getElements().size());
        assertEquals("item3", mergeListOne.getElements().get(2));
    }
}
