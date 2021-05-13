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

package org.opensearch.ad.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.model.Mergeable;

/**
 * A listener wrapper to help send multiple requests asynchronously and return one final responses together
 */
public class MultiResponsesDelegateActionListener<T extends Mergeable> implements ActionListener<T> {
    private static final Logger LOG = LogManager.getLogger(MultiResponsesDelegateActionListener.class);
    static final String NO_RESPONSE = "No response collected";

    private final ActionListener<T> delegate;
    private final AtomicInteger collectedResponseCount;
    private final AtomicInteger maxResponseCount;
    // save responses from multiple requests
    private final List<T> savedResponses;
    private List<String> exceptions;
    private String finalErrorMsg;
    private final boolean returnOnPartialResults;

    public MultiResponsesDelegateActionListener(
        ActionListener<T> delegate,
        int maxResponseCount,
        String finalErrorMsg,
        boolean returnOnPartialResults
    ) {
        this.delegate = delegate;
        this.collectedResponseCount = new AtomicInteger(0);
        this.maxResponseCount = new AtomicInteger(maxResponseCount);
        this.savedResponses = Collections.synchronizedList(new ArrayList<T>());
        this.exceptions = Collections.synchronizedList(new ArrayList<String>());
        this.finalErrorMsg = finalErrorMsg;
        this.returnOnPartialResults = returnOnPartialResults;
    }

    @Override
    public void onResponse(T response) {
        try {
            if (response != null) {
                this.savedResponses.add(response);
            }
        } finally {
            // If expectedResponseCount == 0 , collectedResponseCount.incrementAndGet() will be greater than expectedResponseCount
            if (collectedResponseCount.incrementAndGet() >= maxResponseCount.get()) {
                finish();
            }
        }

    }

    @Override
    public void onFailure(Exception e) {
        LOG.error(e);
        try {
            this.exceptions.add(e.getMessage());
        } finally {
            // no matter the asynchronous request is a failure or success, we need to increment the count.
            // We need finally here to increment the count when there is a failure.
            if (collectedResponseCount.incrementAndGet() >= maxResponseCount.get()) {
                finish();
            }
        }
    }

    private void finish() {
        if (this.returnOnPartialResults || this.exceptions.size() == 0) {
            if (this.exceptions.size() > 0) {
                LOG.error(String.format(Locale.ROOT, "Although returning result, there exists exceptions: %s", this.exceptions));
            }
            handleSavedResponses();
        } else {
            this.delegate.onFailure(new RuntimeException(String.format(Locale.ROOT, finalErrorMsg + " Exceptions: %s", exceptions)));
        }
    }

    private void handleSavedResponses() {
        if (savedResponses.size() == 0) {
            this.delegate.onFailure(new RuntimeException(NO_RESPONSE));
        } else {
            T response0 = savedResponses.get(0);
            for (int i = 1; i < savedResponses.size(); i++) {
                response0.merge(savedResponses.get(i));
            }
            this.delegate.onResponse(response0);
        }
    }
}
