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

package org.opensearch.timeseries.common.exception;

public class TaskCancelledException extends TimeSeriesException {
    private String cancelledBy;

    public TaskCancelledException(String msg, String user) {
        super(msg);
        this.cancelledBy = user;
        this.countedInStats(false);
    }

    public String getCancelledBy() {
        return cancelledBy;
    }
}
