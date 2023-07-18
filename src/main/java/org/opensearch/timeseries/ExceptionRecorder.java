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

package org.opensearch.timeseries;

import java.util.Optional;

public interface ExceptionRecorder {
    public void setException(String id, Exception e);

    public Optional<Exception> fetchExceptionAndClear(String id);
}
