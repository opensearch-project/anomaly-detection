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

package com.amazon.opendistroforelasticsearch.ad.common.exception;

/**
 * All exception visible to AD transport layer's client is under ClientException.
 */
public class ClientException extends AnomalyDetectionException {

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String anomalyDetectorId, String message) {
        super(anomalyDetectorId, message);
    }

    public ClientException(String anomalyDetectorId, String message, Throwable throwable) {
        super(anomalyDetectorId, message, throwable);
    }

    public ClientException(String anomalyDetectorId, Throwable cause) {
        super(anomalyDetectorId, cause);
    }
}
