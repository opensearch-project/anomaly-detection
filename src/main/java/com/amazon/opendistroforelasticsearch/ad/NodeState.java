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

package com.amazon.opendistroforelasticsearch.ad;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

/**
 * Storing intermediate state during the execution of transport action
 *
 */
public class NodeState implements ExpiringState {
    private String detectorId;
    // detector definition
    private AnomalyDetector detectorDef;
    // number of partitions
    private int partitonNumber;
    // checkpoint fetch time
    private Instant lastAccessTime;
    // last detection error recorded in result index. Used by DetectorStateHandler
    // to check if the error for a detector has changed or not. If changed, trigger indexing.
    private Optional<String> lastDetectionError;
    // last training error. Used to save cold start error by a concurrent cold start thread.
    private Optional<AnomalyDetectionException> lastColdStartException;
    // flag indicating whether checkpoint for the detector exists
    private boolean checkPointExists;
    // clock to get current time
    private final Clock clock;
    // cold start running flag to prevent concurrent cold start
    private boolean coldStartRunning;

    public NodeState(String detectorId, Clock clock) {
        this.detectorId = detectorId;
        this.detectorDef = null;
        this.partitonNumber = -1;
        this.lastAccessTime = clock.instant();
        this.lastDetectionError = Optional.empty();
        this.lastColdStartException = Optional.empty();
        this.checkPointExists = false;
        this.clock = clock;
        this.coldStartRunning = false;
    }

    public String getDetectorId() {
        return detectorId;
    }

    /**
     *
     * @return Detector configuration object
     */
    public AnomalyDetector getDetectorDef() {
        refreshLastUpdateTime();
        return detectorDef;
    }

    /**
     *
     * @param detectorDef Detector configuration object
     */
    public void setDetectorDef(AnomalyDetector detectorDef) {
        this.detectorDef = detectorDef;
        refreshLastUpdateTime();
    }

    /**
     *
     * @return RCF partition number of the detector
     */
    public int getPartitonNumber() {
        refreshLastUpdateTime();
        return partitonNumber;
    }

    /**
     *
     * @param partitonNumber RCF partition number
     */
    public void setPartitonNumber(int partitonNumber) {
        this.partitonNumber = partitonNumber;
        refreshLastUpdateTime();
    }

    /**
     * Used to indicate whether cold start succeeds or not
     * @return whether checkpoint of models exists or not.
     */
    public boolean doesCheckpointExists() {
        refreshLastUpdateTime();
        return checkPointExists;
    }

    /**
     *
     * @param checkpointExists mark whether checkpoint of models exists or not.
     */
    public void setCheckpointExists(boolean checkpointExists) {
        refreshLastUpdateTime();
        this.checkPointExists = checkpointExists;
    };

    /**
     *
     * @return last model inference error
     */
    public Optional<String> getLastDetectionError() {
        refreshLastUpdateTime();
        return lastDetectionError;
    }

    /**
     *
     * @param lastError last model inference error
     */
    public void setLastDetectionError(String lastError) {
        this.lastDetectionError = Optional.ofNullable(lastError);
        refreshLastUpdateTime();
    }

    /**
     *
     * @return last cold start exception if any
     */
    public Optional<AnomalyDetectionException> getLastColdStartException() {
        refreshLastUpdateTime();
        return lastColdStartException;
    }

    /**
     *
     * @param lastColdStartError last cold start exception if any
     */
    public void setLastColdStartException(AnomalyDetectionException lastColdStartError) {
        this.lastColdStartException = Optional.ofNullable(lastColdStartError);
        refreshLastUpdateTime();
    }

    /**
     * Used to prevent concurrent cold start
     * @return whether cold start is running or not
     */
    public boolean isColdStartRunning() {
        refreshLastUpdateTime();
        return coldStartRunning;
    }

    /**
     *
     * @param coldStartRunning  whether cold start is running or not
     */
    public void setColdStartRunning(boolean coldStartRunning) {
        this.coldStartRunning = coldStartRunning;
        refreshLastUpdateTime();
    }

    /**
     * refresh last access time.
     */
    private void refreshLastUpdateTime() {
        lastAccessTime = clock.instant();
    }

    /**
     * @param stateTtl time to leave for the state
     * @return whether the transport state is expired
     */
    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastAccessTime, stateTtl, clock.instant());
    }
}
