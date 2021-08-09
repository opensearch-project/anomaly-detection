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

package org.opensearch.ad;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.monitor.jvm.JvmService;

import com.amazon.randomcutforest.RandomCutForest;

/**
 * Class to track AD memory usage.
 *
 */
public class MemoryTracker {
    private static final Logger LOG = LogManager.getLogger(MemoryTracker.class);

    public enum Origin {
        SINGLE_ENTITY_DETECTOR,
        HC_DETECTOR,
        HISTORICAL_SINGLE_ENTITY_DETECTOR,
    }

    // memory tracker for total consumption of bytes
    private long totalMemoryBytes;
    private final Map<Origin, Long> totalMemoryBytesByOrigin;
    // reserved for models. Cannot be deleted at will.
    private long reservedMemoryBytes;
    private final Map<Origin, Long> reservedMemoryBytesByOrigin;
    private long heapSize;
    private long heapLimitBytes;
    private long desiredModelSize;
    // we observe threshold model uses a fixed size array and the size is the same
    private int thresholdModelBytes;
    private int sampleSize;
    private ADCircuitBreakerService adCircuitBreakerService;

    /**
     * Constructor
     *
     * @param jvmService Service providing jvm info
     * @param modelMaxSizePercentage Percentage of heap for the max size of a model
     * @param modelDesiredSizePercentage percentage of heap for the desired size of a model
     * @param clusterService Cluster service object
     * @param sampleSize The sample size used by stream samplers in a RCF forest
     * @param adCircuitBreakerService Memory circuit breaker
     */
    public MemoryTracker(
        JvmService jvmService,
        double modelMaxSizePercentage,
        double modelDesiredSizePercentage,
        ClusterService clusterService,
        int sampleSize,
        ADCircuitBreakerService adCircuitBreakerService
    ) {
        this.totalMemoryBytes = 0;
        this.totalMemoryBytesByOrigin = new EnumMap<Origin, Long>(Origin.class);
        this.reservedMemoryBytes = 0;
        this.reservedMemoryBytesByOrigin = new EnumMap<Origin, Long>(Origin.class);
        this.heapSize = jvmService.info().getMem().getHeapMax().getBytes();
        this.heapLimitBytes = (long) (heapSize * modelMaxSizePercentage);
        this.desiredModelSize = (long) (heapSize * modelDesiredSizePercentage);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MODEL_MAX_SIZE_PERCENTAGE, it -> this.heapLimitBytes = (long) (heapSize * it));
        this.thresholdModelBytes = 180_000;
        this.sampleSize = sampleSize;
        this.adCircuitBreakerService = adCircuitBreakerService;
    }

    /**
     * This function derives from the old code: https://tinyurl.com/2eaabja6
     *
     * @param detectorId Detector Id
     * @param rcf Random cut forest model
     * @return true if there is enough memory; otherwise throw LimitExceededException.
     */
    public synchronized boolean isHostingAllowed(String detectorId, RandomCutForest rcf) {
        long requiredBytes = estimateTotalModelSize(rcf);
        if (canAllocateReserved(requiredBytes)) {
            return true;
        } else {
            throw new LimitExceededException(
                detectorId,
                String
                    .format(
                        Locale.ROOT,
                        "Exceeded memory limit. New size is %d bytes and max limit is %d bytes",
                        reservedMemoryBytes + requiredBytes,
                        heapLimitBytes
                    )
            );
        }
    }

    /**
     * @param requiredBytes required bytes to allocate
     * @return whether there is enough memory for the required bytes.  This is
     * true when circuit breaker is closed and there is enough reserved memory.
     */
    public synchronized boolean canAllocateReserved(long requiredBytes) {
        return (false == adCircuitBreakerService.isOpen() && reservedMemoryBytes + requiredBytes <= heapLimitBytes);
    }

    /**
     * @param bytes required bytes
     * @return whether there is enough memory for the required bytes.  This is
     * true when circuit breaker is closed and there is enough overall memory.
     */
    public synchronized boolean canAllocate(long bytes) {
        return false == adCircuitBreakerService.isOpen() && totalMemoryBytes + bytes <= heapLimitBytes;
    }

    public synchronized void consumeMemory(long memoryToConsume, boolean reserved, Origin origin) {
        totalMemoryBytes += memoryToConsume;
        adjustOriginMemoryConsumption(memoryToConsume, origin, totalMemoryBytesByOrigin);
        if (reserved) {
            reservedMemoryBytes += memoryToConsume;
            adjustOriginMemoryConsumption(memoryToConsume, origin, reservedMemoryBytesByOrigin);
        }
    }

    private void adjustOriginMemoryConsumption(long memoryToConsume, Origin origin, Map<Origin, Long> mapToUpdate) {
        Long originTotalMemoryBytes = mapToUpdate.getOrDefault(origin, 0L);
        mapToUpdate.put(origin, originTotalMemoryBytes + memoryToConsume);
    }

    public synchronized void releaseMemory(long memoryToShed, boolean reserved, Origin origin) {
        totalMemoryBytes -= memoryToShed;
        adjustOriginMemoryRelease(memoryToShed, origin, totalMemoryBytesByOrigin);
        if (reserved) {
            reservedMemoryBytes -= memoryToShed;
            adjustOriginMemoryRelease(memoryToShed, origin, reservedMemoryBytesByOrigin);
        }
    }

    private void adjustOriginMemoryRelease(long memoryToConsume, Origin origin, Map<Origin, Long> mapToUpdate) {
        Long originTotalMemoryBytes = mapToUpdate.get(origin);
        if (originTotalMemoryBytes != null) {
            mapToUpdate.put(origin, originTotalMemoryBytes - memoryToConsume);
        }
    }

    /**
     * Gets the estimated size of an entity's model.
     *
     * @param forest RCF forest object
     * @return estimated model size in bytes
     */
    public long estimateTotalModelSize(RandomCutForest forest) {
        return estimateRCFModelSize(forest.getDimensions(), forest.getNumberOfTrees(), forest.getBoundingBoxCacheFraction())
            + thresholdModelBytes;
    }

    /**
     * Gets the estimated size of a RCF model.
     *
     * @param forest RCF forest object
     * @return estimated model size in bytes
     */
    public long estimateRCFModelSize(RandomCutForest forest) {
        return estimateRCFModelSize(forest.getDimensions(), forest.getNumberOfTrees(), forest.getBoundingBoxCacheFraction());
    }

    /**
     * Gets the estimated size of an entity's model according to
     * the detector configuration.
     *
     * @param detector detector config object
     * @param numberOfTrees the number of trees in a RCF forest
     * @param boundingBoxCacheFraction Bounding box cache ratio in RCF
     * @return estimated model size in bytes
     */
    public long estimateTotalModelSize(AnomalyDetector detector, int numberOfTrees, double boundingBoxCacheFraction) {
        return estimateRCFModelSize(
            detector.getEnabledFeatureIds().size() * detector.getShingleSize(),
            numberOfTrees,
            boundingBoxCacheFraction
        ) + thresholdModelBytes;
    }

    /**
     * Gets the estimated size of an entity's model.
     *
     * RCF size:
     * Assume the sample size is 256. A compact RCF forest consists of:
     * - Random number generator: 56 bytes
     * - PointStoreCoordinator: 24 bytes
     * - SequentialForestUpdateExecutor: 24 bytes
     * - SequentialForestTraversalExecutor: 16 bytes
     * - PointStoreFloat
     *   + IndexManager
     *     - int array for free indexes: 256 * numberOfTrees * 4, where 4 is the size of an integer
     *   - two int array for locationList and refCount: 256 * numberOfTrees * 4 bytes * 2
     *   - a float array for data store: 256 * trees * dimension * 4 bytes: due to various
     *     optimization like shingleSize(dimensions), we don't use all of the array.  The actual
     *     usage percentage is
     *     {@code IF(dimensions>=32, 1/(LOG(dimensions+1, 2)+LOG(dimensions+1, 10)), 1/LOG(dimensions+1, 2))}
     *     where LOG gets the logarithm of a number and the syntax of LOG is {@code LOG (number, [base])}.
     *     We derive the formula by observing the point store usage ratio is a decreasing function of dimensions
     *     and the relationship is logarithm. Adding 1 to dimension to ensure dimension 1 results in a ratio 1.
     * - ComponentList: an array of size numberOfTrees
     *   + SamplerPlusTree
     *    - CompactSampler: 2248
     *    + CompactRandomCutTreeFloat
     *      - other fields: 152
     *      - SmallNodeStore (small node store since our sample size is 256, less than the max of short): 6120
     *      + BoxCacheFloat
     *        - other: 104
     *        - BoundingBoxFloat: (1040 + 255* (dimension * 4 * 2 + 64)) * adjusted bounding box cache usage,
     *           where if full we have 255 inner node and each box has 80 bytes.
     *           Plus metadata, we can have in total 21544 bytes.
     *           {@code adjusted bounding box cache usage = (bounding box cache fraction >= 0.3? 1: bounding box cache fraction)}
     *           {@code >= 0.3} we will still initialize bounding box cache array of the max size,
     *           but exclude them using the cache ratio.  It is not guaranteed we will only
     *           use cache ratio in the array.  For example, with cache ratio 0.5, we used 150
     *           out of 255 elements.  So can have two float array whose size is the number of
     *           dimensions; other constants are the metadata size.
     * In total, RCF size is
     *  56 + # trees * (2248 + 152 + 6120 + 104 + (1040 + 255* (dimension * 4 * 2 + 64)) * adjusted bounding box cache ratio) +
     *  (256 * # trees  * 2 + 256 * # trees * dimension) * 4 bytes  * 0.5 + 1064 + 24 + 24 + 16
     *  = 56 + # trees * (8624 + (1040 + 255 * (dimension * 8 + 64)) * adjusted bounding box cache ratio) + 256 * # trees *
     *   (3 + dimension) * 4 * 0.5 + 1128
     *
     * @param dimension The number of feature dimensions in RCF
     * @param numberOfTrees The number of trees in RCF
     * @param boundingBoxCacheFraction Bounding box cache ratio in RCF
     * @return estimated RCF model size
     */
    public long estimateRCFModelSize(int dimension, int numberOfTrees, double boundingBoxCacheFraction) {
        double averagePointStoreUsage = 0;
        int logNumber = dimension + 1;
        if (dimension >= 32) {
            averagePointStoreUsage = 1.0d / (log2(logNumber) + Math.log10(logNumber));
        } else {
            averagePointStoreUsage = 1.0d / log2(logNumber);
        }
        double actualBoundingBoxUsage = boundingBoxCacheFraction >= 0.3 ? 1d : boundingBoxCacheFraction;
        long compactRcfSize = (long) (56 + numberOfTrees * (8624 + (1040 + 255 * (dimension * 8 + 64)) * actualBoundingBoxUsage) + 256
            * numberOfTrees * (3 + dimension) * 4 * averagePointStoreUsage + 1128);
        return compactRcfSize;
    }

    /**
     * Function to calculate the log base 2 of an integer
     *
     * @param N input number
     * @return the base 2 logarithm of an integer value
     */
    public static double log2(int N) {
        // calculate log2 N indirectly using log() method
        return Math.log(N) / Math.log(2);
    }

    public long estimateTotalModelSize(int dimension, int numberOfTrees, double boundingBoxCacheFraction) {
        return estimateRCFModelSize(dimension, numberOfTrees, boundingBoxCacheFraction) + thresholdModelBytes;
    }

    /**
     * Bytes to remove to keep AD memory usage within the limit
     * @return bytes to remove
     */
    public synchronized long memoryToShed() {
        return totalMemoryBytes - heapLimitBytes;
    }

    /**
     *
     * @return Allowed heap usage in bytes by AD models
     */
    public long getHeapLimit() {
        return heapLimitBytes;
    }

    /**
     *
     * @return Desired model partition size in bytes
     */
    public long getDesiredModelSize() {
        return desiredModelSize;
    }

    public long getTotalMemoryBytes() {
        return totalMemoryBytes;
    }

    /**
     * In case of bugs/race conditions or users dyanmically changing dedicated/shared
     * cache size, sync used bytes infrequently by recomputing memory usage.
     * @param origin Origin
     * @param totalBytes total bytes from recomputing
     * @param reservedBytes reserved bytes from recomputing
     * @return whether memory adjusted due to mismatch
     */
    public synchronized boolean syncMemoryState(Origin origin, long totalBytes, long reservedBytes) {
        long recordedTotalBytes = totalMemoryBytesByOrigin.getOrDefault(origin, 0L);
        long recordedReservedBytes = reservedMemoryBytesByOrigin.getOrDefault(origin, 0L);
        if (totalBytes == recordedTotalBytes && reservedBytes == recordedReservedBytes) {
            return false;
        }

        LOG
            .info(
                String
                    .format(
                        Locale.ROOT,
                        "Memory states do not match.  Recorded: total bytes %d, reserved bytes %d."
                            + "Actual: total bytes %d, reserved bytes: %d",
                        recordedTotalBytes,
                        recordedReservedBytes,
                        totalBytes,
                        reservedBytes
                    )
            );
        // reserved bytes mismatch
        long reservedDiff = reservedBytes - recordedReservedBytes;
        reservedMemoryBytesByOrigin.put(origin, reservedBytes);
        reservedMemoryBytes += reservedDiff;

        long totalDiff = totalBytes - recordedTotalBytes;
        totalMemoryBytesByOrigin.put(origin, totalBytes);
        totalMemoryBytes += totalDiff;
        return true;
    }

    public int getThresholdModelBytes() {
        return thresholdModelBytes;
    }
}
