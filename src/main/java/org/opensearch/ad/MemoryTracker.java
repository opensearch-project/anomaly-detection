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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.sdk.SDKClusterService;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

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
    private ADCircuitBreakerService adCircuitBreakerService;

    /**
     * Constructor
     *
     * @param jvmService Service providing jvm info
     * @param modelMaxSizePercentage Percentage of heap for the max size of a model
     * @param modelDesiredSizePercentage percentage of heap for the desired size of a model
     * @param clusterService Cluster service object
     * @param adCircuitBreakerService Memory circuit breaker
     */
    public MemoryTracker(
        JvmService jvmService,
        double modelMaxSizePercentage,
        double modelDesiredSizePercentage,
        SDKClusterService clusterService,
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
        this.adCircuitBreakerService = adCircuitBreakerService;
    }

    /**
     * This function derives from the old code: https://tinyurl.com/2eaabja6
     *
     * @param detectorId Detector Id
     * @param trcf Thresholded random cut forest model
     * @return true if there is enough memory; otherwise throw LimitExceededException.
     */
    public synchronized boolean isHostingAllowed(String detectorId, ThresholdedRandomCutForest trcf) {
        long requiredBytes = estimateTRCFModelSize(trcf);
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
     * @param trcf ThresholdedRandomCutForest object
     * @return estimated model size in bytes
     */
    public long estimateTRCFModelSize(ThresholdedRandomCutForest trcf) {
        RandomCutForest forest = trcf.getForest();
        return estimateTRCFModelSize(
            forest.getDimensions(),
            forest.getNumberOfTrees(),
            forest.getBoundingBoxCacheFraction(),
            forest.getShingleSize(),
            forest.isInternalShinglingEnabled()
        );
    }

    /**
     * Gets the estimated size of an entity's model.
     *
     * RCF size:
     * Assume the sample size is 256. I measured the memory size of a ThresholdedRandomCutForest
     * using heap dump.  A ThresholdedRandomCutForest comprises a compact rcf model and
     * a threshold model.
     *
     * A compact RCF forest consists of:
     * - Random number generator: 56 bytes
     * - PointStoreCoordinator: 24 bytes
     * - SequentialForestUpdateExecutor: 24 bytes
     * - SequentialForestTraversalExecutor: 16 bytes
     * - PointStoreFloat
     *   + IndexManager
     *     - int array for free indexes: 256 * numberOfTrees * 4, where 4 is the size of an integer
     *   - two int array for locationList and refCount: 256 * numberOfTrees * 4 bytes * 2
     *   - a float array for data store: 256 * trees * dimension * 4 bytes: due to various
     *     optimization like shingleSize(dimensions), we don't use all of the array.  The average
     *     usage percentage depends on shingle size and if internal shingling is enabled.
     *     I did experiments with power-of-two shingle sizes and internal shingling on/off
     *     by running ThresholdedRandomCutForest over a million points.
     *     My experiment shows that
     *     * if internal shingling is off, data store is filled at full
     *     capacity.
     *     * otherwise, data store usage depends on shingle size:
     *
     *     Shingle Size           usage
     *     1                       1
     *     2                      0.53
     *     4                      0.27
     *     8                      0.27
     *     16                     0.13
     *     32                     0.07
     *     64                     0.07
     *
     *    The formula reflects the data and fits the point store usage to the closest power-of-two case.
     *    For example, if shingle size is 17, we use the usage 0.13 since it is closer to 16.
     *
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
     *        - BoundingBoxFloat: (1040 + 255* ((dimension * 4 + 16) * 2 + 32)) * actual bounding box cache usage,
     *           {@code actual bounding box cache usage = (bounding box cache fraction >= 0.3? 1: bounding box cache fraction)}
     *           {@code >= 0.3} we will still initialize bounding box cache array of the max size.
     *           1040 is the size of BoundingBoxFloat's fields unrelated to tree size (255 nodes in our formula)
     * In total, RCF size is
     *  56 + # trees * (2248 + 152 + 6120 + 104 + (1040 + 255* (dimension * 4 + 16) * 2 + 32)) * adjusted bounding box cache ratio) +
     *  (256 * # trees  * 2 + 256 * # trees * dimension) * 4 bytes  * point store ratio + 30744 * 2 + 15432 + 208) + 24 + 24 + 16
     *  = 56 + # trees * (8624 + (1040 + 255 * (dimension * 8 + 64)) * actual bounding box cache usage) + 256 * # trees *
     *   dimension * 4 * point store ratio + 77192
     *
     *  Thresholder size
     *   + Preprocessor:
     *     - lastShingledInput and lastShingledPoint: 2*(dimension*8 + 16) (2 due to 2 double arrays, 16 are array object size)
     *     - previousTimeStamps: shingle*8
     *     - other: 248
     *   - BasicThrehsolder: 256
     *   + lastAnomalyAttribution:
     *      - high and low: 2*(dimension*8 + 16)(2 due to 2 double arrays, 16 are array object)
     *      - other 24
     *   - lastAnomalyPoint and lastExpectedPoint:  2*(dimension*8 + 16)
     *   -  other like ThresholdedRandomCutForest object size: 96
     * In total, thresholder size is:
     *  6*(dimension*8 + 16) + shingle*8 + 248 + 256 + 24 + 96
     *  = 6*(dimension*8 + 16) + shingle*8 + 624
     *
     * @param dimension The number of feature dimensions in RCF
     * @param numberOfTrees The number of trees in RCF
     * @param boundingBoxCacheFraction Bounding box cache usage in RCF
     * @param shingleSize shingle size
     * @param internalShingling whether internal shingling is enabled or not
     * @return estimated TRCF model size
     *
     * @throws IllegalArgumentException when the input shingle size is out of range [1, 64]
     */
    public long estimateTRCFModelSize(
        int dimension,
        int numberOfTrees,
        double boundingBoxCacheFraction,
        int shingleSize,
        boolean internalShingling
    ) {
        double averagePointStoreUsage = 0;
        if (!internalShingling || shingleSize == 1) {
            averagePointStoreUsage = 1;
        } else if (shingleSize <= 3) {
            averagePointStoreUsage = 0.53;
        } else if (shingleSize <= 12) {
            averagePointStoreUsage = 0.27;
        } else if (shingleSize <= 24) {
            averagePointStoreUsage = 0.13;
        } else if (shingleSize <= 64) {
            averagePointStoreUsage = 0.07;
        } else {
            throw new IllegalArgumentException("out of range shingle size " + shingleSize);
        }

        double actualBoundingBoxUsage = boundingBoxCacheFraction >= 0.3 ? 1d : boundingBoxCacheFraction;
        long compactRcfSize = (long) (56 + numberOfTrees * (8624 + (1040 + 255 * (dimension * 8 + 64)) * actualBoundingBoxUsage) + 256
            * numberOfTrees * dimension * 4 * averagePointStoreUsage + 77192);
        long thresholdSize = 6 * (dimension * 8 + 16) + shingleSize * 8 + 624;
        return compactRcfSize + thresholdSize;
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
