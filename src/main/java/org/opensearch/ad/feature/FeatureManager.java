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

package org.opensearch.ad.feature;

import static java.util.Arrays.copyOfRange;
import static org.apache.commons.math3.linear.MatrixUtils.createRealMatrix;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.CleanState;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.dataprocessor.Imputer;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Entity;

/**
 * A facade managing feature data operations and buffers.
 */
public class FeatureManager implements CleanState {

    private static final Logger logger = LogManager.getLogger(FeatureManager.class);

    // Each anomaly detector has a queue of data points with timestamps (in epoch milliseconds).
    private final Map<String, ArrayDeque<Entry<Long, Optional<double[]>>>> detectorIdsToTimeShingles;

    private final SearchFeatureDao searchFeatureDao;
    private final Imputer imputer;
    private final Clock clock;

    private final int maxTrainSamples;
    private final int maxSampleStride;
    private final int trainSampleTimeRangeInHours;
    private final int minTrainSamples;
    private final double maxMissingPointsRate;
    private final int maxNeighborDistance;
    private final double previewSampleRate;
    private final int maxPreviewSamples;
    private final Duration featureBufferTtl;
    private final ThreadPool threadPool;
    private final String adThreadPoolName;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param searchFeatureDao DAO of features from search
     * @param imputer imputer of samples
     * @param clock clock for system time
     * @param maxTrainSamples max number of samples from search
     * @param maxSampleStride max stride between uninterpolated train samples
     * @param trainSampleTimeRangeInHours time range in hours for collect train samples
     * @param minTrainSamples min number of train samples
     * @param maxMissingPointsRate max proportion of shingle with missing points allowed to generate a shingle
     * @param maxNeighborDistance max distance (number of intervals) between a missing point and a replacement neighbor
     * @param previewSampleRate number of samples to number of all the data points in the preview time range
     * @param maxPreviewSamples max number of samples from search for preview features
     * @param featureBufferTtl time to live for stale feature buffers
     * @param threadPool object through which we can invoke different threadpool using different names
     * @param adThreadPoolName AD threadpool's name
     */
    public FeatureManager(
        SearchFeatureDao searchFeatureDao,
        Imputer imputer,
        Clock clock,
        int maxTrainSamples,
        int maxSampleStride,
        int trainSampleTimeRangeInHours,
        int minTrainSamples,
        double maxMissingPointsRate,
        int maxNeighborDistance,
        double previewSampleRate,
        int maxPreviewSamples,
        Duration featureBufferTtl,
        ThreadPool threadPool,
        String adThreadPoolName
    ) {
        this.searchFeatureDao = searchFeatureDao;
        this.imputer = imputer;
        this.clock = clock;
        this.maxTrainSamples = maxTrainSamples;
        this.maxSampleStride = maxSampleStride;
        this.trainSampleTimeRangeInHours = trainSampleTimeRangeInHours;
        this.minTrainSamples = minTrainSamples;
        this.maxMissingPointsRate = maxMissingPointsRate;
        this.maxNeighborDistance = maxNeighborDistance;
        this.previewSampleRate = previewSampleRate;
        this.maxPreviewSamples = maxPreviewSamples;
        this.featureBufferTtl = featureBufferTtl;

        this.detectorIdsToTimeShingles = new ConcurrentHashMap<>();
        this.threadPool = threadPool;
        this.adThreadPoolName = adThreadPoolName;
    }

    /**
     * Returns to listener unprocessed features and processed features (such as shingle) for the current data point.
     * The listener's onFailure is called with EndRunException on feature query creation errors.
     *
     * This method sends a single query for historical data for data points (including the current point) that are missing
     * from the shingle, and updates the shingle which is persisted to future calls to this method for subsequent time
     * intervals. To allow for time variations/delays, an interval is considered missing from the shingle if no data point
     * is found within half an interval away. See doc for updateUnprocessedFeatures for details on how the shingle is
     * updated.
     *
     * @param detector anomaly detector for which the features are returned
     * @param startTime start time of the data point in epoch milliseconds
     * @param endTime end time of the data point in epoch milliseconds
     * @param listener onResponse is called with unprocessed features and processed features for the current data point
     */
    public void getCurrentFeatures(AnomalyDetector detector, long startTime, long endTime, ActionListener<SinglePointFeatures> listener) {

        int shingleSize = detector.getShingleSize();
        Deque<Entry<Long, Optional<double[]>>> shingle = detectorIdsToTimeShingles
            .computeIfAbsent(detector.getId(), id -> new ArrayDeque<>(shingleSize));

        // To allow for small time variations/delays in running the detector.
        long maxTimeDifference = detector.getIntervalInMilliseconds() / 2;
        Map<Long, Entry<Long, Optional<double[]>>> featuresMap = getNearbyPointsForShingle(detector, shingle, endTime, maxTimeDifference)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        List<Entry<Long, Long>> missingRanges = getMissingRangesInShingle(detector, featuresMap, endTime);

        if (missingRanges.size() > 0) {
            try {
                searchFeatureDao.getFeatureSamplesForPeriods(detector, missingRanges, AnalysisType.AD, ActionListener.wrap(points -> {
                    for (int i = 0; i < points.size(); i++) {
                        Optional<double[]> point = points.get(i);
                        long rangeEndTime = missingRanges.get(i).getValue();
                        featuresMap.put(rangeEndTime, new SimpleImmutableEntry<>(rangeEndTime, point));
                    }
                    updateUnprocessedFeatures(detector, shingle, featuresMap, endTime, listener);
                }, listener::onFailure));
            } catch (IOException e) {
                listener.onFailure(new EndRunException(detector.getId(), CommonMessages.INVALID_SEARCH_QUERY_MSG, e, true));
            }
        } else {
            listener.onResponse(getProcessedFeatures(shingle, detector, endTime));
        }
    }

    private List<Entry<Long, Long>> getMissingRangesInShingle(
        AnomalyDetector detector,
        Map<Long, Entry<Long, Optional<double[]>>> featuresMap,
        long endTime
    ) {
        long intervalMilli = detector.getIntervalInMilliseconds();
        int shingleSize = detector.getShingleSize();
        return getFullShingleEndTimes(endTime, intervalMilli, shingleSize)
            .filter(time -> !featuresMap.containsKey(time))
            .mapToObj(time -> new SimpleImmutableEntry<>(time - intervalMilli, time))
            .collect(Collectors.toList());
    }

    /**
     * Updates the shingle to contain one Optional data point for each of shingleSize consecutive time intervals, ending
     * with the current interval. Each entry in the shingle contains the timestamp of the data point as the key, and the
     * data point wrapped in an Optional. If the data point is missing (even after querying, since this method is invoked
     * after querying), an entry with an empty Optional value is stored in the shingle to prevent subsequent calls to
     * getCurrentFeatures from re-querying the missing data point again.
     *
     * Note that in the presence of time variations/delays up to half an interval, the shingle stores the actual original
     * end times of the data points, not the computed end times that were calculated based on the current endTime.
     * Ex: if data points are queried at times 100, 201, 299, then the shingle will contain [100: x, 201: y, 299: z].
     *
     * @param detector anomaly detector for which the features are returned.
     * @param shingle buffer which persists the past shingleSize data points to subsequent calls of getCurrentFeature.
     *                Each entry contains the timestamp of the data point and an optional data point value.
     * @param featuresMap A map where the keys are the computed millisecond timestamps associated with intervals in the
     *                    shingle, and the values are entries that contain the actual timestamp of the data point and
     *                    an optional data point value.
     * @param listener onResponse is called with unprocessed features and processed features for the current data point.
     */
    private void updateUnprocessedFeatures(
        AnomalyDetector detector,
        Deque<Entry<Long, Optional<double[]>>> shingle,
        Map<Long, Entry<Long, Optional<double[]>>> featuresMap,
        long endTime,
        ActionListener<SinglePointFeatures> listener
    ) {
        shingle.clear();
        getFullShingleEndTimes(endTime, detector.getIntervalInMilliseconds(), detector.getShingleSize())
            .mapToObj(time -> featuresMap.getOrDefault(time, new SimpleImmutableEntry<>(time, Optional.empty())))
            .forEach(e -> shingle.add(e));

        listener.onResponse(getProcessedFeatures(shingle, detector, endTime));
    }

    private double[][] filterAndFill(Deque<Entry<Long, Optional<double[]>>> shingle, long endTime, AnomalyDetector detector) {
        int shingleSize = detector.getShingleSize();
        Deque<Entry<Long, Optional<double[]>>> filteredShingle = shingle
            .stream()
            .filter(e -> e.getValue().isPresent())
            .collect(Collectors.toCollection(ArrayDeque::new));
        double[][] result = null;
        if (filteredShingle.size() >= shingleSize - getMaxMissingPoints(shingleSize)) {
            // Imputes missing data points with the values of neighboring data points.
            long maxMillisecondsDifference = maxNeighborDistance * detector.getIntervalInMilliseconds();
            result = getNearbyPointsForShingle(detector, filteredShingle, endTime, maxMillisecondsDifference)
                .map(e -> e.getValue().getValue().orElse(null))
                .filter(d -> d != null)
                .toArray(double[][]::new);

            if (result.length < shingleSize) {
                result = null;
            }
        }
        return result;
    }

    /**
     * Helper method that associates data points (along with their actual timestamps) to the intervals of a full shingle.
     *
     * Depending on the timestamp tolerance (maxMillisecondsDifference), this can be used to allow for small time
     * variations/delays in running the detector, or used for imputing missing points in the shingle with neighboring points.
     *
     * @return A stream of entries, where the key is the computed millisecond timestamp associated with an interval in
     * the shingle, and the value is an entry that contains the actual timestamp of the data point and an optional data
     * point value.
     */
    private Stream<Entry<Long, Entry<Long, Optional<double[]>>>> getNearbyPointsForShingle(
        AnomalyDetector detector,
        Deque<Entry<Long, Optional<double[]>>> shingle,
        long endTime,
        long maxMillisecondsDifference
    ) {
        long intervalMilli = detector.getIntervalInMilliseconds();
        int shingleSize = detector.getShingleSize();
        TreeMap<Long, Optional<double[]>> search = new TreeMap<>(
            shingle.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue))
        );
        return getFullShingleEndTimes(endTime, intervalMilli, shingleSize).mapToObj(t -> {
            Optional<Entry<Long, Optional<double[]>>> after = Optional.ofNullable(search.ceilingEntry(t));
            Optional<Entry<Long, Optional<double[]>>> before = Optional.ofNullable(search.floorEntry(t));
            return after
                .filter(a -> Math.abs(t - a.getKey()) <= before.map(b -> Math.abs(t - b.getKey())).orElse(Long.MAX_VALUE))
                .map(Optional::of)
                .orElse(before)
                .filter(e -> Math.abs(t - e.getKey()) < maxMillisecondsDifference)
                .map(e -> new SimpleImmutableEntry<>(t, e));
        }).filter(Optional::isPresent).map(Optional::get);
    }

    private LongStream getFullShingleEndTimes(long endTime, long intervalMilli, int shingleSize) {
        return LongStream.rangeClosed(1, shingleSize).map(i -> endTime - (shingleSize - i) * intervalMilli);
    }

    /**
     * Returns to listener data for cold-start training.
     *
     * Training data starts with getting samples from (costly) search.
     * Samples are increased in dimension via shingling.
     *
     * @param detector contains data info (indices, documents, etc)
     * @param listener onResponse is called with data for cold-start training, or empty if unavailable
     *                 onFailure is called with EndRunException on feature query creation errors
     */
    public void getColdStartData(AnomalyDetector detector, ActionListener<Optional<double[][]>> listener) {
        ActionListener<Optional<Long>> latestTimeListener = ActionListener
            .wrap(latest -> getColdStartSamples(latest, detector, listener), listener::onFailure);
        searchFeatureDao
            .getLatestDataTime(detector, new ThreadedActionListener<>(logger, threadPool, adThreadPoolName, latestTimeListener, false));
    }

    private void getColdStartSamples(Optional<Long> latest, AnomalyDetector detector, ActionListener<Optional<double[][]>> listener) {
        int shingleSize = detector.getShingleSize();
        if (latest.isPresent()) {
            List<Entry<Long, Long>> sampleRanges = getColdStartSampleRanges(detector, latest.get());
            try {
                ActionListener<List<Optional<double[]>>> getFeaturesListener = ActionListener
                    .wrap(samples -> processColdStartSamples(samples, shingleSize, listener), listener::onFailure);
                searchFeatureDao
                    .getFeatureSamplesForPeriods(
                        detector,
                        sampleRanges,
                        AnalysisType.AD,
                        new ThreadedActionListener<>(logger, threadPool, adThreadPoolName, getFeaturesListener, false)
                    );
            } catch (IOException e) {
                listener.onFailure(new EndRunException(detector.getId(), CommonMessages.INVALID_SEARCH_QUERY_MSG, e, true));
            }
        } else {
            listener.onResponse(Optional.empty());
        }
    }

    private void processColdStartSamples(List<Optional<double[]>> samples, int shingleSize, ActionListener<Optional<double[][]>> listener) {
        List<double[]> shingles = new ArrayList<>();
        LinkedList<Optional<double[]>> currentShingle = new LinkedList<>();
        for (Optional<double[]> sample : samples) {
            currentShingle.addLast(sample);
            if (currentShingle.size() == shingleSize) {
                sample.ifPresent(s -> fillAndShingle(currentShingle, shingleSize).ifPresent(shingles::add));
                currentShingle.remove();
            }
        }
        listener.onResponse(Optional.of(shingles.toArray(new double[0][0])).filter(results -> results.length > 0));
    }

    private Optional<double[]> fillAndShingle(LinkedList<Optional<double[]>> shingle, int shingleSize) {
        Optional<double[]> result = null;
        if (shingle.stream().filter(s -> s.isPresent()).count() >= shingleSize - getMaxMissingPoints(shingleSize)) {
            TreeMap<Integer, double[]> search = new TreeMap<>(
                IntStream
                    .range(0, shingleSize)
                    .filter(i -> shingle.get(i).isPresent())
                    .boxed()
                    .collect(Collectors.toMap(i -> i, i -> shingle.get(i).get()))
            );
            result = Optional.of(IntStream.range(0, shingleSize).mapToObj(i -> {
                Optional<Entry<Integer, double[]>> after = Optional.ofNullable(search.ceilingEntry(i));
                Optional<Entry<Integer, double[]>> before = Optional.ofNullable(search.floorEntry(i));
                return after
                    .filter(a -> Math.abs(i - a.getKey()) <= before.map(b -> Math.abs(i - b.getKey())).orElse(Integer.MAX_VALUE))
                    .map(Optional::of)
                    .orElse(before)
                    .filter(e -> Math.abs(i - e.getKey()) <= maxNeighborDistance)
                    .map(Entry::getValue)
                    .orElse(null);
            }).filter(d -> d != null).toArray(double[][]::new))
                .filter(d -> d.length == shingleSize)
                .map(d -> batchShingle(d, shingleSize)[0]);
        } else {
            result = Optional.empty();
        }
        return result;
    }

    private List<Entry<Long, Long>> getColdStartSampleRanges(AnomalyDetector detector, long endMillis) {
        long interval = detector.getIntervalInMilliseconds();
        int numSamples = Math.max((int) (Duration.ofHours(this.trainSampleTimeRangeInHours).toMillis() / interval), this.minTrainSamples);
        return IntStream
            .rangeClosed(1, numSamples)
            .mapToObj(i -> new SimpleImmutableEntry<>(endMillis - (numSamples - i + 1) * interval, endMillis - (numSamples - i) * interval))
            .collect(Collectors.toList());
    }

    /**
     * Shingles a batch of data points by concatenating neighboring data points.
     *
     * @param points M, N where M is the number of data points and N is the dimension of a point
     * @param shingleSize the size of a shingle
     * @return P, Q where P = M - {@code shingleSize} + 1 and Q = N * {@code shingleSize}
     * @throws IllegalArgumentException when input is invalid
     */
    public double[][] batchShingle(double[][] points, int shingleSize) {
        if (points.length == 0 || points[0].length == 0 || points.length < shingleSize || shingleSize < 1) {
            throw new IllegalArgumentException("Invalid data for shingling.");
        }
        int numPoints = points.length;
        int dimPoint = points[0].length;
        int numShingles = numPoints - shingleSize + 1;
        int dimShingle = dimPoint * shingleSize;
        double[][] shingles = new double[numShingles][dimShingle];
        for (int i = 0; i < numShingles; i++) {
            for (int j = 0; j < shingleSize; j++) {
                System.arraycopy(points[i + j], 0, shingles[i], j * dimPoint, dimPoint);
            }
        }
        return shingles;
    }

    /**
     * Deletes managed features for the detector.
     *
     * @param detectorId ID of the detector
     */
    @Override
    public void clear(String detectorId) {
        detectorIdsToTimeShingles.remove(detectorId);
    }

    /**
     * Does maintenance work.
     *
     * The current implementation removes feature buffers that are updated more than ttlFeatureBuffer (3 days for example) ago.
     * The cleanup is needed since feature buffers are not explicitly deleted after a detector is deleted or relocated.
     */
    public void maintenance() {
        try {
            detectorIdsToTimeShingles
                .entrySet()
                .removeIf(
                    idQueue -> Optional
                        .ofNullable(idQueue.getValue().peekLast())
                        .map(p -> Instant.ofEpochMilli(p.getKey()).plus(featureBufferTtl).isBefore(clock.instant()))
                        .orElse(true)
                );
        } catch (Exception e) {
            logger.warn("Caught exception during maintenance", e);
        }
    }

    /**
     * Returns the entities for preview to listener
     * @param detector detector config
     * @param startTime start of the range in epoch milliseconds
     * @param endTime end of the range in epoch milliseconds
     * @param listener onResponse is called when entities are found
     */
    public void getPreviewEntities(AnomalyDetector detector, long startTime, long endTime, ActionListener<List<Entity>> listener) {
        searchFeatureDao.getHighestCountEntities(detector, startTime, endTime, listener);
    }

    /**
     * Returns to listener feature data points (unprocessed and processed) from the period for preview purpose for specific entity.
     *
     * Due to the constraints (workload, latency) from preview, a small number of data samples are from actual
     * query results and the remaining are from interpolation. The results are approximate to the actual features.
     *
     * @param detector detector info containing indices, features, interval, etc
     * @param entity entity specified
     * @param startMilli start of the range in epoch milliseconds
     * @param endMilli end of the range in epoch milliseconds
     * @param listener onResponse is called with time ranges, unprocessed features,
     *                                      and processed features of the data points from the period
     *                 onFailure is called with IllegalArgumentException when there is no data to preview
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    public void getPreviewFeaturesForEntity(
        AnomalyDetector detector,
        Entity entity,
        long startMilli,
        long endMilli,
        ActionListener<Features> listener
    ) throws IOException {
        // TODO refactor this common lines so that these code can be run for 1 time for all entities
        Entry<List<Entry<Long, Long>>, Integer> sampleRangeResults = getSampleRanges(detector, startMilli, endMilli);
        List<Entry<Long, Long>> sampleRanges = sampleRangeResults.getKey();
        int stride = sampleRangeResults.getValue();
        int shingleSize = detector.getShingleSize();

        getPreviewSamplesInRangesForEntity(detector, sampleRanges, entity, getFeatureSamplesListener(stride, shingleSize, listener));
    }

    private ActionListener<Entry<List<Entry<Long, Long>>, double[][]>> getFeatureSamplesListener(
        int stride,
        int shingleSize,
        ActionListener<Features> listener
    ) {
        return ActionListener.wrap(samples -> {
            List<Entry<Long, Long>> searchTimeRange = samples.getKey();
            if (searchTimeRange.size() == 0) {
                listener.onFailure(new IllegalArgumentException("No data to preview anomaly detection."));
                return;
            }
            double[][] sampleFeatures = samples.getValue();
            List<Entry<Long, Long>> previewRanges = getPreviewRanges(searchTimeRange, stride, shingleSize);
            Entry<double[][], double[][]> previewFeatures = getPreviewFeatures(sampleFeatures, stride, shingleSize);
            listener.onResponse(new Features(previewRanges, previewFeatures.getKey(), previewFeatures.getValue()));
        }, listener::onFailure);
    }

    /**
     * Returns to listener feature data points (unprocessed and processed) from the period for preview purpose.
     *
     * Due to the constraints (workload, latency) from preview, a small number of data samples are from actual
     * query results and the remaining are from interpolation. The results are approximate to the actual features.
     *
     * @param detector detector info containing indices, features, interval, etc
     * @param startMilli start of the range in epoch milliseconds
     * @param endMilli end of the range in epoch milliseconds
     * @param listener onResponse is called with time ranges, unprocessed features,
     *                                      and processed features of the data points from the period
     *                 onFailure is called with IllegalArgumentException when there is no data to preview
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    public void getPreviewFeatures(AnomalyDetector detector, long startMilli, long endMilli, ActionListener<Features> listener)
        throws IOException {
        Entry<List<Entry<Long, Long>>, Integer> sampleRangeResults = getSampleRanges(detector, startMilli, endMilli);
        List<Entry<Long, Long>> sampleRanges = sampleRangeResults.getKey();
        int stride = sampleRangeResults.getValue();
        int shingleSize = detector.getShingleSize();

        getSamplesForRanges(detector, sampleRanges, getFeatureSamplesListener(stride, shingleSize, listener));
    }

    /**
     * Gets time ranges of sampled data points.
     *
     * To reduce workload/latency from search, most data points in the preview time ranges are not from search results.
     * This implementation selects up to maxPreviewSamples evenly spaced points from the entire time range.
     *
     * @return key is a list of sampled time ranges, value is the stride between samples
     */
    private Entry<List<Entry<Long, Long>>, Integer> getSampleRanges(AnomalyDetector detector, long startMilli, long endMilli) {
        long start = truncateToMinute(startMilli);
        long end = truncateToMinute(endMilli);
        long bucketSize = detector.getIntervalInMilliseconds();
        int numBuckets = (int) Math.floor((end - start) / (double) bucketSize);
        int numSamples = (int) Math.max(Math.min(numBuckets * previewSampleRate, maxPreviewSamples), 1);
        int stride = (int) Math.max(1, Math.floor((double) numBuckets / numSamples));
        int numStrides = (int) Math.ceil(numBuckets / (double) stride);
        List<Entry<Long, Long>> sampleRanges = Stream
            .iterate(start, i -> i + stride * bucketSize)
            .limit(numStrides)
            .map(time -> new SimpleImmutableEntry<>(time, time + bucketSize))
            .collect(Collectors.toList());
        return new SimpleImmutableEntry<>(sampleRanges, stride);
    }

    /**
     * Gets search results in the sampled time ranges for specified entity.
     *
     * @param entity specified entity
     * @param listener handle search results map: key is time ranges, value is corresponding search results
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    void getPreviewSamplesInRangesForEntity(
        AnomalyDetector detector,
        List<Entry<Long, Long>> sampleRanges,
        Entity entity,
        ActionListener<Entry<List<Entry<Long, Long>>, double[][]>> listener
    ) throws IOException {
        searchFeatureDao
            .getColdStartSamplesForPeriods(
                detector,
                sampleRanges,
                Optional.ofNullable(entity),
                true,
                AnalysisType.AD,
                getSamplesRangesListener(sampleRanges, listener)
            );
    }

    private ActionListener<List<Optional<double[]>>> getSamplesRangesListener(
        List<Entry<Long, Long>> sampleRanges,
        ActionListener<Entry<List<Entry<Long, Long>>, double[][]>> listener
    ) {
        return ActionListener.wrap(featureSamples -> {
            List<Entry<Long, Long>> ranges = new ArrayList<>(featureSamples.size());
            List<double[]> samples = new ArrayList<>(featureSamples.size());
            for (int i = 0; i < featureSamples.size(); i++) {
                Entry<Long, Long> currentRange = sampleRanges.get(i);
                featureSamples.get(i).ifPresent(sample -> {
                    ranges.add(currentRange);
                    samples.add(sample);
                });
            }
            listener.onResponse(new SimpleImmutableEntry<>(ranges, samples.toArray(new double[0][0])));
        }, listener::onFailure);
    }

    /**
     * Gets search results for the sampled time ranges.
     *
     * @param listener handle search results map: key is time ranges, value is corresponding search results
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    void getSamplesForRanges(
        AnomalyDetector detector,
        List<Entry<Long, Long>> sampleRanges,
        ActionListener<Entry<List<Entry<Long, Long>>, double[][]>> listener
    ) throws IOException {
        searchFeatureDao
            .getFeatureSamplesForPeriods(detector, sampleRanges, AnalysisType.AD, getSamplesRangesListener(sampleRanges, listener));
    }

    /**
     * Gets time ranges for the data points in the preview range that begins with the first
     * sample time range and ends with the last.
     *
     * @param ranges time ranges of samples
     * @param stride the number of data points between samples
     * @param shingleSize the size of a shingle
     * @return time ranges for all data points
     */
    private List<Entry<Long, Long>> getPreviewRanges(List<Entry<Long, Long>> ranges, int stride, int shingleSize) {
        double[] rangeStarts = ranges.stream().mapToDouble(Entry::getKey).toArray();
        double[] rangeEnds = ranges.stream().mapToDouble(Entry::getValue).toArray();
        double[] previewRangeStarts = imputer.impute(new double[][] { rangeStarts }, stride * (ranges.size() - 1) + 1)[0];
        double[] previewRangeEnds = imputer.impute(new double[][] { rangeEnds }, stride * (ranges.size() - 1) + 1)[0];
        List<Entry<Long, Long>> previewRanges = IntStream
            .range(shingleSize - 1, previewRangeStarts.length)
            .mapToObj(i -> new SimpleImmutableEntry<>((long) previewRangeStarts[i], (long) previewRangeEnds[i]))
            .collect(Collectors.toList());
        return previewRanges;
    }

    /**
     * Gets unprocessed and processed features for the data points in the preview range.
     *
     * To reduce workload on search, the data points within the preview range are interpolated based on
     * sample query results. Unprocessed features are interpolated query results.
     * Processed features are inputs to models, transformed (such as shingle) from unprocessed features.
     *
     * @return unprocessed and processed features
     */
    private Entry<double[][], double[][]> getPreviewFeatures(double[][] samples, int stride, int shingleSize) {
        Entry<double[][], double[][]> unprocessedAndProcessed = Optional
            .of(samples)
            .map(m -> transpose(m))
            .map(m -> imputer.impute(m, stride * (samples.length - 1) + 1))
            .map(m -> transpose(m))
            .map(m -> new SimpleImmutableEntry<>(copyOfRange(m, shingleSize - 1, m.length), batchShingle(m, shingleSize)))
            .get();
        return unprocessedAndProcessed;
    }

    public double[][] transpose(double[][] matrix) {
        return createRealMatrix(matrix).transpose().getData();
    }

    private long truncateToMinute(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).truncatedTo(ChronoUnit.MINUTES).toEpochMilli();
    }

    /**
     * @return max number of missing points allowed to generate a shingle
     */
    private int getMaxMissingPoints(int shingleSize) {
        return (int) Math.floor(shingleSize * maxMissingPointsRate);
    }

    public int getShingleSize(String detectorId) {
        Deque<Entry<Long, Optional<double[]>>> shingle = detectorIdsToTimeShingles.get(detectorId);
        if (shingle != null) {
            return Math.toIntExact(shingle.stream().filter(entry -> entry.getValue().isPresent()).count());
        } else {
            return -1;
        }
    }

    public void getFeatureDataPointsByBatch(
        AnomalyDetector detector,
        Entity entity,
        long startTime,
        long endTime,
        ActionListener<Map<Long, Optional<double[]>>> listener
    ) {
        try {
            searchFeatureDao.getFeaturesForPeriodByBatch(detector, entity, startTime, endTime, ActionListener.wrap(points -> {
                logger.debug("features size: {}", points.size());
                listener.onResponse(points);
            }, listener::onFailure));
        } catch (Exception e) {
            logger.error("Failed to get features for detector: " + detector.getId());
            listener.onFailure(e);
        }
    }

    public SinglePointFeatures getShingledFeatureForHistoricalAnalysis(
        AnomalyDetector detector,
        Deque<Entry<Long, Optional<double[]>>> shingle,
        Optional<double[]> dataPoint,
        long endTime
    ) {
        while (shingle.size() >= detector.getShingleSize()) {
            shingle.poll();
        }
        shingle.add(new AbstractMap.SimpleEntry<>(endTime, dataPoint));

        return getProcessedFeatures(shingle, detector, endTime);
    }

    private SinglePointFeatures getProcessedFeatures(
        Deque<Entry<Long, Optional<double[]>>> shingle,
        AnomalyDetector detector,
        long endTime
    ) {
        int shingleSize = detector.getShingleSize();
        Optional<double[]> currentPoint = shingle.peekLast().getValue();
        return new SinglePointFeatures(
            currentPoint,
            Optional
                // if current point is not present or current shingle has more missing data points than
                // max missing rate, will return null
                .ofNullable(currentPoint.isPresent() ? filterAndFill(shingle, endTime, detector) : null)
                .map(points -> batchShingle(points, shingleSize)[0])
        );
    }

}
