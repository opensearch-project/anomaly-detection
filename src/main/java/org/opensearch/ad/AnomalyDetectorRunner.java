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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.Features;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.EntityAnomalyResult;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.util.MultiResponsesDelegateActionListener;
import org.opensearch.common.util.concurrent.ThreadContext;

/**
 * Runner to trigger an anomaly detector.
 */
public final class AnomalyDetectorRunner {

    private final Logger logger = LogManager.getLogger(AnomalyDetectorRunner.class);
    private final ModelManager modelManager;
    private final FeatureManager featureManager;
    private final int maxPreviewResults;

    public AnomalyDetectorRunner(ModelManager modelManager, FeatureManager featureManager, int maxPreviewResults) {
        this.modelManager = modelManager;
        this.featureManager = featureManager;
        this.maxPreviewResults = maxPreviewResults;
    }

    /**
     * run anomaly detector and return anomaly result.
     *
     * @param detector  anomaly detector instance
     * @param startTime detection period start time
     * @param endTime   detection period end time
     * @param listener handle anomaly result
     * @throws IOException - if a user gives wrong query input when defining a detector
     */
    public void executeDetector(
        AnomalyDetector detector,
        Instant startTime,
        Instant endTime,
        ThreadContext.StoredContext context,
        ActionListener<List<AnomalyResult>> listener
    ) throws IOException {
        context.restore();
        List<String> categoryField = detector.getCategoryField();
        if (categoryField != null && !categoryField.isEmpty()) {
            featureManager.getPreviewEntities(detector, startTime.toEpochMilli(), endTime.toEpochMilli(), ActionListener.wrap(entities -> {

                if (entities == null || entities.isEmpty()) {
                    // TODO return exception like IllegalArgumentException to explain data is not enough for preview
                    // This also requires front-end change to handle error message correspondingly
                    // We return empty list for now to avoid breaking front-end
                    listener.onResponse(Collections.emptyList());
                }
                ActionListener<EntityAnomalyResult> entityAnomalyResultListener = ActionListener
                    .wrap(
                        entityAnomalyResult -> { listener.onResponse(entityAnomalyResult.getAnomalyResults()); },
                        e -> onFailure(e, listener, detector.getDetectorId())
                    );
                MultiResponsesDelegateActionListener<EntityAnomalyResult> multiEntitiesResponseListener =
                    new MultiResponsesDelegateActionListener<EntityAnomalyResult>(
                        entityAnomalyResultListener,
                        entities.size(),
                        String.format(Locale.ROOT, "Fail to get preview result for multi entity detector %s", detector.getDetectorId()),
                        true
                    );
                for (Entity entity : entities) {
                    featureManager
                        .getPreviewFeaturesForEntity(
                            detector,
                            entity,
                            startTime.toEpochMilli(),
                            endTime.toEpochMilli(),
                            ActionListener.wrap(features -> {
                                List<ThresholdingResult> entityResults = modelManager.getPreviewResults(features.getProcessedFeatures());
                                List<AnomalyResult> sampledEntityResults = sample(
                                    parsePreviewResult(detector, features, entityResults, Arrays.asList(entity)),
                                    maxPreviewResults
                                );
                                multiEntitiesResponseListener.onResponse(new EntityAnomalyResult(sampledEntityResults));
                            }, e -> multiEntitiesResponseListener.onFailure(e))
                        );
                }
            }, e -> onFailure(e, listener, detector.getDetectorId())));
        } else {
            featureManager.getPreviewFeatures(detector, startTime.toEpochMilli(), endTime.toEpochMilli(), ActionListener.wrap(features -> {
                try {
                    List<ThresholdingResult> results = modelManager.getPreviewResults(features.getProcessedFeatures());
                    listener.onResponse(sample(parsePreviewResult(detector, features, results, null), maxPreviewResults));
                } catch (Exception e) {
                    onFailure(e, listener, detector.getDetectorId());
                }
            }, e -> onFailure(e, listener, detector.getDetectorId())));
        }
    }

    private void onFailure(Exception e, ActionListener<List<AnomalyResult>> listener, String detectorId) {
        logger.info("Fail to preview anomaly detector " + detectorId, e);
        // TODO return exception like IllegalArgumentException to explain data is not enough for preview
        // This also requires front-end change to handle error message correspondingly
        // We return empty list for now to avoid breaking front-end
        if (e instanceof OpenSearchSecurityException) {
            listener.onFailure(e);
        }
        listener.onResponse(Collections.emptyList());
    }

    private List<AnomalyResult> parsePreviewResult(
        AnomalyDetector detector,
        Features features,
        List<ThresholdingResult> results,
        List<Entity> entity
    ) {
        // unprocessedFeatures[][], each row is for one date range.
        // For example, unprocessedFeatures[0][2] is for the first time range, the third feature
        double[][] unprocessedFeatures = features.getUnprocessedFeatures();
        List<Map.Entry<Long, Long>> timeRanges = features.getTimeRanges();
        List<Feature> featureAttributes = detector.getFeatureAttributes().stream().filter(Feature::getEnabled).collect(Collectors.toList());

        List<AnomalyResult> anomalyResults = new ArrayList<>();
        if (timeRanges != null && timeRanges.size() > 0) {
            for (int i = 0; i < timeRanges.size(); i++) {
                Map.Entry<Long, Long> timeRange = timeRanges.get(i);

                List<FeatureData> featureDatas = new ArrayList<>();
                for (int j = 0; j < featureAttributes.size(); j++) {
                    double value = unprocessedFeatures[i][j];
                    Feature feature = featureAttributes.get(j);
                    FeatureData data = new FeatureData(feature.getId(), feature.getName(), value);
                    featureDatas.add(data);
                }

                AnomalyResult result;
                if (results != null && results.size() > i) {
                    ThresholdingResult thresholdingResult = results.get(i);
                    result = new AnomalyResult(
                        detector.getDetectorId(),
                        null,
                        thresholdingResult.getGrade(),
                        thresholdingResult.getConfidence(),
                        featureDatas,
                        Instant.ofEpochMilli(timeRange.getKey()),
                        Instant.ofEpochMilli(timeRange.getValue()),
                        null,
                        null,
                        null,
                        entity,
                        detector.getUser(),
                        CommonValue.NO_SCHEMA_VERSION
                    );
                } else {
                    result = new AnomalyResult(
                        detector.getDetectorId(),
                        null,
                        null,
                        null,
                        featureDatas,
                        Instant.ofEpochMilli(timeRange.getKey()),
                        Instant.ofEpochMilli(timeRange.getValue()),
                        null,
                        null,
                        null,
                        entity,
                        detector.getUser(),
                        CommonValue.NO_SCHEMA_VERSION
                    );
                }

                anomalyResults.add(result);
            }
        }
        return anomalyResults;
    }

    private List<AnomalyResult> sample(List<AnomalyResult> results, int sampleSize) {
        if (results.size() <= sampleSize) {
            return results;
        } else {
            double stepSize = (results.size() - 1.0) / (sampleSize - 1.0);
            List<AnomalyResult> samples = new ArrayList<>(sampleSize);
            for (int i = 0; i < sampleSize; i++) {
                int index = Math.min((int) (stepSize * i), results.size() - 1);
                samples.add(results.get(index));
            }
            return samples;
        }
    }

}
