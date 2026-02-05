/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.opensearch.ad.correlation.Anomaly;
import org.opensearch.ad.correlation.AnomalyCorrelation;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorMetadata;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Transforms correlation output into structured insights-results documents.
 */
public class InsightsGenerator {

    /**
     * Generate insights document from AnomalyCorrelation clusters.
     *
     * @param clusters Correlation clusters
     * @param anomalyResultByAnomaly Map of correlation anomaly to raw anomaly result
     * @param detectorMetadataMap Detector metadata for name/index enrichment
     * @param executionStartTime Start of analysis window
     * @param executionEndTime End of analysis window
     * @return Optional empty if clusters is null/empty (or produces no valid cluster docs); otherwise a builder ready to index
     */
    public static Optional<XContentBuilder> generateInsightsFromClusters(
        List<AnomalyCorrelation.Cluster> clusters,
        Map<Anomaly, AnomalyResult> anomalyResultByAnomaly,
        Map<String, DetectorMetadata> detectorMetadataMap,
        Instant executionStartTime,
        Instant executionEndTime
    ) throws IOException {
        if (clusters == null || clusters.isEmpty()) {
            return Optional.empty();
        }
        List<AnomalyCorrelation.Cluster> safeClusters = clusters;
        Map<Anomaly, AnomalyResult> safeAnomalyMap = anomalyResultByAnomaly == null ? new HashMap<>() : anomalyResultByAnomaly;
        Map<String, DetectorMetadata> safeDetectorMetadata = detectorMetadataMap == null ? new HashMap<>() : detectorMetadataMap;

        Set<String> docDetectorIds = new HashSet<>();
        Set<String> docDetectorNames = new HashSet<>();
        Set<String> docIndices = new HashSet<>();
        Set<String> docModelIds = new HashSet<>();
        int totalAnomalies = 0;

        List<Map<String, Object>> clusterDocs = new ArrayList<>();
        for (AnomalyCorrelation.Cluster cluster : safeClusters) {
            if (cluster == null) {
                continue;
            }
            List<Anomaly> clusterAnomalies = cluster.getAnomalies();
            if (clusterAnomalies == null || clusterAnomalies.isEmpty()) {
                continue;
            }

            Set<String> clusterDetectorIds = new HashSet<>();
            Set<String> clusterDetectorNames = new HashSet<>();
            Set<String> clusterIndices = new HashSet<>();
            Set<String> clusterEntities = new HashSet<>();
            Set<String> clusterModelIds = new HashSet<>();
            List<Map<String, Object>> anomalyDocs = new ArrayList<>();

            for (Anomaly anomaly : clusterAnomalies) {
                if (anomaly == null) {
                    continue;
                }
                totalAnomalies++;

                String detectorId = anomaly.getConfigId();
                String modelId = anomaly.getModelId();

                if (detectorId != null) {
                    clusterDetectorIds.add(detectorId);
                    docDetectorIds.add(detectorId);
                }
                if (modelId != null) {
                    clusterModelIds.add(modelId);
                    docModelIds.add(modelId);
                }

                DetectorMetadata metadata = detectorId != null ? safeDetectorMetadata.get(detectorId) : null;
                if (metadata != null) {
                    if (metadata.getDetectorName() != null) {
                        clusterDetectorNames.add(metadata.getDetectorName());
                        docDetectorNames.add(metadata.getDetectorName());
                    }
                    if (metadata.getIndices() != null) {
                        clusterIndices.addAll(metadata.getIndices());
                        docIndices.addAll(metadata.getIndices());
                    }
                }

                AnomalyResult rawAnomaly = safeAnomalyMap.get(anomaly);
                String entityKey = buildEntityKey(rawAnomaly);
                if (entityKey != null) {
                    clusterEntities.add(entityKey);
                }

                Map<String, Object> anomalyDoc = new HashMap<>();
                anomalyDoc.put("model_id", modelId);
                anomalyDoc.put("detector_id", detectorId);
                anomalyDoc.put("data_start_time", anomaly.getDataStartTime().toEpochMilli());
                anomalyDoc.put("data_end_time", anomaly.getDataEndTime().toEpochMilli());
                anomalyDocs.add(anomalyDoc);
            }

            Map<String, Object> clusterDoc = new HashMap<>();
            clusterDoc.put("event_start", cluster.getEventWindow().getStart().toEpochMilli());
            clusterDoc.put("event_end", cluster.getEventWindow().getEnd().toEpochMilli());
            clusterDoc
                .put(
                    "cluster_text",
                    generateClusterText(
                        clusterDetectorIds,
                        clusterIndices,
                        clusterEntities,
                        cluster.getEventWindow().getStart(),
                        cluster.getEventWindow().getEnd(),
                        clusterAnomalies.size()
                    )
                );
            clusterDoc.put("detector_ids", new ArrayList<>(clusterDetectorIds));
            clusterDoc.put("detector_names", new ArrayList<>(clusterDetectorNames));
            clusterDoc.put("indices", new ArrayList<>(clusterIndices));
            clusterDoc.put("entities", new ArrayList<>(clusterEntities));
            clusterDoc.put("model_ids", new ArrayList<>(clusterModelIds));
            clusterDoc.put("num_anomalies", clusterAnomalies.size());
            clusterDoc.put("anomalies", anomalyDocs);

            clusterDocs.add(clusterDoc);
        }

        if (clusterDocs.isEmpty()) {
            // No valid clusters (e.g., null clusters or clusters with no anomalies)
            return Optional.empty();
        }

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        builder.field("window_start", executionStartTime.toEpochMilli());
        builder.field("window_end", executionEndTime.toEpochMilli());
        builder.field("generated_at", Instant.now().toEpochMilli());

        builder.field("doc_detector_names", new ArrayList<>(docDetectorNames));
        builder.field("doc_detector_ids", new ArrayList<>(docDetectorIds));
        builder.field("doc_indices", new ArrayList<>(docIndices));
        builder.field("doc_model_ids", new ArrayList<>(docModelIds));

        builder.startArray("clusters");
        for (Map<String, Object> clusterDoc : clusterDocs) {
            builder.startObject();
            for (Map.Entry<String, Object> entry : clusterDoc.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        builder.endArray();

        builder.startObject("stats");
        builder.field("num_clusters", clusterDocs.size());
        builder.field("num_anomalies", totalAnomalies);
        builder.field("num_detectors", docDetectorIds.size());
        builder.field("num_indices", docIndices.size());
        builder.field("num_series", docModelIds.size());
        builder.endObject();

        builder.endObject();

        return Optional.of(builder);
    }

    // Legacy ML-commons correlation generator removed.

    private static String buildEntityKey(AnomalyResult anomaly) {
        if (anomaly == null || anomaly.getEntity() == null || !anomaly.getEntity().isPresent()) {
            return null;
        }
        org.opensearch.timeseries.model.Entity entity = anomaly.getEntity().get();
        if (entity.getAttributes() == null || entity.getAttributes().isEmpty()) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        for (Map.Entry<String, String> entry : entity.getAttributes().entrySet()) {
            parts.add(entry.getKey() + "=" + entry.getValue());
        }
        parts.sort(String::compareTo);
        return String.join(",", parts);
    }

    private static String generateClusterText(
        Set<String> detectorIds,
        Set<String> indices,
        Set<String> entities,
        Instant eventStart,
        Instant eventEnd,
        int numAnomalies
    ) {
        StringBuilder text = new StringBuilder();

        DateTimeFormatter friendlyFormatter = DateTimeFormatter.ofPattern("MMM d, yyyy HH:mm z", Locale.ROOT).withZone(ZoneOffset.UTC);
        String startStr = friendlyFormatter.format(eventStart);
        String endStr = friendlyFormatter.format(eventEnd);

        text.append(String.format(Locale.ROOT, "Correlated anomalies detected across %d detector(s)", detectorIds.size()));

        if (!entities.isEmpty()) {
            text.append(String.format(Locale.ROOT, ", affecting %d entities", entities.size()));
        }

        text.append(".");
        text.append(String.format(Locale.ROOT, " Detected from %s to %s with %d anomaly record(s).", startStr, endStr, numAnomalies));

        return text.toString();
    }
}
