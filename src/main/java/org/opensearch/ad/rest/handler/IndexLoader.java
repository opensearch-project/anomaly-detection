package org.opensearch.ad.rest.handler;

import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.ad.model.AnomalyDetector.*;

/**
 * Serializer for detector with Map<String, Object> type and XContent
 */

public class IndexLoader {
    public Map<String, Object> document(AnomalyDetector anomalyDetector) throws IOException {
        Map<String, Object> builder = new HashMap<>();
        builder.put(NAME_FIELD, anomalyDetector.getName());
        builder.put(DESCRIPTION_FIELD, anomalyDetector.getDescription());
        builder.put(TIMEFIELD_FIELD, anomalyDetector.getTimeField());
        builder.put(INDICES_FIELD, anomalyDetector.getIndices());
        builder.put(FILTER_QUERY_FIELD, anomalyDetector.getFilterQuery());
        builder.put(DETECTION_INTERVAL_FIELD, anomalyDetector.getDetectionInterval());
        builder.put(WINDOW_DELAY_FIELD, anomalyDetector.getWindowDelay());
        builder.put(SHINGLE_SIZE_FIELD, anomalyDetector.getShingleSize());
        builder.put(CommonName.SCHEMA_VERSION_FIELD, anomalyDetector.getSchemaVersion());
        builder.put(FEATURE_ATTRIBUTES_FIELD,anomalyDetector.getFeatureAttributes());

        if (anomalyDetector.getUiMetadata() != null && !anomalyDetector.getUiMetadata().isEmpty()) {
            builder.put(UI_METADATA_FIELD, anomalyDetector.getUiMetadata());
        }
        if (anomalyDetector.getLastUpdateTime() != null) {
            builder.put(LAST_UPDATE_TIME_FIELD, anomalyDetector.getLastUpdateTime().toEpochMilli());
        }
        if (anomalyDetector.getCategoryField() != null) {
            builder.put(CATEGORY_FIELD, anomalyDetector.getCategoryField());
        }
        if (anomalyDetector.getUser() != null) {
            builder.put(USER_FIELD, anomalyDetector.getUser() );
        }
        if (anomalyDetector.getDetectorType() != null) {
            builder.put(DETECTOR_TYPE_FIELD, anomalyDetector.getDetectorType());
        }
        if (anomalyDetector.getDetectionDateRange() != null) {
            builder.put(DETECTION_DATE_RANGE_FIELD, anomalyDetector.getDetectionDateRange());
        }
        if (anomalyDetector.getResultIndex() != null) {
            builder.put(RESULT_INDEX_FIELD, anomalyDetector.getResultIndex());
        }

        return builder;

    }

    public XContentBuilder toXContent(XContentBuilder builder, AnomalyDetector anomalyDetector) throws IOException {
        XContentBuilder xContentBuilder = builder
                .startObject()
                .field(NAME_FIELD,  anomalyDetector.getName())
                .field(DESCRIPTION_FIELD, anomalyDetector.getDescription())
                .field(TIMEFIELD_FIELD, anomalyDetector.getTimeField())
                .field(INDICES_FIELD, anomalyDetector.getIndices())
                .field(FILTER_QUERY_FIELD, anomalyDetector.getFilterQuery())
                .field(DETECTION_INTERVAL_FIELD, anomalyDetector.getDetectionInterval())
                .field(WINDOW_DELAY_FIELD, anomalyDetector.getWindowDelay())
                .field(SHINGLE_SIZE_FIELD, anomalyDetector.getShingleSize())
                .field(CommonName.SCHEMA_VERSION_FIELD, anomalyDetector.getSchemaVersion())
                .field(FEATURE_ATTRIBUTES_FIELD, anomalyDetector.getFeatureAttributes());


        if (anomalyDetector.getUiMetadata() != null && !anomalyDetector.getUiMetadata().isEmpty()) {
            xContentBuilder.field(UI_METADATA_FIELD, anomalyDetector.getUiMetadata());
        }
        if (anomalyDetector.getLastUpdateTime() != null) {
            xContentBuilder.field(LAST_UPDATE_TIME_FIELD, anomalyDetector.getLastUpdateTime().toEpochMilli());
        }
        if (anomalyDetector.getCategoryField() != null) {
            xContentBuilder.field(CATEGORY_FIELD, anomalyDetector.getCategoryField());
        }
        if (anomalyDetector.getUser() != null) {
            xContentBuilder.field(USER_FIELD, anomalyDetector.getUser() );
        }
        if (anomalyDetector.getDetectorType() != null) {
            xContentBuilder.field(DETECTOR_TYPE_FIELD, anomalyDetector.getDetectorType());
        }
        if (anomalyDetector.getDetectionDateRange() != null) {
            xContentBuilder.field(DETECTION_DATE_RANGE_FIELD, anomalyDetector.getDetectionDateRange());
        }
        if (anomalyDetector.getResultIndex() != null) {
            xContentBuilder.field(RESULT_INDEX_FIELD, anomalyDetector.getResultIndex());
        }
        xContentBuilder.endObject();
        return xContentBuilder;
    }

    public void fromXContent() {

    }
}
