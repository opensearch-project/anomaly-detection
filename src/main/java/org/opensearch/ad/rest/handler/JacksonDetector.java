package org.opensearch.ad.rest.handler;

import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;

import static org.opensearch.ad.model.AnomalyDetector.*;

/**
 * Serializer for detector with String type
 */

public class JacksonDetector {
    String detectorString;
    AnomalyDetector anomalyDetector;

    public JacksonDetector(AnomalyDetector anomalyDetector, String detectorString) {
        this.anomalyDetector = anomalyDetector;
        this.detectorString = detectorString;
    }

    JacksonDetector() {}

    public XContentBuilder toXContent() {
        XContentBuilder sources = null;
        try {
            sources = XContentBuilder.builder(XContentType.JSON.xContent()).startObject()
                    .field(NAME_FIELD, anomalyDetector.getName())
                    .field(DESCRIPTION_FIELD, anomalyDetector.getDescription())
                    .field(TIMEFIELD_FIELD, anomalyDetector.getTimeField())
                    .field(INDICES_FIELD, anomalyDetector.getIndices())
                    .field(FILTER_QUERY_FIELD, anomalyDetector.getFilterQuery())
                    .field(DETECTION_INTERVAL_FIELD, anomalyDetector.getDetectionInterval())
                    .field(WINDOW_DELAY_FIELD, anomalyDetector.getWindowDelay())
                    .field(SHINGLE_SIZE_FIELD, anomalyDetector.getShingleSize())
                    .field(CommonName.SCHEMA_VERSION_FIELD, anomalyDetector.getSchemaVersion())
                    .field(FEATURE_ATTRIBUTES_FIELD,anomalyDetector.getFeatureAttributes())
                    .endObject();

        } catch(Exception e) {
            e.printStackTrace();;
        }

        try{
            return  anomalyDetector.toXContent(sources);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;

    }
}
