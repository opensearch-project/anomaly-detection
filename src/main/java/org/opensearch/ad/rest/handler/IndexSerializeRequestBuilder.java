package org.opensearch.ad.rest.handler;

import jakarta.json.stream.JsonGenerator;
import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.TimeConfiguration;
import org.opensearch.client.json.*;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.util.ObjectBuilder;
import org.opensearch.client.util.ObjectBuilderBase;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Builder class for IndexRequest
 */
public class IndexSerializeRequestBuilder extends IndexRequest.Builder implements JsonpSerializable {

    private String detectorString;
    private AnomalyDetector anomalyDetector;

    @Nonnull
    public final AnomalyDetector anomalyDetector() {
        return this.anomalyDetector;
    }

    @Nonnull
    public final String getDetectorString() {
        return this.detectorString;
    }

    private IndexSerializeRequestBuilder() {}


    public IndexSerializeRequestBuilder(Builder builder) {
        this.detectorString = builder.detectorString;
        this.anomalyDetector = builder.anomalyDetector;
    }

    @Override
    public void serialize(JsonGenerator builder, JsonpMapper mapper) {
        builder.writeStartObject();
        builder.write("detector", this.detectorString);
        builder.writeEnd();
    }

    public static class Builder extends ObjectBuilderBase implements ObjectBuilder<IndexSerializeRequestBuilder> {
        private String detectorString;
        private AnomalyDetector anomalyDetector;

        public final Builder detectorString(@Nonnull String v) {
            this.detectorString = v;
            return this;
        }

        public final Builder detector(@Nonnull AnomalyDetector anomalyDetector) {
            this.anomalyDetector = anomalyDetector;
            return this;
        }

        @Override
        public IndexSerializeRequestBuilder build() {
            _checkSingleUse();
            return new IndexSerializeRequestBuilder(this);
        }


    }
//
//    XContentParser parser =
//            XContentType.JSON.xContent().createParser(xContentRegistry,
//                    LoggingDeprecationHandler.INSTANCE,
//                    sourceJson);
//
//    public static final JsonpDeserializer<IndexSerializeRequestBuilder> _DESERIALIZER = ObjectBuilderDeserializer
//            .lazy(IndexSerializeRequestBuilder.Builder::new, IndexSerializeRequestBuilder::setupCreateIndexRequestDeserializer);
//
//    protected static void setupCreateIndexRequestDeserializer(ObjectDeserializer<IndexSerializeRequestBuilder.Builder> op) {
//        op.add(IndexSerializeRequestBuilder.Builder::detectorString, String._DESERIALIZER, "detectorString");
//
//        ObjectBuilderParser
//    }

}
