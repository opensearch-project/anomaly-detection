package org.opensearch.ad.rest.handler;

import org.opensearch.client.json.JsonpDeserializable;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.ObjectBuilderDeserializer;
import org.opensearch.client.json.ObjectDeserializer;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.util.ObjectBuilder;
import org.opensearch.client.util.ObjectBuilderBase;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;

/**
 * Builder class for CreateIndexRequest
 */
@JsonpDeserializable
public class CreateIndexRequestBuilder extends CreateIndexRequest.Builder{

    private CreateIndexRequestBuilder(Builder builder) {
        mappings(builder.mappings);
        settings(builder.settings);
    }


    public static class Builder extends ObjectBuilderBase implements ObjectBuilder<CreateIndexRequestBuilder> {

        @Nullable
        private TypeMapping mappings;
        @Nullable
        private IndexSettings settings;

        public final Builder mappings(@Nullable TypeMapping value) {
            this.mappings = value;
            return this;
        }

        public final Builder settings(@Nullable IndexSettings value) {
            this.settings = value;
            return this;
        }

        public CreateIndexRequestBuilder build() {
            _checkSingleUse();
            return new CreateIndexRequestBuilder(this);
        }
    }

    /**
     * Json deserializer for {@link CreateIndexRequestBuilder}
     */
    public static final JsonpDeserializer<CreateIndexRequestBuilder> _DESERIALIZER = ObjectBuilderDeserializer
            .lazy(Builder::new, CreateIndexRequestBuilder::setupCreateIndexRequestDeserializer);

    protected static void setupCreateIndexRequestDeserializer(ObjectDeserializer<Builder> op) {
        op.add(Builder::mappings, TypeMapping._DESERIALIZER, "mappings");
        op.add(Builder::settings, IndexSettings._DESERIALIZER, "settings");
    }



}
