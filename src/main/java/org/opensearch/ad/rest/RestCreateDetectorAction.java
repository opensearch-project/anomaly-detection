package org.opensearch.ad.rest;

import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.ANOMALY_DETECTORS_INDEX_MAPPING_FILE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionRestHandler;
import org.opensearch.search.aggregations.BaseAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import jakarta.json.stream.JsonParser;

public class RestCreateDetectorAction implements ExtensionRestHandler {
    private final Logger logger = LogManager.getLogger(RestCreateDetectorAction.class);
    private AnomalyDetectorExtension anomalyDetectorExtension = new AnomalyDetectorExtension();
    private OpenSearchClient sdkClient = anomalyDetectorExtension.getClient();

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/detectors"));
    }

    private String getAnomalyDetectorMappings() throws IOException {
        URL url = AnomalyDetectionIndices.class.getClassLoader().getResource(ANOMALY_DETECTORS_INDEX_MAPPING_FILE);
        return Resources.toString(url, Charsets.UTF_8);
    }

    private IndexResponse indexAnomalyDetector(AnomalyDetector anomalyDetector) throws IOException {
        AnomalyDetector detector = new AnomalyDetector(
            anomalyDetector.getName(),
            anomalyDetector.getVersion(),
            anomalyDetector.getName(),
            anomalyDetector.getDescription(),
            anomalyDetector.getTimeField(),
            anomalyDetector.getIndices(),
            anomalyDetector.getFeatureAttributes(),
            anomalyDetector.getFilterQuery(),
            anomalyDetector.getDetectionInterval(),
            anomalyDetector.getWindowDelay(),
            anomalyDetector.getShingleSize(),
            anomalyDetector.getUiMetadata(),
            anomalyDetector.getSchemaVersion(),
            Instant.now(),
            anomalyDetector.getCategoryField(),
            null,
            anomalyDetector.getResultIndex()
        );

        IndexRequest<AnomalyDetector> indexRequest = new IndexRequest.Builder<AnomalyDetector>()
            .index(ANOMALY_DETECTORS_INDEX)
            .document(detector)
            .build();
        IndexResponse indexResponse = sdkClient.index(indexRequest);
        return indexResponse;

    }

    public List<NamedXContentRegistry.Entry> getNamedXWriteables() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(AnomalyDetector.XCONTENT_REGISTRY);
        entries.add(AnomalyResult.XCONTENT_REGISTRY);
        entries.add(DetectorInternalState.XCONTENT_REGISTRY);
        entries
            .add(
                registerQuery(
                    new SearchPlugin.QuerySpec<>(NestedQueryBuilder.NAME, NestedQueryBuilder::new, NestedQueryBuilder::fromXContent)
                )
            );
        entries
            .add(registerQuery(new SearchPlugin.QuerySpec<>(BoolQueryBuilder.NAME, BoolQueryBuilder::new, BoolQueryBuilder::fromXContent)));
        entries
            .add(
                registerAggregation(
                    new SearchPlugin.AggregationSpec(SumAggregationBuilder.NAME, SumAggregationBuilder::new, SumAggregationBuilder.PARSER)
                        .addResultReader(InternalSum::new)
                )
            );

        entries
            .add(
                registerAggregation(
                    new SearchPlugin.AggregationSpec(
                        FilterAggregationBuilder.NAME,
                        FilterAggregationBuilder::new,
                        FilterAggregationBuilder::parse
                    ).addResultReader(InternalFilter::new)
                )
            );
        entries
            .add(
                registerQuery(new SearchPlugin.QuerySpec<>(RangeQueryBuilder.NAME, RangeQueryBuilder::new, RangeQueryBuilder::fromXContent))
            );
        return entries;

    }

    private NamedXContentRegistry.Entry registerQuery(SearchPlugin.QuerySpec<?> spec) {
        return new NamedXContentRegistry.Entry(QueryBuilder.class, spec.getName(), (p, c) -> spec.getParser().fromXContent(p));
    }

    private NamedXContentRegistry.Entry registerAggregation(SearchPlugin.AggregationSpec spec) {
        return new NamedXContentRegistry.Entry(BaseAggregationBuilder.class, spec.getName(), (p, c) -> {
            String name = (String) c;
            return spec.getParser().parse(p, name);
        });
    }

    private CreateIndexRequest initAnomalyDetectorIndex() {
        JsonpMapper mapper = sdkClient._transport().jsonpMapper();
        JsonParser parser = null;
        try {
            parser = mapper
                .jsonProvider()
                .createParser(new ByteArrayInputStream(getAnomalyDetectorMappings().getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        CreateIndexRequest request = null;
        try {
            request = new CreateIndexRequest.Builder()
                .index(ANOMALY_DETECTORS_INDEX)
                .mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper))
                .build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return request;
    }

    @Override
    public ExtensionRestResponse handleRequest(ExtensionRestRequest request) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        Method method = request.method();

        if (!Method.POST.equals(method)) {
            return new ExtensionRestResponse(
                request,
                NOT_FOUND,
                "Extension REST action improperly configured to handle " + request.toString()
            );
        }

        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(getNamedXWriteables());
        XContentParser parser;
        AnomalyDetector detector;
        XContentBuilder builder = null;
        CreateIndexRequest createIndexRequest;
        try {
            parser = request.contentParser(xContentRegistry);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            detector = AnomalyDetector.parse(parser);
            createIndexRequest = initAnomalyDetectorIndex();
            CreateIndexResponse createIndexResponse = sdkClient.indices().create(createIndexRequest);
            if (createIndexResponse.acknowledged()) {
                IndexResponse indexResponse = indexAnomalyDetector(detector);
                try {
                    builder = XContentBuilder.builder(XContentType.JSON.xContent());
                    builder.startObject();
                    builder.field("id", indexResponse.id());
                    builder.field("version", indexResponse.version());
                    builder.field("seqNo", indexResponse.seqNo());
                    builder.field("primaryTerm", indexResponse.primaryTerm());
                    builder.field("detector", detector);
                    builder.field("status", RestStatus.CREATED);
                    builder.endObject();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            return new ExtensionRestResponse(request, BAD_REQUEST, builder);
        }
        return new ExtensionRestResponse(request, OK, builder);
    }

}
