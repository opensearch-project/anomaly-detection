package org.opensearch.ad.rest;

import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.indices.GetIndexRequest;
import org.opensearch.client.opensearch.indices.GetIndexResponse;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionRestHandler;

public class RestGetDetectorAction implements ExtensionRestHandler {
    private final Logger logger = LogManager.getLogger(RestGetDetectorAction.class);
    private AnomalyDetectorExtension anomalyDetectorExtension = new AnomalyDetectorExtension();
    private OpenSearchClient sdkClient = anomalyDetectorExtension.getClient();

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/detectors/{detectorId}"));
    }

    @Override
    public ExtensionRestResponse handleRequest(ExtensionRestRequest request) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        Method method = request.method();

        if (!Method.GET.equals(method)) {
            return new ExtensionRestResponse(
                request,
                NOT_FOUND,
                "Extension REST action improperly configured to handle " + request.toString()
            );
        }

        String detectorId = request.param("detectorId");
        logger.info("DETECTOR : {}", detectorId);
        // TODO
        // 1. Get Detector id
        // 2. Find a way to query SearchRequest
        // 3. Else, match detectorId with the SearchRequest response
        // 4. Follow the steps on the issue
        // 5. Look for MultiGetRequest

        // Search without query

//        Query query = Query.of(qb -> qb.match(q -> q.field("name").query(t -> t.stringValue("test-detector"))));
//        logger.info("Query: {}", query);
//        final SearchRequest.Builder searchRequest = new SearchRequest.Builder()
//            .allowPartialSearchResults(false)
//            .index(ANOMALY_DETECTORS_INDEX);
//            .query(query);

        SearchResponse<AnomalyDetector> searchResponse = null;
         try {
            searchResponse = sdkClient.search(s -> s.index(ANOMALY_DETECTORS_INDEX), AnomalyDetector.class);
         } catch (Exception e) {
            e.printStackTrace();
         }

//         GetIndexRequest getIndexRequest = new GetIndexRequest.Builder().index(ANOMALY_DETECTORS_INDEX).build();
//        GetIndexResponse getIndexResponse = null;
//        try {
//             getIndexResponse = sdkClient.indices().get(getIndexRequest);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        logger.info("GetIndexResponse : {}", getIndexResponse.result().);

//        try {
//            searchResponse = sdkClient.search(searchRequest.build(), AnomalyDetector.class);
//            logger.info("SearchResponse: {}", searchResponse.hits().hits().size());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(getNamedXWriteables());
        XContentParser parser;
        AnomalyDetector detector;
        XContentBuilder builder = null;
        try {
            parser = request.contentParser(xContentRegistry);
//            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            detector = AnomalyDetector.parse(parser);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < searchResponse.hits().hits().size(); i++) {
            logger.info("Search query response: {}", searchResponse.hits().hits().get(i).source());
            logger.info("Documents: {}", searchResponse.documents());
            if (searchResponse.hits().hits().get(i).id() == detectorId) {
                builder = XContentBuilder.builder(XContentType.JSON.xContent());
                builder.startObject();
                builder.field("id", detectorId);
                builder.field("version", searchResponse.hits().hits().get(i).version());
                builder.field("seqNo", searchResponse.hits().hits().get(i).seqNo());
                builder.field("primaryTerm", searchResponse.hits().hits().get(i).primaryTerm());
                builder.field("detector", detector);
                builder.field("status", RestStatus.CREATED);
            }
            logger.info("2: {}", searchResponse.hits().hits().get(i).id());
            logger.info("2: {}", searchResponse.hits().hits().get(i).version());
        }

        // do things with request
        return new ExtensionRestResponse(request, OK, "placeholder");
    }

}
