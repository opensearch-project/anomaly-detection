package org.opensearch.ad.rest;

import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestStatus.OK;
import static org.opensearch.rest.RestStatus.BAD_REQUEST;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.common.xcontent.*;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;

public class RestGetDetectorAction extends BaseExtensionRestHandler {
    private final Logger logger = LogManager.getLogger(RestGetDetectorAction.class);
    private final OpenSearchClient sdkClient;
    private final NamedXContentRegistry xContentRegistry;

    public RestGetDetectorAction(ExtensionsRunner runner, AnomalyDetectorExtension extension) {
        this.xContentRegistry = runner.getNamedXContentRegistry().getRegistry();
        this.sdkClient = extension.getClient();
    }

    @Override
    protected List<RouteHandler> routeHandlers() {
        return List.of(new RouteHandler(GET, "/detectors/{detectorId}", (r) -> handleGetRequest(r)));
    }

    public ExtensionRestResponse handleGetRequest(ExtensionRestRequest request) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param("detectorId");
        // TODO
        // 1. Get Detector id
        // 2. Find a way to query SearchRequest
        // 3. Else, match detectorId with the SearchRequest response
        // 4. Follow the steps on the issue
        // 5. Look for MultiGetRequest

        SearchResponse<AnomalyDetector> searchResponse = null;
        try {
            searchResponse = sdkClient.search(s -> s.index(ANOMALY_DETECTORS_INDEX), AnomalyDetector.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        XContentBuilder builder = null;
        try {
            for (int i = 0; i < searchResponse.hits().hits().size(); i++) {
                logger.info("Search query response: {}", searchResponse.hits().hits().size());
                if (searchResponse.hits().hits().get(i).id().equals(detectorId)) {
                    builder = XContentBuilder.builder(XContentType.JSON.xContent());
                    builder.startObject();
                    builder.field("id", searchResponse.hits().hits().get(i).id());
                    builder.field("version", searchResponse.hits().hits().get(i).version());
                    builder.field("seqNo", searchResponse.hits().hits().get(i).seqNo());
                    builder.field("primaryTerm", searchResponse.hits().hits().get(i).primaryTerm());
                    builder.field("detector", searchResponse.hits().hits().get(i).source());
                    builder.field("status", RestStatus.CREATED);
                    builder.endObject();
                }
            }
        } catch (Exception e) {
            return new ExtensionRestResponse(request, BAD_REQUEST, builder);
        }
        return new ExtensionRestResponse(request, OK, builder);
    }
}
