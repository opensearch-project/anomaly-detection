package org.opensearch.ad.rest;

import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
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
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.sdk.ExtensionRestHandler;

public class RestGetDetectorAction implements ExtensionRestHandler {
    private final Logger logger = LogManager.getLogger(RestGetDetectorAction.class);
    private AnomalyDetectorExtension anomalyDetectorExtension = new AnomalyDetectorExtension();
    private OpenSearchClient sdkClient = anomalyDetectorExtension.getClient();

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/detectors"));
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

//        String detectorId = request.param("detectorId");


        // Search without query
        SearchResponse<AnomalyDetector> searchResponse = null;
        try {
            searchResponse = sdkClient.search(s -> s.index(ANOMALY_DETECTORS_INDEX), AnomalyDetector.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int i = 0; i< searchResponse.hits().hits().size(); i++) {
            logger.info("Search query response: {}", searchResponse.hits().hits().get(i).source());
            logger.info("Documents: {}", searchResponse.documents());
            logger.info("2: {}", searchResponse.hits().hits().get(i).id());
        }


        // do things with request
        return new ExtensionRestResponse(request, OK, "placeholder");
    }

}
