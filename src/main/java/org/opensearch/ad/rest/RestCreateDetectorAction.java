package org.opensearch.ad.rest;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.PUT;
import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.sdk.ExtensionRestHandler;
import org.opensearch.sdk.ExtensionRestResponse;

public class RestCreateDetectorAction implements ExtensionRestHandler {

    private static final String GREETING = "Hello, %s!";
    private String worldName = "World";

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/hello"), new Route(PUT, "/hello/{name}"));
    }

    @Override
    public ExtensionRestResponse handleRequest(Method method, String uri) {
        // We need to track which parameters are consumed to pass back to OpenSearch
        List<String> consumedParams = new ArrayList<>();
        if (Method.GET.equals(method) && "/hello".equals(uri)) {
            return new ExtensionRestResponse(OK, String.format(GREETING, worldName), consumedParams);
        } else if (Method.PUT.equals(method) && uri.startsWith("/hello/")) {
            // Placeholder code here for parameters in named wildcard paths
            // Full implementation based on params() will be implemented as part of
            // https://github.com/opensearch-project/opensearch-sdk-java/issues/111
            String name = uri.substring("/hello/".length());
            consumedParams.add("name");
            try {
                worldName = URLDecoder.decode(name, StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                return new ExtensionRestResponse(BAD_REQUEST, e.getMessage(), consumedParams);
            }
            return new ExtensionRestResponse(OK, "Updated the world's name to " + worldName, consumedParams);
        }
        return new ExtensionRestResponse(
            NOT_FOUND,
            "Extension REST action improperly configured to handle " + method.name() + " " + uri,
            consumedParams
        );
    }

}
