package org.opensearch.ad.rest;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.PUT;
import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.NOT_ACCEPTABLE;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.NOT_MODIFIED;
import static org.opensearch.rest.RestStatus.OK;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.sdk.ExtensionRestHandler;

public class RestCreateDetectorAction implements ExtensionRestHandler {

    private static final String GREETING = "Hello, %s!";
    private String worldName = "World";
    private List<String> worldAdjectives = new ArrayList<>();
    private Random rand = new Random();

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/hello"), new Route(PUT, "/hello/{name}"));
    }

    @Override
    public ExtensionRestResponse handleRequest(ExtensionRestRequest request) {
        Method method = request.method();

        if (Method.GET.equals(method)) {
            return handleGetRequest(request);
        } else if (Method.POST.equals(method)) {
            return handlePostRequest(request);
        } else if (Method.DELETE.equals(method)) {
            return handleDeleteRequest(request);
        } else if (Method.PUT.equals(method)) {
            return handlePutRequest(request);
        }
        return handleBadRequest(request);
    }

    private ExtensionRestResponse handleGetRequest(ExtensionRestRequest request) {
        String worldNameWithRandomAdjective = worldAdjectives.isEmpty()
            ? worldName
            : String.join(" ", worldAdjectives.get(rand.nextInt(worldAdjectives.size())), worldName);
        return new ExtensionRestResponse(request, OK, String.format(GREETING, worldNameWithRandomAdjective));
    }

    private ExtensionRestResponse handlePostRequest(ExtensionRestRequest request) {
        if (request.hasContent()) {
            String adjective = "";
            XContentType contentType = request.getXContentType();
            if (contentType == null) {
                // Plain text
                adjective = request.content().utf8ToString();
            } else if (contentType.equals(XContentType.JSON)) {
                adjective = parseJsonAdjective(request.content().utf8ToString());
            } else {
                return new ExtensionRestResponse(request, NOT_ACCEPTABLE, "Only text and JSON content types are supported");
            }
            if (!adjective.isBlank()) {
                worldAdjectives.add(adjective);
                return new ExtensionRestResponse(request, OK, "Added " + adjective + " to words that describe the world!");
            }
            return new ExtensionRestResponse(request, BAD_REQUEST, "No adjective included with POST request");
        }
        return new ExtensionRestResponse(request, BAD_REQUEST, "No content included with POST request");
    }

    private ExtensionRestResponse handleDeleteRequest(ExtensionRestRequest request) {
        if (request.hasContent()) {
            String adjective = "";
            XContentType contentType = request.getXContentType();
            if (contentType == null) {
                // Plain text
                adjective = request.content().utf8ToString();
            } else if (contentType.equals(XContentType.JSON)) {
                adjective = parseJsonAdjective(request.content().utf8ToString());
            } else {
                return new ExtensionRestResponse(request, NOT_ACCEPTABLE, "Only text and JSON content types are supported");
            }
            if (!adjective.isBlank()) {
                if (worldAdjectives.remove(adjective)) {
                    return new ExtensionRestResponse(request, OK, "Goodbye, " + adjective + " world!");
                }
                return new ExtensionRestResponse(request, NOT_MODIFIED, "");
            }
            return new ExtensionRestResponse(request, BAD_REQUEST, "No adjective included with DELETE request");
        }
        return new ExtensionRestResponse(request, BAD_REQUEST, "No content included with DELETE request");
    }

    private ExtensionRestResponse handlePutRequest(ExtensionRestRequest request) {
        String name = request.param("name");
        try {
            worldName = URLDecoder.decode(name, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return new ExtensionRestResponse(request, BAD_REQUEST, e.getMessage());
        }
        return new ExtensionRestResponse(request, OK, "Updated the world's name to " + worldName);
    }

    private ExtensionRestResponse handleBadRequest(ExtensionRestRequest request) {
        return new ExtensionRestResponse(request, NOT_FOUND, "Extension REST action improperly configured to handle " + request.toString());
    }

    private String parseJsonAdjective(String json) {
        // TODO: Once CreateComponents has an XContentRegistry available we can parse from there
        // For now we just hack our way into the result.
        boolean foundLabel = false;
        boolean foundColon = false;
        for (String s : json.split("\"")) {
            if (!foundLabel) {
                foundLabel = "adjective".equals(s);
            } else if (!foundColon) {
                foundColon = s.contains(":");
            } else {
                // This is the adjective!
                return s;
            }
        }
        return "";
    }
}
