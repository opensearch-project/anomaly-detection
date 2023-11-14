/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad.tools;

import com.google.gson.Gson;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.model.AnomalyDetector.DESCRIPTION_FIELD;
import static org.opensearch.ad.model.AnomalyDetector.NAME_FIELD;

/**
 * This tool will get all anomaly detectors in the cluster.
 */
@ToolAnnotation(GetAllDetectorsTool.TYPE)
public class GetAllDetectorsTool implements Tool {

    public static final String TYPE = "GetAllDetectorsTool";
    private String name;
    private static String DEFAULT_DESCRIPTION = "Use this tool to search anomaly detectors.";
    private String description = DEFAULT_DESCRIPTION;
    private Client client;
    private static Gson gson = new Gson();
    private static int MAX_SIZE = 1000;

    private GetAllDetectorsTool() {}

    public static class Builder {
        private Client client = null;

        public Builder() {}

        public Builder client(Client client) {
            this.client = client;
            return this;
        }

        public GetAllDetectorsTool build() {
            GetAllDetectorsTool tool = new GetAllDetectorsTool();
            tool.client = this.client;
            return tool;
        }
    }

    @Override
    public <T> T run(Map<String, String> parameters) {
        return Tool.super.run(parameters);
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder()).size(MAX_SIZE).fetchSource(new String[]{NAME_FIELD, DESCRIPTION_FIELD}, null);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(ANOMALY_DETECTORS_INDEX);

        client.execute(SearchAnomalyDetectorAction.INSTANCE, searchRequest, ActionListener.wrap(r-> {
            SearchHits hits = r.getHits();
            if (hits.getTotalHits().value > 0) {
                List<Map<String, Object>> detectors = new ArrayList<>();
                for(SearchHit hit : hits.getHits()) {
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    detectors.add(sourceAsMap);
                }

                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    listener.onResponse((T) ("These are all anomaly detectors in this cluster:\n" + gson.toJson(detectors)));
                    return null;
                });
            } else {
                listener.onResponse((T)"no anomaly detectors found");
            }
        }, e-> {
            listener.onFailure(e);
        }));
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getVersion() {
        return "1.0";
    }

    @Override
    public void setInputParser(Parser parser) {
        Tool.super.setInputParser(parser);
    }

    @Override
    public void setOutputParser(Parser parser) {
        Tool.super.setOutputParser(parser);
    }


    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return true;
    }

    @Override
    public boolean end(String input, Map<String, String> toolParameters) {
        return Tool.super.end(input, toolParameters);
    }

    public static class Factory implements Tool.Factory<GetAllDetectorsTool> {
        private Client client;
        private static Factory INSTANCE;
        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (GetAllDetectorsTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public GetAllDetectorsTool create(Map<String, Object> params) {
            return new Builder()
                    .client(client)
                    .build();
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }
}
