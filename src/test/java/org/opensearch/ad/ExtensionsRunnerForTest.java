package org.opensearch.ad;

import java.io.IOException;

import org.opensearch.sdk.BaseExtension;
import org.opensearch.sdk.ExtensionSettings;
import org.opensearch.sdk.ExtensionsRunner;

public class ExtensionsRunnerForTest extends ExtensionsRunner {

    public static final String NODE_NAME = "sample-extension";
    public static final String NODE_HOST = "127.0.0.1";
    public static final String NODE_PORT = "4532";

    /**
     * Instantiates a new Extensions Runner using test settings.
     *
     * @throws IOException if the runner failed to read settings or API.
     */
    public ExtensionsRunnerForTest() throws IOException {
        super(new BaseExtension(new ExtensionSettings(NODE_NAME, NODE_HOST, NODE_PORT, "127.0.0.1", "9200")) {
        });
    }
}
