package org.opensearch.ad;

import java.io.IOException;
import java.util.List;

import org.opensearch.ad.rest.RestCreateDetectorAction;
import org.opensearch.sdk.Extension;
import org.opensearch.sdk.ExtensionRestHandler;
import org.opensearch.sdk.ExtensionSettings;
import org.opensearch.sdk.ExtensionsRunner;

public class AnomalyDetectorExtension implements Extension {

    private static final String EXTENSION_SETTINGS_PATH = "/ad-extension.yml";

    private ExtensionSettings settings;

    public AnomalyDetectorExtension() {
        try {
            this.settings = initializeSettings();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public ExtensionSettings getExtensionSettings() {
        return this.settings;
    }

    @Override
    public List<ExtensionRestHandler> getExtensionRestHandlers() {
        return List.of(new RestCreateDetectorAction());
    }

    private static ExtensionSettings initializeSettings() throws IOException {
        ExtensionSettings settings = Extension.readSettingsFromYaml(EXTENSION_SETTINGS_PATH);
        if (settings == null || settings.getHostAddress() == null || settings.getHostPort() == null) {
            throw new IOException("Failed to initialize Extension settings. No port bound.");
        }
        return settings;
    }

    public static void main(String[] args) throws IOException {
        // Execute this extension by instantiating it and passing to ExtensionsRunner
        ExtensionsRunner.run(new AnomalyDetectorExtension());
    }
}
