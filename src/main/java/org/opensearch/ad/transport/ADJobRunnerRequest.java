package org.opensearch.ad.transport;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;

public class ADJobRunnerRequest extends ActionRequest {
    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
