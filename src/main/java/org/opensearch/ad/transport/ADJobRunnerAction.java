package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.extensions.action.ExtensionActionResponse;

public class ADJobRunnerAction extends ActionType<ExtensionActionResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + RestHandlerUtils.EXTENSION_JOB_RUNNER_ACTION_NAME;
    public static final ADJobRunnerAction INSTANCE = new ADJobRunnerAction();

    private ADJobRunnerAction() {
        super(NAME, ExtensionActionResponse::new);
    }

}
