package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.extensions.action.ExtensionActionResponse;

public class ADJobRunnerAction extends ActionType<ExtensionActionResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "get/job_runner";
    public static final ADJobRunnerAction INSTANCE = new ADJobRunnerAction();

    private ADJobRunnerAction() {
        super(NAME, ExtensionActionResponse::new);
    }

}
