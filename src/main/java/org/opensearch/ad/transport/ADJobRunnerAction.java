package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;

public class ADJobRunnerAction extends ActionType<ADJobRunnerResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "extension/job_runner";

    public static final ADJobRunnerAction INSTANCE = new ADJobRunnerAction();

    public ADJobRunnerAction() {
        super(NAME, ADJobRunnerResponse::new);
    }
}
