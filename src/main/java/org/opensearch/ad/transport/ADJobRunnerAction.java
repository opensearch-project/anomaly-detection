package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.common.io.stream.Writeable;

public class ADJobRunnerAction extends ActionType<ADJobRunnerResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "get/job_runner";
    public static final ADJobRunnerAction INSTANCE = new ADJobRunnerAction();

    private ADJobRunnerAction() {
        super(NAME, ADJobRunnerResponse::new);
    }

    public ADJobRunnerAction(String name, Writeable.Reader<ADJobRunnerResponse> adJobRunnerResponseReader) {
        super(name, adJobRunnerResponseReader);
    }
}
