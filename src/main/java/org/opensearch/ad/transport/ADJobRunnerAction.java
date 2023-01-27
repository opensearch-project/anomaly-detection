package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.extensions.action.ExtensionActionResponse;

public class ADJobRunnerAction extends ActionType<ExtensionActionResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "get/job_runner";
    public static final ADJobRunnerAction INSTANCE = new ADJobRunnerAction();

    private ADJobRunnerAction() {
        super(NAME, ExtensionActionResponse::new);
    }

    public ADJobRunnerAction(String name, Writeable.Reader<ExtensionActionResponse> adJobRunnerResponseReader) {
        super(name, adJobRunnerResponseReader);
    }
}
