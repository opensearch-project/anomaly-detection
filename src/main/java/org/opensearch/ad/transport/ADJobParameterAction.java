package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.extensions.action.ExtensionActionResponse;

public class ADJobParameterAction extends ActionType<ExtensionActionResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "get/job_parameter";
    public static final ADJobParameterAction INSTANCE = new ADJobParameterAction();

    private ADJobParameterAction() {
        super(NAME, ExtensionActionResponse::new);
    }

}
