package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;

public class ADJobParameterAction extends ActionType<ADJobParameterResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "extension/job_provider";

    public static final ADJobParameterAction INSTANCE = new ADJobParameterAction();

    public ADJobParameterAction() {
        super(NAME, ADJobParameterResponse::new);
    }
}
