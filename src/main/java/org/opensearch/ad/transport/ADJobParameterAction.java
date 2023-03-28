package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.jobscheduler.transport.response.JobParameterResponse;

public class ADJobParameterAction extends ActionType<JobParameterResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + RestHandlerUtils.EXTENSION_JOB_PARAMETER_ACTION_NAME;
    public static final ADJobParameterAction INSTANCE = new ADJobParameterAction();

    private ADJobParameterAction() {
        super(NAME, JobParameterResponse::new);
    }

}
