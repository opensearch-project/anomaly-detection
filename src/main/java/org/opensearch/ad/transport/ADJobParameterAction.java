package org.opensearch.ad.transport;

import org.opensearch.action.ActionType;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.common.io.stream.Writeable;

public class ADJobParameterAction extends ActionType<ADJobParameterResponse> {

    public static final String NAME = CommonValue.EXTERNAL_ACTION_PREFIX + "get/job_parameter";
    public static final ADJobParameterAction INSTANCE = new ADJobParameterAction();

    private ADJobParameterAction() {
        super(NAME, ADJobParameterResponse::new);
    }

    public ADJobParameterAction(String name, Writeable.Reader<ADJobParameterResponse> adJobParameterResponseReader) {
        super(name, adJobParameterResponseReader);
    }
}
