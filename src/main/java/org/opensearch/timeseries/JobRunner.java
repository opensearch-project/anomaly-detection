/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import org.opensearch.ad.ADJobProcessor;
import org.opensearch.ad.InsightsJobProcessor;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.forecast.ForecastJobProcessor;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.timeseries.model.Job;

public class JobRunner implements ScheduledJobRunner {
    private static JobRunner INSTANCE;

    public static JobRunner getJobRunnerInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (JobRunner.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new JobRunner();
            return INSTANCE;
        }
    }

    @Override
    public void runJob(ScheduledJobParameter scheduledJobParameter, JobExecutionContext context) {
        if (!(scheduledJobParameter instanceof Job)) {
            throw new IllegalArgumentException(
                "Job parameter is not instance of Job, type: " + scheduledJobParameter.getClass().getCanonicalName()
            );
        }
        Job jobParameter = (Job) scheduledJobParameter;

        // Route to InsightsJobProcessor if this is the special Insights job
        if (ADCommonName.INSIGHTS_JOB_NAME.equals(jobParameter.getName())) {
            InsightsJobProcessor.getInstance().process(jobParameter, context);
            return;
        }

        // Route based on analysis type for regular jobs
        switch (jobParameter.getAnalysisType()) {
            case AD:
                ADJobProcessor.getInstance().process(jobParameter, context);
                break;
            case FORECAST:
                ForecastJobProcessor.getInstance().process(jobParameter, context);
                break;
            default:
                throw new IllegalArgumentException("Analysis type is not supported, type: : " + jobParameter.getAnalysisType());
        }
    }
}
