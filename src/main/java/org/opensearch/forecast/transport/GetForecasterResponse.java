/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.forecast.transport;

import java.io.IOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.model.ForecasterProfile;
import org.opensearch.timeseries.model.EntityProfile;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class GetForecasterResponse extends ActionResponse implements ToXContentObject {

    public static final String FORECASTER_PROFILE = "forecasterProfile";
    public static final String ENTITY_PROFILE = "entityProfile";
    private String id;
    private long version;
    private long primaryTerm;
    private long seqNo;
    private Forecaster forecaster;
    private Job forecastJob;
    private ForecastTask realtimeTask;
    private ForecastTask runOnceTask;
    private RestStatus restStatus;
    private ForecasterProfile forecasterProfile;
    private EntityProfile entityProfile;
    private boolean profileResponse;
    private boolean returnJob;
    private boolean returnTask;

    public GetForecasterResponse(StreamInput in) throws IOException {
        super(in);
        profileResponse = in.readBoolean();
        if (profileResponse) {
            String profileType = in.readString();
            if (FORECASTER_PROFILE.equals(profileType)) {
                forecasterProfile = new ForecasterProfile(in);
            } else {
                entityProfile = new EntityProfile(in);
            }
        } else {
            id = in.readString();
            version = in.readLong();
            primaryTerm = in.readLong();
            seqNo = in.readLong();
            restStatus = in.readEnum(RestStatus.class);
            forecaster = new Forecaster(in);
            returnJob = in.readBoolean();
            if (returnJob) {
                forecastJob = new Job(in);
            } else {
                forecastJob = null;
            }
            returnTask = in.readBoolean();
            if (in.readBoolean()) {
                realtimeTask = new ForecastTask(in);
            } else {
                realtimeTask = null;
            }
            if (in.readBoolean()) {
                runOnceTask = new ForecastTask(in);
            } else {
                runOnceTask = null;
            }
        }

    }

    public GetForecasterResponse(
        String id,
        long version,
        long primaryTerm,
        long seqNo,
        Forecaster forecaster,
        Job job,
        boolean returnJob,
        ForecastTask realtimeTask,
        ForecastTask runOnceTask,
        boolean returnTask,
        RestStatus restStatus,
        ForecasterProfile forecasterProfile,
        EntityProfile entityProfile,
        boolean profileResponse
    ) {
        this.id = id;
        this.version = version;
        this.primaryTerm = primaryTerm;
        this.seqNo = seqNo;
        this.forecaster = forecaster;
        this.forecastJob = job;
        this.returnJob = returnJob;
        if (this.returnJob) {
            this.forecastJob = job;
        } else {
            this.forecastJob = null;
        }
        this.returnTask = returnTask;
        if (this.returnTask) {
            this.realtimeTask = realtimeTask;
            this.runOnceTask = runOnceTask;
        } else {
            this.realtimeTask = null;
            this.runOnceTask = null;
        }
        this.restStatus = restStatus;
        this.forecasterProfile = forecasterProfile;
        this.entityProfile = entityProfile;
        this.profileResponse = profileResponse;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (profileResponse) {
            out.writeBoolean(true); // profileResponse is true
            if (forecasterProfile != null) {
                out.writeString(FORECASTER_PROFILE);
                forecasterProfile.writeTo(out);
            } else if (entityProfile != null) {
                out.writeString(ENTITY_PROFILE);
                entityProfile.writeTo(out);
            }
        } else {
            out.writeBoolean(false); // profileResponse is false
            out.writeString(id);
            out.writeLong(version);
            out.writeLong(primaryTerm);
            out.writeLong(seqNo);
            out.writeEnum(restStatus);
            forecaster.writeTo(out);
            if (returnJob) {
                out.writeBoolean(true); // returnJob is true
                forecastJob.writeTo(out);
            } else {
                out.writeBoolean(false); // returnJob is false
            }
            out.writeBoolean(returnTask);
            if (realtimeTask != null) {
                out.writeBoolean(true);
                realtimeTask.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            if (runOnceTask != null) {
                out.writeBoolean(true);
                runOnceTask.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (profileResponse) {
            if (forecasterProfile != null) {
                forecasterProfile.toXContent(builder, params);
            } else {
                entityProfile.toXContent(builder, params);
            }
        } else {
            builder.startObject();
            builder.field(RestHandlerUtils._ID, id);
            builder.field(RestHandlerUtils._VERSION, version);
            builder.field(RestHandlerUtils._PRIMARY_TERM, primaryTerm);
            builder.field(RestHandlerUtils._SEQ_NO, seqNo);
            builder.field(RestHandlerUtils.REST_STATUS, restStatus);
            builder.field(RestHandlerUtils.FORECASTER, forecaster);
            if (returnJob) {
                builder.field(RestHandlerUtils.FORECASTER_JOB, forecastJob);
            }
            if (returnTask) {
                builder.field(RestHandlerUtils.REALTIME_TASK, realtimeTask);
                builder.field(RestHandlerUtils.RUN_ONCE_TASK, runOnceTask);
            }
            builder.endObject();
        }
        return builder;
    }

    public Job getForecastJob() {
        return forecastJob;
    }

    public ForecastTask getRealtimeTask() {
        return realtimeTask;
    }

    public ForecastTask getRunOnceTask() {
        return runOnceTask;
    }

    public Forecaster getForecaster() {
        return forecaster;
    }

    public ForecasterProfile getForecasterProfile() {
        return forecasterProfile;
    }

    public EntityProfile getEntityProfile() {
        return entityProfile;
    }
}
