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

package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.model.EntityProfile;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

public class GetAnomalyDetectorResponse extends ActionResponse implements ToXContentObject {
    public static final String DETECTOR_PROFILE = "detectorProfile";
    public static final String ENTITY_PROFILE = "entityProfile";
    private long version;
    private String id;
    private long primaryTerm;
    private long seqNo;
    private AnomalyDetector detector;
    // private AnomalyDetectorJob adJob;
    private ADTask realtimeAdTask;
    private ADTask historicalAdTask;
    private RestStatus restStatus;
    private DetectorProfile detectorProfile;
    private EntityProfile entityProfile;
    private boolean profileResponse;
    private boolean returnJob;
    private boolean returnTask;

    public GetAnomalyDetectorResponse(StreamInput in) throws IOException {
        super(in);
        profileResponse = in.readBoolean();
        if (profileResponse) {
            String profileType = in.readString();
            if (DETECTOR_PROFILE.equals(profileType)) {
                detectorProfile = new DetectorProfile(in);
            } else {
                entityProfile = new EntityProfile(in);
            }

        } else {
            detectorProfile = null;
            id = in.readString();
            version = in.readLong();
            primaryTerm = in.readLong();
            seqNo = in.readLong();
            restStatus = in.readEnum(RestStatus.class);
            detector = new AnomalyDetector(in);
            returnJob = in.readBoolean();
            // if (returnJob) {
            // adJob = new AnomalyDetectorJob(in);
            // } else {
            // adJob = null;
            // }
            returnTask = in.readBoolean();
            if (in.readBoolean()) {
                realtimeAdTask = new ADTask(in);
            } else {
                realtimeAdTask = null;
            }
            if (in.readBoolean()) {
                historicalAdTask = new ADTask(in);
            } else {
                historicalAdTask = null;
            }
        }
    }

    public GetAnomalyDetectorResponse(
        long version,
        String id,
        long primaryTerm,
        long seqNo,
        AnomalyDetector detector,
        // AnomalyDetectorJob adJob,
        boolean returnJob,
        ADTask realtimeAdTask,
        ADTask historicalAdTask,
        boolean returnTask,
        RestStatus restStatus,
        DetectorProfile detectorProfile,
        EntityProfile entityProfile,
        boolean profileResponse
    ) {
        this.version = version;
        this.id = id;
        this.primaryTerm = primaryTerm;
        this.seqNo = seqNo;
        this.detector = detector;
        this.restStatus = restStatus;
        this.returnJob = returnJob;

        // if (this.returnJob) {
        // this.adJob = adJob;
        // } else {
        // this.adJob = null;
        // }
        this.returnTask = returnTask;
        if (this.returnTask) {
            this.realtimeAdTask = realtimeAdTask;
            this.historicalAdTask = historicalAdTask;
        } else {
            this.realtimeAdTask = null;
            this.historicalAdTask = null;
        }
        this.detectorProfile = detectorProfile;
        this.entityProfile = entityProfile;
        this.profileResponse = profileResponse;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (profileResponse) {
            out.writeBoolean(true); // profileResponse is true
            if (detectorProfile != null) {
                out.writeString(DETECTOR_PROFILE);
                detectorProfile.writeTo(out);
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
            detector.writeTo(out);
            // if (returnJob) {
            // out.writeBoolean(true); // returnJob is true
            // adJob.writeTo(out);
            // } else {
            // out.writeBoolean(false); // returnJob is false
            // }
            out.writeBoolean(returnTask);
            if (realtimeAdTask != null) {
                out.writeBoolean(true);
                realtimeAdTask.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            if (historicalAdTask != null) {
                out.writeBoolean(true);
                historicalAdTask.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (profileResponse) {
            if (detectorProfile != null) {
                detectorProfile.toXContent(builder, params);
            } else {
                entityProfile.toXContent(builder, params);
            }
        } else {
            builder.startObject();
            builder.field(RestHandlerUtils._ID, id);
            builder.field(RestHandlerUtils._VERSION, version);
            builder.field(RestHandlerUtils._PRIMARY_TERM, primaryTerm);
            builder.field(RestHandlerUtils._SEQ_NO, seqNo);
            builder.field(RestHandlerUtils.ANOMALY_DETECTOR, detector);
            // if (returnJob) {
            // builder.field(RestHandlerUtils.ANOMALY_DETECTOR_JOB, adJob);
            // }
            if (returnTask) {
                builder.field(RestHandlerUtils.REALTIME_TASK, realtimeAdTask);
                builder.field(RestHandlerUtils.HISTORICAL_ANALYSIS_TASK, historicalAdTask);
            }
            builder.endObject();
        }
        return builder;
    }

    public DetectorProfile getDetectorProfile() {
        return detectorProfile;
    }

    // public AnomalyDetectorJob getAdJob() {
    // return adJob;
    // }

    public ADTask getRealtimeAdTask() {
        return realtimeAdTask;
    }

    public ADTask getHistoricalAdTask() {
        return historicalAdTask;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }
}
