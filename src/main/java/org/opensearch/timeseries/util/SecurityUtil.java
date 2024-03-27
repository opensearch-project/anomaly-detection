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

package org.opensearch.timeseries.util;

import java.util.Collections;
import java.util.List;

import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;

import com.google.common.collect.ImmutableList;

public class SecurityUtil {
    /**
     * @param userObj the last user who edited the detector config
     * @param settings Node settings
     * @return converted user for bwc if necessary
     */
    private static User getAdjustedUserBWC(User userObj, Settings settings) {
        /*
         * We need to handle 3 cases:
         * 1. Detectors created by older versions and never updated. These detectors wont have User details in the
         * detector object. `detector.user` will be null. Insert `all_access, AmazonES_all_access` role.
         * 2. Detectors are created when security plugin is disabled, these will have empty User object.
         * (`detector.user.name`, `detector.user.roles` are empty )
         * 3. Detectors are created when security plugin is enabled, these will have an User object.
         * This will inject user role and check if the user role has permissions to call the execute
         * Anomaly Result API.
         */
        String user;
        List<String> roles;
        if (userObj == null) {
            // It's possible that user create domain with security disabled, then enable security
            // after upgrading. This is for BWC, for old detectors which created when security
            // disabled, the user will be null.
            // This is a huge promotion in privileges. To prevent a caller code from making a mistake and pass a null object,
            // we make the method private and only allow fetching user object from detector or job configuration (see the public
            // access methods with the same name).
            user = "";
            roles = settings.getAsList("", ImmutableList.of("all_access", "AmazonES_all_access"));
            return new User(user, Collections.emptyList(), roles, Collections.emptyList());
        } else {
            return userObj;
        }
    }

    /**
     * *
     * @param config analysis config
     * @param settings Node settings
     * @return user recorded by a detector. Made adjstument for BWC (backward-compatibility) if necessary.
     */
    public static User getUserFromConfig(Config config, Settings settings) {
        return getAdjustedUserBWC(config.getUser(), settings);
    }

    /**
     * *
     * @param detectorJob Detector Job
     * @param settings Node settings
     * @return user recorded by a detector job
     */
    public static User getUserFromJob(Job detectorJob, Settings settings) {
        return getAdjustedUserBWC(detectorJob.getUser(), settings);
    }
}
