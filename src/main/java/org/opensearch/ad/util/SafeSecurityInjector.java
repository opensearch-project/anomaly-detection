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

package org.opensearch.ad.util;

import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.InjectSecurity;

public abstract class SafeSecurityInjector implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(SafeSecurityInjector.class);
    // user header used by security plugin. As we cannot take security plugin as
    // a compile dependency, we have to duplicate it here.
    private static final String OPENDISTRO_SECURITY_USER = "_opendistro_security_user";

    private InjectSecurity rolesInjectorHelper;
    protected String id;
    protected Settings settings;
    protected ThreadContext tc;

    public SafeSecurityInjector(String id, Settings settings, ThreadContext tc) {
        this.id = id;
        this.settings = settings;
        this.tc = tc;
        this.rolesInjectorHelper = null;
    }

    protected boolean shouldInject() {
        if (id == null || settings == null || tc == null) {
            LOG.debug(String.format(Locale.ROOT, "null value: id: %s, settings: %s, threadContext: %s", id, settings, tc));
            return false;
        }
        // user not null means the request comes from user (e.g., public restful API)
        // we don't need to inject roles.
        Object userIn = tc.getTransient(OPENDISTRO_SECURITY_USER);
        if (userIn != null) {
            LOG.debug(new ParameterizedMessage("User not empty in thread context: [{}]", userIn));
            return false;
        }
        userIn = tc.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        if (userIn != null) {
            LOG.debug(new ParameterizedMessage("User not empty in thread context: [{}]", userIn));
            return false;
        }
        Object rolesin = tc.getTransient(ConfigConstants.OPENSEARCH_SECURITY_INJECTED_ROLES);
        if (rolesin != null) {
            LOG.warn(new ParameterizedMessage("Injected roles not empty in thread context: [{}]", rolesin));
            return false;
        }

        return true;
    }

    protected void inject(String user, List<String> roles) {
        if (roles == null) {
            LOG.warn("Cannot inject empty roles in thread context");
            return;
        }
        if (rolesInjectorHelper == null) {
            // lazy init
            rolesInjectorHelper = new InjectSecurity(id, settings, tc);
        }
        rolesInjectorHelper.inject(user, roles);
    }

    @Override
    public void close() {
        if (rolesInjectorHelper != null) {
            rolesInjectorHelper.close();
        }
    }
}
