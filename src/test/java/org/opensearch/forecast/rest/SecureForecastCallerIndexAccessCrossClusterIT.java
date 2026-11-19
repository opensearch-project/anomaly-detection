/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.timeseries.CrossClusterRestTestCase;

/**
 * A caller that holds the forecast permissions but is denied a forecaster's remote source index must not be able to
 * read that index by running a forecaster another user created, whether the config names the remote index directly,
 * through a remote alias, or through a remote wildcard. Covers the cross-cluster index access check of ConfigIndexAccess;
 * run by the integTestCrossCluster task.
 *
 * The test cluster runs with do_not_fail_on_forbidden off, so a caller's own search over a remote alias or wildcard is
 * already denied. The case where do_not_fail_on_forbidden is on (the caller's search is reduced to what it may read and
 * succeeds, so only the owner-identity listing catches the denied index) is covered by ConfigIndexAccessTests.
 */
public class SecureForecastCallerIndexAccessCrossClusterIT extends CrossClusterRestTestCase {
    private static final String FORECASTERS_URI = "/_plugins/_forecast/forecasters";
    private static final String OWNER = "ccs_owner";
    private static final String CALLER = "ccs_caller";
    private static final String NO_RESOLVE_OWNER = "ccs_no_resolve_owner";
    private static final String OWNER_ROLE = "ccs_owner_role";
    private static final String CALLER_ROLE = "ccs_caller_role";
    private static final String NO_RESOLVE_OWNER_ROLE = "ccs_no_resolve_owner_role";
    private static final String INDEX_ACCESS_DENIAL = "User doesn't have read permissions for one or more configured index";
    private static final String READABLE = "ccs-caller-readable";
    private static final String DENIED = "ccs-caller-denied";
    private static final String ALIAS = "ccs-caller-alias";

    private RestClient ownerClient;
    private RestClient callerClient;
    private RestClient noResolveOwnerClient;
    private String password;
    private boolean prepared = false;

    // Not in setUp()/@Before: the framework builds the REST client in its own @Before, which runs after setUp().
    private void prepare() throws Exception {
        if (prepared) {
            return;
        }
        // integTestCrossCluster runs with and without security; this test needs the security plugin and https
        assumeTrue("secure cross-cluster test requires https", isHttps());
        assumeFalse("covers the deployment without resource sharing", isResourceSharingFeatureEnabled());
        updateClusterSettings(ForecastEnabledSetting.FORECAST_ENABLED, true);
        connectRemoteCluster();
        Instant end = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        password = generatePassword(OWNER);

        createFollowerIndex(READABLE, 200, end);
        createFollowerIndex(DENIED, 200, end);
        createFollowerAlias(ALIAS, READABLE, DENIED);

        // The owner may read every index and may resolve. The caller holds the same forecast permissions but may read
        // only the readable source index and the forecast system and result indices. The no-resolve owner may read
        // every index but lacks indices:admin/resolve/index. Users and roles exist on both clusters so the caller is
        // mapped the same way when a search reaches the follower.
        for (RestClient c : new RestClient[] { client(), followerClient() }) {
            createSecurityRole(c, OWNER_ROLE, "[\"*\"]", true);
            createSecurityRole(c, CALLER_ROLE, "[\"" + READABLE + "\",\"opensearch-forecast-*\"]", true);
            createSecurityRole(c, NO_RESOLVE_OWNER_ROLE, "[\"*\"]", false);
            createSecurityUser(c, OWNER, OWNER_ROLE);
            createSecurityUser(c, CALLER, CALLER_ROLE);
            createSecurityUser(c, NO_RESOLVE_OWNER, NO_RESOLVE_OWNER_ROLE);
        }

        ownerClient = clientForUser(OWNER);
        callerClient = clientForUser(CALLER);
        noResolveOwnerClient = clientForUser(NO_RESOLVE_OWNER);
        prepared = true;
    }

    @Override
    public void tearDown() throws Exception {
        if (prepared) {
            closeQuietly(ownerClient);
            closeQuietly(callerClient);
            closeQuietly(noResolveOwnerClient);
            for (RestClient c : new RestClient[] { client(), followerClientSafe() }) {
                if (c == null) {
                    continue;
                }
                for (String user : new String[] { OWNER, CALLER, NO_RESOLVE_OWNER }) {
                    deleteSecurity(c, "internalusers/" + user);
                }
                for (String role : new String[] { OWNER_ROLE, CALLER_ROLE, NO_RESOLVE_OWNER_ROLE }) {
                    deleteSecurity(c, "rolesmapping/" + role);
                    deleteSecurity(c, "roles/" + role);
                }
            }
        }
        super.tearDown();
    }

    public void testCallerDeniedRemoteIndex() throws Exception {
        prepare();
        denyCallerAllowOwner(ownerClient, remote(DENIED));
    }

    public void testCallerDeniedRemoteAlias() throws Exception {
        prepare();
        denyCallerAllowOwner(ownerClient, remote(ALIAS));
    }

    public void testCallerDeniedRemoteWildcard() throws Exception {
        prepare();
        denyCallerAllowOwner(ownerClient, remote("ccs-caller-*"));
    }

    public void testCallerAllowedReadableRemoteIndex() throws Exception {
        prepare();
        // The caller may read this one, so it may run it once and start it.
        assertCallerStatus(callerClient, remote(READABLE), "_run_once", RestStatus.OK, null);
        assertCallerStatus(callerClient, remote(READABLE), "_start", RestStatus.OK, null);
    }

    public void testCallerDeniedWhenOwnerLacksResolvePermission() throws Exception {
        prepare();
        // The bypass of the previous revision: with an owner that cannot resolve the remote indices, the caller was let
        // through. It must still be denied, by probing the expression as the caller.
        denyCallerAllowOwner(noResolveOwnerClient, remote(DENIED));
        denyCallerAllowOwner(noResolveOwnerClient, remote(ALIAS));
        denyCallerAllowOwner(noResolveOwnerClient, remote("ccs-caller-*"));
    }

    private void denyCallerAllowOwner(RestClient ownerC, String index) throws Exception {
        // Both run-once (the originally reported path) and start read the source under the config's identity, so the
        // caller must be denied and the owner allowed on each. A separate forecaster per action avoids a run-once vs
        // start conflict on the same config.
        denyCallerAllowOwner(ownerC, index, "_run_once");
        denyCallerAllowOwner(ownerC, index, "_start");
    }

    private void denyCallerAllowOwner(RestClient ownerC, String index, String action) throws Exception {
        String forecasterId = createForecaster(ownerC, index);
        try {
            // the caller is denied by the index access check, not by another check such as resource sharing
            perform(callerClient, forecasterId, action, RestStatus.FORBIDDEN, INDEX_ACCESS_DENIAL);
            // the owner, who can read the index, is allowed
            perform(ownerC, forecasterId, action, RestStatus.OK, null);
        } finally {
            if (action.equals("_start")) {
                stop(forecasterId);
            }
        }
    }

    private void assertCallerStatus(RestClient client, String index, String action, RestStatus expected, String expectedMessage)
        throws Exception {
        String forecasterId = createForecaster(ownerClient, index);
        try {
            perform(client, forecasterId, action, expected, expectedMessage);
        } finally {
            if (action.equals("_start")) {
                stop(forecasterId);
            }
        }
    }

    private void perform(RestClient client, String forecasterId, String action, RestStatus expected, String expectedMessage)
        throws IOException {
        Request request = new Request("POST", String.format(Locale.ROOT, "%s/%s/%s", FORECASTERS_URI, forecasterId, action));
        try {
            Response response = client.performRequest(request);
            assertEquals(expected.getStatus(), response.getStatusLine().getStatusCode());
        } catch (ResponseException e) {
            assertEquals(e.getMessage(), expected.getStatus(), e.getResponse().getStatusLine().getStatusCode());
            if (expectedMessage != null) {
                assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
            }
        }
    }

    private void stop(String forecasterId) {
        try {
            client().performRequest(new Request("POST", String.format(Locale.ROOT, "%s/%s/_stop", FORECASTERS_URI, forecasterId)));
        } catch (Exception e) {
            // nothing was started
        }
    }

    private String createForecaster(RestClient client, String index) throws IOException {
        String body = String
            .format(
                Locale.ROOT,
                "{\"name\":\"ccs-caller-%d\",\"time_field\":\"%s\",\"indices\":[\"%s\"],"
                    + "\"feature_attributes\":[{\"feature_id\":\"max1\",\"feature_name\":\"max1\",\"feature_enabled\":true,"
                    + "\"importance\":1,\"aggregation_query\":{\"max1\":{\"max\":{\"field\":\"%s\"}}}}],"
                    + "\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"MINUTES\"}},\"schema_version\":2,\"horizon\":24,"
                    + "\"forecast_interval\":{\"period\":{\"interval\":10,\"unit\":\"MINUTES\"}},\"category_field\":[\"%s\"]}",
                System.nanoTime(),
                TIME_FIELD,
                index,
                VALUE_FIELD,
                CATEGORY_FIELD
            );
        Request request = new Request("POST", FORECASTERS_URI);
        request.setJsonEntity(body);
        Response response = client.performRequest(request);
        String forecasterId = (String) entityAsMap(response).get("_id");
        assertNotNull(forecasterId);
        return forecasterId;
    }

    private RestClient clientForUser(String user) throws IOException {
        return new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), user, password)
            .setSocketTimeout(60000)
            .build();
    }

    private void createSecurityRole(RestClient client, String role, String indexPatterns, boolean canResolve) throws IOException {
        StringBuilder actions = new StringBuilder("\"indices:data/read*\",\"indices:admin/mappings/fields/get*\",")
            .append("\"indices:admin/mappings/get\",\"indices:admin/aliases/get\",\"indices_monitor\"");
        if (canResolve) {
            actions.append(",\"indices:admin/resolve/index\"");
        }
        String body = String
            .format(
                Locale.ROOT,
                "{\"cluster_permissions\":[\"cluster:admin/plugin/forecast/*\",\"cluster:monitor/*\"],"
                    + "\"index_permissions\":[{\"index_patterns\":%s,\"allowed_actions\":[%s]}]}",
                indexPatterns,
                actions
            );
        putSecurity(client, "roles/" + role, body);
    }

    private void createSecurityUser(RestClient client, String user, String role) throws IOException {
        putSecurity(client, "internalusers/" + user, String.format(Locale.ROOT, "{\"password\":\"%s\"}", password));
        putSecurity(client, "rolesmapping/" + role, String.format(Locale.ROOT, "{\"users\":[\"%s\"]}", user));
    }

    private void putSecurity(RestClient client, String path, String body) throws IOException {
        Request request = new Request("PUT", "/_plugins/_security/api/" + path);
        request.setJsonEntity(body);
        request.setOptions(request.getOptions().toBuilder().addHeader("User-Agent", "Kibana"));
        client.performRequest(request);
    }

    private void deleteSecurity(RestClient client, String path) {
        try {
            Request request = new Request("DELETE", "/_plugins/_security/api/" + path);
            request.setOptions(request.getOptions().toBuilder().addHeader("User-Agent", "Kibana"));
            client.performRequest(request);
        } catch (Exception e) {
            // best effort cleanup
        }
    }

    private static void closeQuietly(RestClient client) {
        try {
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }

    private RestClient followerClientSafe() {
        try {
            return followerClient();
        } catch (IOException e) {
            return null;
        }
    }
}
