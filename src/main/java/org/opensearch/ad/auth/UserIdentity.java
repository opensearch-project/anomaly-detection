/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.auth;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Response;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.internal.ToStringBuilder;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;

/**
 * Placeholder class to replace the {@code User} class in common-utils in an extension context. The process of populating the object
 * from an authenticated user will be developed in the future.
 * <p>
 * Actual user name and roles will be removed and replaced by an access token which correlates to the granted roles. Metadata will
 * replace custom attribute names.
 * <p>
 * Gets current Authenticated User - name, odfe roles. If security-plugin is not
 * installed or disabled, it returns empty for user name and roles.
 */
final public class UserIdentity implements Writeable, ToXContent {

    // field name in toXContent
    public static final String NAME_FIELD = "name";
    public static final String BACKEND_ROLES_FIELD = "backend_roles";
    public static final String ROLES_FIELD = "roles";
    public static final String CUSTOM_ATTRIBUTE_NAMES_FIELD = "custom_attribute_names";
    public static final String REQUESTED_TENANT_FIELD = "user_requested_tenant";

    private final String name;
    private final List<String> backendRoles;
    private final List<String> roles;
    private final List<String> customAttNames;
    @Nullable
    private final String requestedTenant;

    /**
     * A default user identity with no name, no roles, no custom attributes, and no tenant.
     */
    public UserIdentity() {
        name = "";
        backendRoles = new ArrayList<>();
        roles = new ArrayList<>();
        customAttNames = new ArrayList<>();
        requestedTenant = null;
    }

    /**
     * A user identity with the requested name, backend roles, roles, and custom attributes, but no tenant.
     *
     * @param name  The user name.
     * @param backendRoles  The user's backend roles.
     * @param roles  The user's roles,
     * @param customAttNames  A list of custom attribute names.
     */
    public UserIdentity(final String name, final List<String> backendRoles, List<String> roles, List<String> customAttNames) {
        this.name = name;
        this.backendRoles = backendRoles;
        this.roles = roles;
        this.customAttNames = customAttNames;
        this.requestedTenant = null;
    }

    /**
     * A user identity with the requested name, backend roles, roles, custom attributes, and tenant.
     *
     * @param name  The user name.
     * @param backendRoles  The user's backend roles.
     * @param roles  The user's roles,
     * @param customAttNames  A list of custom attribute names.
     * @param requestedTenant  The requested tenant.
     */
    public UserIdentity(
        final String name,
        final List<String> backendRoles,
        final List<String> roles,
        final List<String> customAttNames,
        @Nullable final String requestedTenant
    ) {
        this.name = name;
        this.backendRoles = backendRoles;
        this.roles = roles;
        this.customAttNames = customAttNames;
        this.requestedTenant = requestedTenant;
    }

    /**
     * Response of "GET /_opendistro/_security/authinfo".  This parses the response of a call to the security plugin into a User Identity.
     *
     * @param response  The security plugin response.
     * @throws IOException  If there was an error receiving the response.
     */
    public UserIdentity(final Response response) throws IOException, ParseException {
        this(EntityUtils.toString(response.getEntity()));
    }

    /**
     * A user identity created from a JSON string.
     *
     * @param json  The JSON containing the user information.
     */
    @SuppressWarnings("unchecked")
    public UserIdentity(String json) {
        if (Strings.isNullOrEmpty(json)) {
            throw new IllegalArgumentException("Response json cannot be null");
        }

        Map<String, Object> mapValue = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
        name = (String) mapValue.get("user_name");
        backendRoles = (List<String>) mapValue.get("backend_roles");
        roles = (List<String>) mapValue.get("roles");
        customAttNames = (List<String>) mapValue.get("custom_attribute_names");
        requestedTenant = (String) mapValue.getOrDefault("user_requested_tenant", null);
    }

    /**
     * A user identity created from a stream.
     *
     * @param in  The input stream.
     * @throws IOException  if there was an error reading the stream.
     */
    public UserIdentity(StreamInput in) throws IOException {
        name = in.readString();
        backendRoles = in.readStringList();
        roles = in.readStringList();
        customAttNames = in.readStringList();
        requestedTenant = in.readOptionalString();
    }

    /**
     * Create a user identity from a parser.
     *
     * @param parser  The parser containing the identiy.
     * @return THe user identity.
     * @throws IOException  If there was an exception parsing.
     */
    public static UserIdentity parse(XContentParser parser) throws IOException {
        String name = "";
        List<String> backendRoles = new ArrayList<>();
        List<String> roles = new ArrayList<>();
        List<String> customAttNames = new ArrayList<>();
        String requestedTenant = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case BACKEND_ROLES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        backendRoles.add(parser.text());
                    }
                    break;
                case ROLES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        roles.add(parser.text());
                    }
                    break;
                case CUSTOM_ATTRIBUTE_NAMES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        customAttNames.add(parser.text());
                    }
                    break;
                case REQUESTED_TENANT_FIELD:
                    requestedTenant = parser.textOrNull();
                    break;
                default:
                    break;
            }
        }
        return new UserIdentity(name, backendRoles, roles, customAttNames, requestedTenant);
    }

    /**
     * Create a user identity from a pipe-delimited string.
     *
     * @param userString  A string in the format user_name|backendrole1,backendrole2|roles1,role2
     * @return The user identity.
     */
    public static UserIdentity parse(final String userString) {
        if (Strings.isNullOrEmpty(userString)) {
            return null;
        }

        String[] strs = userString.split("\\|");
        if ((strs.length == 0) || (Strings.isNullOrEmpty(strs[0]))) {
            return null;
        }

        String userName = strs[0].trim();
        List<String> backendRoles = new ArrayList<>();
        List<String> roles = new ArrayList<>();
        String requestedTenant = null;

        if ((strs.length > 1) && !Strings.isNullOrEmpty(strs[1])) {
            backendRoles.addAll(Arrays.asList(strs[1].split(",")));
        }
        if ((strs.length > 2) && !Strings.isNullOrEmpty(strs[2])) {
            roles.addAll(Arrays.asList(strs[2].split(",")));
        }
        if ((strs.length > 3) && !Strings.isNullOrEmpty(strs[3])) {
            requestedTenant = strs[3].trim();
        }
        return new UserIdentity(userName, backendRoles, roles, Arrays.asList(), requestedTenant);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder
            .startObject()
            .field(NAME_FIELD, name)
            .field(BACKEND_ROLES_FIELD, backendRoles)
            .field(ROLES_FIELD, roles)
            .field(CUSTOM_ATTRIBUTE_NAMES_FIELD, customAttNames)
            .field(REQUESTED_TENANT_FIELD, requestedTenant);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(backendRoles);
        out.writeStringCollection(roles);
        out.writeStringCollection(customAttNames);
        out.writeOptionalString(requestedTenant);
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this.getClass());
        builder.add(NAME_FIELD, name);
        builder.add(BACKEND_ROLES_FIELD, backendRoles);
        builder.add(ROLES_FIELD, roles);
        builder.add(CUSTOM_ATTRIBUTE_NAMES_FIELD, customAttNames);
        builder.add(REQUESTED_TENANT_FIELD, requestedTenant);
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UserIdentity)) {
            return false;
        }
        UserIdentity that = (UserIdentity) obj;
        return this.name.equals(that.name)
            && this.getBackendRoles().equals(that.backendRoles)
            && this.getRoles().equals(that.roles)
            && this.getCustomAttNames().equals(that.customAttNames)
            && (Objects.equals(this.requestedTenant, that.requestedTenant));
    }

    public String getName() {
        return name;
    }

    public List<String> getBackendRoles() {
        return backendRoles;
    }

    public List<String> getRoles() {
        return roles;
    }

    public List<String> getCustomAttNames() {
        return customAttNames;
    }

    @Nullable
    public String getRequestedTenant() {
        return requestedTenant;
    }
}
