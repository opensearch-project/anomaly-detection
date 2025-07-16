/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.timeseries.util;

import static org.opensearch.timeseries.constant.CommonName.TENANT_ID_HEADER;

import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;

/**
 * Helper class for tenant ID validation and throttling in time series analytics
 */
public class TenantAwareHelper {

    private TenantAwareHelper() {}

    /**
     * Finds the tenant id in the REST Headers
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @param restRequest the RestRequest
     * @return The tenant ID from the headers or null if multitenancy is not enabled
     */
    public static String getTenantID(Boolean isMultiTenancyEnabled, RestRequest restRequest) {
        if (!isMultiTenancyEnabled) {
            return null;
        }

        Map<String, List<String>> headers = restRequest.getHeaders();

        List<String> tenantIdList = headers.get(TENANT_ID_HEADER);
        if (tenantIdList == null || tenantIdList.isEmpty()) {
            throw new OpenSearchStatusException("Tenant ID header is missing or has no value", RestStatus.FORBIDDEN);
        }

        String tenantId = tenantIdList.get(0);
        if (tenantId == null) {
            throw new OpenSearchStatusException("Tenant ID can't be null", RestStatus.FORBIDDEN);
        }

        return tenantId;
    }

    /**
     * Validates the tenant ID based on the multi-tenancy feature setting.
     *
     * @param isMultiTenancyEnabled whether the multi-tenancy feature is enabled.
     * @param tenantId The tenant ID to validate.
     * @param listener The action listener to handle failure cases.
     * @return true if the tenant ID is valid or if multi-tenancy is not enabled; false if the tenant ID is invalid and multi-tenancy is enabled.
     */
    public static boolean validateTenantId(boolean isMultiTenancyEnabled, String tenantId, ActionListener<?> listener) {
        if (isMultiTenancyEnabled && tenantId == null) {
            listener.onFailure(new OpenSearchStatusException("No permission to access this resource", RestStatus.FORBIDDEN));
            return false;
        } else {
            return true;
        }
    }
}
