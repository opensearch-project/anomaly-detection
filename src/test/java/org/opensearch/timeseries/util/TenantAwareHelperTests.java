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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

/**
 * Unit tests for TenantAwareHelper
 */
public class TenantAwareHelperTests extends OpenSearchTestCase {

    public void testValidateTenantIdWithMultiTenancyEnabled() {
        ActionListener<Object> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(Object response) {
                fail("Should not reach here");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof OpenSearchStatusException);
                assertEquals("No permission to access this resource", e.getMessage());
            }
        };

        assertFalse(TenantAwareHelper.validateTenantId(true, null, actionListener));
        assertTrue(TenantAwareHelper.validateTenantId(true, "_tenant_id", actionListener));
    }

    public void testValidateTenantIdWithMultiTenancyDisabled() {
        ActionListener<Object> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(Object response) {
                fail("Should not reach here");
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not reach here");
            }
        };

        assertTrue(TenantAwareHelper.validateTenantId(false, null, actionListener));
    }

    public void testGetTenantIdFromHeaders() {
        String tenantId = "test-tenant";
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(TENANT_ID_HEADER, Collections.singletonList(tenantId));

        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        assertEquals(tenantId, TenantAwareHelper.getTenantID(true, restRequest));
    }

    public void testGetTenantIdFromHeadersWithNullValue() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(TENANT_ID_HEADER, Collections.singletonList(null));

        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TenantAwareHelper.getTenantID(true, restRequest)
        );
        assertEquals("Tenant ID can't be null", exception.getMessage());
    }

    public void testGetTenantIdFromHeadersWithMissingHeader() {
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).build();

        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TenantAwareHelper.getTenantID(true, restRequest)
        );
        assertEquals("Tenant ID header is missing or has no value", exception.getMessage());
    }

    public void testGetTenantIdFromHeadersWithEmptyList() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(TENANT_ID_HEADER, Collections.emptyList());

        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> TenantAwareHelper.getTenantID(true, restRequest)
        );
        assertEquals("Tenant ID header is missing or has no value", exception.getMessage());
    }

    public void testGetTenantIdWithMultiTenancyDisabled() {
        String tenantId = "test-tenant";
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(TENANT_ID_HEADER, Collections.singletonList(tenantId));

        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        assertNull(TenantAwareHelper.getTenantID(false, restRequest));
    }
}
