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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.rest;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.http.HttpHost;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.rest.RestStatus;

public class SecureADRestIT extends AnomalyDetectorRestTestCase {
    String aliceUser = "alice";
    RestClient aliceClient;
    String bobUser = "bob";
    RestClient bobClient;
    String catUser = "cat";
    RestClient catClient;
    String dogUser = "dog";
    RestClient dogClient;
    String elkUser = "elk";
    RestClient elkClient;

    @Before
    public void setupSecureTests() throws IOException {
        if (!isHttps())
            throw new IllegalArgumentException("Secure Tests are running but HTTPS is not set");
        createIndexRole("index_all_access", "*");
        createUser(aliceUser, aliceUser, new ArrayList<>(Arrays.asList("odfe")));
        aliceClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), aliceUser, aliceUser)
            .setSocketTimeout(60000)
            .build();

        createUser(bobUser, bobUser, new ArrayList<>(Arrays.asList("odfe")));
        bobClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), bobUser, bobUser)
            .setSocketTimeout(60000)
            .build();

        createUser(catUser, catUser, new ArrayList<>(Arrays.asList("aes")));
        catClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), catUser, catUser)
            .setSocketTimeout(60000)
            .build();

        createUser(dogUser, dogUser, new ArrayList<>(Arrays.asList()));
        dogClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), dogUser, dogUser)
            .setSocketTimeout(60000)
            .build();

        createUser(elkUser, elkUser, new ArrayList<>(Arrays.asList("odfe")));
        elkClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), elkUser, elkUser)
            .setSocketTimeout(60000)
            .build();

        createRoleMapping("anomaly_read_access", new ArrayList<>(Arrays.asList(bobUser)));
        createRoleMapping("anomaly_full_access", new ArrayList<>(Arrays.asList(aliceUser, catUser, dogUser, elkUser)));
        createRoleMapping("index_all_access", new ArrayList<>(Arrays.asList(aliceUser, bobUser, catUser, dogUser)));
    }

    @After
    public void deleteUserSetup() throws IOException {
        aliceClient.close();
        bobClient.close();
        catClient.close();
        dogClient.close();
        elkClient.close();
        deleteUser(aliceUser);
        deleteUser(bobUser);
        deleteUser(catUser);
        deleteUser(dogUser);
        deleteUser(elkUser);
    }

    public void testCreateAnomalyDetectorWithWriteAccess() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Assert.assertNotNull("User alice could not create detector", aliceDetector.getDetectorId());
    }

    public void testCreateAnomalyDetectorWithReadAccess() {
        // User Bob has AD read access, should not be able to create a detector
        Exception exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, bobClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]"));
    }

    public void testStartDetectorWithReadAccess() throws IOException {
        // User Bob has AD read access, should not be able to modify a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Assert.assertNotNull(aliceDetector.getDetectorId());
        Exception exception = expectThrows(IOException.class, () -> { startAnomalyDetector(aliceDetector.getDetectorId(), bobClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/jobmanagement]"));
    }

    public void testStartDetectorForWriteUser() throws IOException {
        // User Alice has AD full access, should be able to modify a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Assert.assertNotNull(aliceDetector.getDetectorId());
        Response response = startAnomalyDetector(aliceDetector.getDetectorId(), aliceClient);
        Assert.assertEquals(response.getStatusLine().toString(), "HTTP/1.1 200 OK");
    }

    public void testFilterByDisabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        // User Cat has AD full access, should be able to get a detector
        AnomalyDetector detector = getAnomalyDetector(aliceDetector.getDetectorId(), catClient);
        Assert.assertEquals(aliceDetector.getDetectorId(), detector.getDetectorId());
    }

    public void testGetApiFilterByEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { getAnomalyDetector(aliceDetector.getDetectorId(), catClient); });
        Assert
            .assertTrue(
                exception.getMessage().contains("User does not have permissions to access detector: " + aliceDetector.getDetectorId())
            );
    }

    public void testStartApiFilterByEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { startAnomalyDetector(aliceDetector.getDetectorId(), catClient); });
        Assert
            .assertTrue(
                exception.getMessage().contains("User does not have permissions to access detector: " + aliceDetector.getDetectorId())
            );
    }

    public void testStopApiFilterByEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { stopAnomalyDetector(aliceDetector.getDetectorId(), catClient); });
        Assert
            .assertTrue(
                exception.getMessage().contains("User does not have permissions to access detector: " + aliceDetector.getDetectorId())
            );
    }

    public void testDeleteApiFilterByEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { deleteAnomalyDetector(aliceDetector.getDetectorId(), catClient); });
        Assert
            .assertTrue(
                exception.getMessage().contains("User does not have permissions to access detector: " + aliceDetector.getDetectorId())
            );
    }

    public void testCreateAnomalyDetectorWithNoBackendRole() throws IOException {
        enableFilterBy();
        // User Dog has AD full access, but has no backend role
        // When filter by is enabled, we block creating Detectors
        Exception exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, dogClient); });
        Assert
            .assertTrue(
                exception.getMessage().contains("Filter by backend roles is enabled and User dog does not have backend roles configured")
            );
    }

    public void testCreateAnomalyDetectorWithNoReadPermissionOfIndex() throws IOException {
        enableFilterBy();
        // User alice has AD full access and index permission, so can create detector
        AnomalyDetector anomalyDetector = createRandomAnomalyDetector(false, false, aliceClient);
        // User elk has AD full access, but has no read permission of index
        String indexName = anomalyDetector.getIndices().get(0);
        Exception exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, indexName, elkClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [indices:data/read/search]"));
    }

    public void testPreviewAnomalyDetectorWithWriteAccess() throws IOException {
        // User Alice has AD full access, should be able to create/preview a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            aliceDetector.getDetectorId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );
        Response response = previewAnomalyDetector(aliceDetector.getDetectorId(), aliceClient, input);
        Assert.assertEquals(RestStatus.OK, restStatus(response));
    }

    public void testPreviewAnomalyDetectorWithReadAccess() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            randomAlphaOfLength(5),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );
        // User bob has AD read access, should not be able to preview a detector
        Exception exception = expectThrows(
            IOException.class,
            () -> { previewAnomalyDetector(aliceDetector.getDetectorId(), bobClient, input); }
        );
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/preview]"));
    }

    public void testPreviewAnomalyDetectorWithFilterEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            aliceDetector.getDetectorId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(
            IOException.class,
            () -> { previewAnomalyDetector(aliceDetector.getDetectorId(), catClient, input); }
        );
        Assert
            .assertTrue(
                exception.getMessage().contains("User does not have permissions to access detector: " + aliceDetector.getDetectorId())
            );
    }

    // TODO: enable this test case when we have latest docker image
    @Ignore
    public void testPreviewAnomalyDetectorWithNoReadPermissionOfIndex() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            aliceDetector.getDetectorId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            aliceDetector
        );
        enableFilterBy();
        // User elk has no read permission of index
        Exception exception = expectThrows(
            Exception.class,
            () -> { previewAnomalyDetector(aliceDetector.getDetectorId(), elkClient, input); }
        );
        Assert.assertTrue(exception.getMessage().contains("no permissions for [indices:data/read/search]"));
    }
}
