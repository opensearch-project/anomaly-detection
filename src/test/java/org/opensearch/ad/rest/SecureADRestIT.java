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

package org.opensearch.ad.rest;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.http.HttpHost;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.commons.authuser.User;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.rest.RestStatus;

import com.google.common.collect.ImmutableList;

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
    String fishUser = "fish";
    RestClient fishClient;
    String goatUser = "goat";
    RestClient goatClient;
    String lionUser = "lion";
    RestClient lionClient;
    private String indexAllAccessRole = "index_all_access";
    private String indexSearchAccessRole = "index_all_search";

    @Before
    public void setupSecureTests() throws IOException {
        if (!isHttps())
            throw new IllegalArgumentException("Secure Tests are running but HTTPS is not set");
        createIndexRole(indexAllAccessRole, "*");
        createSearchRole(indexSearchAccessRole, "*");
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

        createUser(fishUser, fishUser, new ArrayList<>(Arrays.asList("odfe", "aes")));
        fishClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), fishUser, fishUser)
            .setSocketTimeout(60000)
            .build();

        createUser(goatUser, goatUser, new ArrayList<>(Arrays.asList("opensearch")));
        goatClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), goatUser, goatUser)
            .setSocketTimeout(60000)
            .build();

        createUser(lionUser, lionUser, new ArrayList<>(Arrays.asList("opensearch")));
        lionClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), lionUser, lionUser)
            .setSocketTimeout(60000)
            .build();

        createRoleMapping("anomaly_read_access", new ArrayList<>(Arrays.asList(bobUser)));
        createRoleMapping("anomaly_full_access", new ArrayList<>(Arrays.asList(aliceUser, catUser, dogUser, elkUser, fishUser, goatUser)));
        createRoleMapping(indexAllAccessRole, new ArrayList<>(Arrays.asList(aliceUser, bobUser, catUser, dogUser, fishUser, lionUser)));
        createRoleMapping(indexSearchAccessRole, new ArrayList<>(Arrays.asList(goatUser)));
    }

    @After
    public void deleteUserSetup() throws IOException {
        aliceClient.close();
        bobClient.close();
        catClient.close();
        dogClient.close();
        elkClient.close();
        fishClient.close();
        goatClient.close();
        lionClient.close();
        deleteUser(aliceUser);
        deleteUser(bobUser);
        deleteUser(catUser);
        deleteUser(dogUser);
        deleteUser(elkUser);
        deleteUser(fishUser);
        deleteUser(goatUser);
        deleteUser(lionUser);
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
        Exception exception = expectThrows(
            IOException.class,
            () -> { startAnomalyDetector(aliceDetector.getDetectorId(), null, bobClient); }
        );
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/jobmanagement]"));
    }

    public void testStartDetectorForWriteUser() throws IOException {
        // User Alice has AD full access, should be able to modify a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Assert.assertNotNull(aliceDetector.getDetectorId());
        Instant now = Instant.now();
        Response response = startAnomalyDetector(
            aliceDetector.getDetectorId(),
            new DetectionDateRange(now.minus(10, ChronoUnit.DAYS), now),
            aliceClient
        );
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

    public void testUpdateApiFilterByEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Assert
            .assertArrayEquals(
                "Wrong user roles",
                new String[] { "odfe" },
                aliceDetector.getUser().getBackendRoles().toArray(new String[0])
            );
        AnomalyDetector newDetector = new AnomalyDetector(
            aliceDetector.getDetectorId(),
            aliceDetector.getVersion(),
            aliceDetector.getName(),
            randomAlphaOfLength(10),
            aliceDetector.getTimeField(),
            aliceDetector.getIndices(),
            aliceDetector.getFeatureAttributes(),
            aliceDetector.getFilterQuery(),
            aliceDetector.getDetectionInterval(),
            aliceDetector.getWindowDelay(),
            aliceDetector.getShingleSize(),
            aliceDetector.getUiMetadata(),
            aliceDetector.getSchemaVersion(),
            Instant.now(),
            aliceDetector.getCategoryField(),
            new User(
                randomAlphaOfLength(5),
                ImmutableList.of("odfe", randomAlphaOfLength(5)),
                ImmutableList.of(randomAlphaOfLength(5)),
                ImmutableList.of(randomAlphaOfLength(5))
            ),
            null
        );
        enableFilterBy();
        // User Fish has AD full access, and has "odfe" backend role which is one of Alice's backend role, so
        // Fish should be able to update detectors created by Alice. But the detector's backend role should
        // not be replaced as Fish's backend roles.
        Response response = updateAnomalyDetector(aliceDetector.getDetectorId(), newDetector, fishClient);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        AnomalyDetector anomalyDetector = getAnomalyDetector(aliceDetector.getDetectorId(), aliceClient);
        Assert
            .assertArrayEquals(
                "Wrong user roles",
                new String[] { "odfe" },
                anomalyDetector.getUser().getBackendRoles().toArray(new String[0])
            );
    }

    public void testStartApiFilterByEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Instant now = Instant.now();
        Exception exception = expectThrows(
            IOException.class,
            () -> {
                startAnomalyDetector(aliceDetector.getDetectorId(), new DetectionDateRange(now.minus(10, ChronoUnit.DAYS), now), catClient);
            }
        );
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
        Exception exception = expectThrows(
            IOException.class,
            () -> { stopAnomalyDetector(aliceDetector.getDetectorId(), catClient, true); }
        );
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

    public void testCreateAnomalyDetectorWithCustomResultIndex() throws IOException {
        // User alice has AD full access and index permission, so can create detector
        AnomalyDetector anomalyDetector = createRandomAnomalyDetector(false, false, aliceClient);
        // User elk has AD full access, but has no read permission of index

        String resultIndex = CommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        AnomalyDetector detector = cloneDetector(anomalyDetector, resultIndex);
        // User goat has no permission to create index
        Exception exception = expectThrows(IOException.class, () -> { createAnomalyDetector(detector, true, goatClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [indices:admin/create]"));

        // User cat has permission to create index
        resultIndex = CommonName.CUSTOM_RESULT_INDEX_PREFIX + "test2";
        AnomalyDetector detectorOfCat = createAnomalyDetector(cloneDetector(anomalyDetector, resultIndex), true, catClient);
        assertEquals(resultIndex, detectorOfCat.getResultIndex());
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
        Assert.assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
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

    public void testValidateAnomalyDetectorWithWriteAccess() throws IOException {
        // User Alice has AD full access, should be able to validate a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Response validateResponse = validateAnomalyDetector(aliceDetector, aliceClient);
        Assert.assertNotNull("User alice validatedDetector", validateResponse);
    }

    public void testValidateAnomalyDetectorWithNoADAccess() throws IOException {
        // User Lion has no AD access at all, should not be able to validate a detector
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        Exception exception = expectThrows(IOException.class, () -> { validateAnomalyDetector(detector, lionClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/validate]"));

    }

    public void testValidateAnomalyDetectorWithReadAccess() throws IOException {
        // User Bob has AD read access, should still be able to validate a detector
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        Response validateResponse = validateAnomalyDetector(detector, bobClient);
        Assert.assertNotNull("User bob validatedDetector", validateResponse);
    }

    public void testValidateAnomalyDetectorWithNoReadPermissionOfIndex() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        enableFilterBy();
        // User elk has no read permission of index, can't validate detector
        Exception exception = expectThrows(Exception.class, () -> { validateAnomalyDetector(detector, elkClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [indices:data/read/search]"));
    }

    public void testValidateAnomalyDetectorWithNoBackendRole() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        enableFilterBy();
        // User Dog has AD full access, but has no backend role
        // When filter by is enabled, we block validating Detectors
        Exception exception = expectThrows(IOException.class, () -> { validateAnomalyDetector(detector, dogClient); });
        Assert
            .assertTrue(
                exception.getMessage().contains("Filter by backend roles is enabled and User dog does not have backend roles configured")
            );
    }
}
