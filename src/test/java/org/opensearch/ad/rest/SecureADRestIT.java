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
import java.util.Map;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.commons.authuser.User;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

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
    String oceanUser = "ocean";
    RestClient oceanClient;

    @Before
    public void setupSecureTests() throws IOException {
        if (!isHttps()) {
            throw new IllegalArgumentException("Secure Tests are running but HTTPS is not set");
        }
        createIndexRole(indexAllAccessRole, "*");
        createSearchRole(indexSearchAccessRole, "*");
        String alicePassword = generatePassword(aliceUser);
        createUser(aliceUser, alicePassword, new ArrayList<>(Arrays.asList("odfe")));
        aliceClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), aliceUser, alicePassword)
            .setSocketTimeout(60000)
            .build();

        String bobPassword = generatePassword(bobUser);
        createUser(bobUser, bobPassword, new ArrayList<>(Arrays.asList("odfe")));
        bobClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), bobUser, bobPassword)
            .setSocketTimeout(60000)
            .build();

        String catPassword = generatePassword(catUser);
        createUser(catUser, catPassword, new ArrayList<>(Arrays.asList("aes")));
        catClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), catUser, catPassword)
            .setSocketTimeout(60000)
            .build();

        String dogPassword = generatePassword(dogUser);
        createUser(dogUser, dogPassword, new ArrayList<>(Arrays.asList()));
        dogClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), dogUser, dogPassword)
            .setSocketTimeout(60000)
            .build();

        String elkPassword = generatePassword(elkUser);
        createUser(elkUser, elkPassword, new ArrayList<>(Arrays.asList("odfe")));
        elkClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), elkUser, elkPassword)
            .setSocketTimeout(60000)
            .build();

        String fishPassword = generatePassword(fishUser);
        createUser(fishUser, fishPassword, new ArrayList<>(Arrays.asList("odfe", "aes")));
        fishClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), fishUser, fishPassword)
            .setSocketTimeout(60000)
            .build();

        String goatPassword = generatePassword(goatUser);
        createUser(goatUser, goatPassword, new ArrayList<>(Arrays.asList("opensearch")));
        goatClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), goatUser, goatPassword)
            .setSocketTimeout(60000)
            .build();

        String lionPassword = generatePassword(lionUser);
        createUser(lionUser, lionPassword, new ArrayList<>(Arrays.asList("opensearch")));
        lionClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), lionUser, lionPassword)
            .setSocketTimeout(60000)
            .build();

        String oceanPassword = generatePassword(oceanUser);
        createUser(oceanUser, oceanPassword, new ArrayList<>(Arrays.asList("odfe")));
        oceanClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), oceanUser, oceanPassword)
            .setSocketTimeout(60000)
            .build();

        createRoleMapping("anomaly_read_access", new ArrayList<>(Arrays.asList(bobUser, oceanUser)));
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
        oceanClient.close();
        deleteUser(aliceUser);
        deleteUser(bobUser);
        deleteUser(catUser);
        deleteUser(dogUser);
        deleteUser(elkUser);
        deleteUser(fishUser);
        deleteUser(goatUser);
        deleteUser(lionUser);
        deleteUser(oceanUser);
    }

    private void confirmingClientIsAdmin() throws IOException {
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "GET",
                "_plugins/_security/api/account",
                null,
                "",
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "admin"))
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        ArrayList<String> roles = (ArrayList<String>) responseMap.get("roles");
        assertTrue(roles.contains("all_access"));
    }

    public void testCreateAnomalyDetector() throws IOException {

        // 1. NO Backend_role

        enableFilterBy();
        // User Dog has AD full access, but has no backend role
        // When filter by is enabled, we block creating Detectors
        Exception exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, dogClient); });
        Assert
            .assertTrue(
                exception.getMessage().contains("Filter by backend roles is enabled and User dog does not have backend roles configured")
            );

        disableFilterBy();
        // When filter by is enabled, we allow creating Detector
        AnomalyDetector detectorOfDog = createRandomAnomalyDetector(false, false, dogClient);
        assertNotNull(detectorOfDog.getId());

        // 2. No read permission on index

        // 2.1
        enableFilterBy();
        // User alice has AD full access and index permission, so can create detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        // User ocean has AD full access, but has no write permission of index
        String indexName = aliceDetector.getIndices().getFirst();
        exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, indexName, oceanClient); });
        Assert
            .assertTrue(
                "actual: " + exception.getMessage(),
                exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]")
            );
        // User Bob has AD read access, should not be able to create a detector
        exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, bobClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]"));

        // 2.2
        disableFilterBy();
        // User Ocean is not allowed to create detector without write permission
        exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, indexName, oceanClient); });
        Assert
            .assertTrue(
                "actual: " + exception.getMessage(),
                exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]")
            );

        // User Bob has AD read access, should not be able to create a detector
        exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, bobClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]"));

        // 3. With Custom Result Index
        // 3.1
        enableFilterBy();
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        AnomalyDetector detector = cloneDetector(aliceDetector, resultIndex);
        // User goat doesn't have permission to create index
        exception = expectThrows(IOException.class, () -> { createAnomalyDetector(detector, true, goatClient); });
        Assert
            .assertTrue(
                "got " + exception.getMessage(),
                exception.getMessage().contains("indices:admin/aliases")
                    && exception.getMessage().contains("indices:admin/create")
                    && exception.getMessage().contains("no permissions for")
            );

        // User cat has permission to create index
        resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test2";
        TestHelpers.createIndexWithTimeField(client(), aliceDetector.getIndices().getFirst(), aliceDetector.getTimeField());
        AnomalyDetector detectorOfCat = createAnomalyDetector(cloneDetector(aliceDetector, resultIndex), true, catClient);
        assertEquals(resultIndex, detectorOfCat.getCustomResultIndexOrAlias());

        // 3.2
        disableFilterBy();
        // User goat doesn't have permission to create index
        exception = expectThrows(IOException.class, () -> { createAnomalyDetector(detector, true, goatClient); });
        Assert
            .assertTrue(
                "got " + exception.getMessage(),
                exception.getMessage().contains("indices:admin/aliases")
                    && exception.getMessage().contains("indices:admin/create")
                    && exception.getMessage().contains("no permissions for")
            );

        // User cat has permission to create index
        resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test2";
        TestHelpers.createIndexWithTimeField(client(), aliceDetector.getIndices().getFirst(), aliceDetector.getTimeField());
        AnomalyDetector detectorOfCat2 = createAnomalyDetector(cloneDetector(aliceDetector, resultIndex), true, catClient);
        assertEquals(resultIndex, detectorOfCat2.getCustomResultIndexOrAlias());
    }

    public void testGetDetector() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);

        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { getConfig(aliceDetector.getId(), catClient); });
        assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

        disableFilterBy();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // User Cat has AD full access, is part of different backend role but filter by is disabled, Cat can access Alice's detector
        AnomalyDetector detector = getConfig(aliceDetector.getId(), catClient);
        assertEquals(detector.getId(), aliceDetector.getId());

        enableFilterBy();
        confirmingClientIsAdmin();
        detector = getConfig(aliceDetector.getId(), client());
        Assert
            .assertArrayEquals(
                "User backend role of detector doesn't change",
                new String[] { "odfe" },
                detector.getUser().getBackendRoles().toArray(new String[0])
            );
    }

    public void testUpdateDetector() throws IOException {
        // User Alice has AD full access, should be able to create a detector and has backend role "odfe"
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);

        enableFilterBy();

        AnomalyDetector newDetector = new AnomalyDetector(
            aliceDetector.getId(),
            aliceDetector.getVersion(),
            aliceDetector.getName(),
            randomAlphaOfLength(10),
            aliceDetector.getTimeField(),
            aliceDetector.getIndices(),
            aliceDetector.getFeatureAttributes(),
            aliceDetector.getFilterQuery(),
            aliceDetector.getInterval(),
            aliceDetector.getWindowDelay(),
            aliceDetector.getShingleSize(),
            aliceDetector.getUiMetadata(),
            aliceDetector.getSchemaVersion(),
            Instant.now(),
            aliceDetector.getCategoryFields(),
            new User(
                randomAlphaOfLength(5),
                ImmutableList.of("odfe", randomAlphaOfLength(5)),
                ImmutableList.of(randomAlphaOfLength(5)),
                ImmutableList.of(randomAlphaOfLength(5))
            ),
            null,
            aliceDetector.getImputationOption(),
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now(),
            aliceDetector.getFrequency()
        );

        // User client has admin all access, and has "opensearch" backend role so client should be able to update detector
        // But the detector's backend role should not be replaced as client's backend roles (all_access).
        Response response = updateAnomalyDetector(aliceDetector.getId(), newDetector, client());
        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        AnomalyDetector anomalyDetector = getConfig(aliceDetector.getId(), client());
        Assert
            .assertArrayEquals(
                "odfe is still the backendrole, not opensearch",
                new String[] { "odfe" },
                anomalyDetector.getUser().getBackendRoles().toArray(new String[0])
            );

        // User Fish has AD full access, and has "odfe" backend role which is one of Alice's backend role, so
        // Fish should be able to update detectors created by Alice. But the detector's backend role should
        // not be replaced as Fish's backend roles.
        response = updateAnomalyDetector(aliceDetector.getId(), newDetector, fishClient);
        assertEquals(200, response.getStatusLine().getStatusCode());
        anomalyDetector = getConfig(aliceDetector.getId(), fishClient);
        Assert
            .assertArrayEquals(
                "Wrong user roles",
                new String[] { "odfe" },
                anomalyDetector.getUser().getBackendRoles().toArray(new String[0])
            );

        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(
            IOException.class,
            () -> { updateAnomalyDetector(aliceDetector.getId(), newDetector, catClient); }
        );
        assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

        disableFilterBy();
        // User Cat has AD full access, is part of different backend role but filter by is disabled, Cat can access Alice's detector
        // But the detector's backend role should not be replaced as Cat's backend roles (all_access).
        response = updateAnomalyDetector(aliceDetector.getId(), newDetector, catClient);
        assertEquals(200, response.getStatusLine().getStatusCode());
        anomalyDetector = getConfig(aliceDetector.getId(), catClient);
        Assert
            .assertArrayEquals(
                "Wrong user roles",
                new String[] { "odfe" },
                anomalyDetector.getUser().getBackendRoles().toArray(new String[0])
            );
    }

    public void testStartAndStopDetector() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);

        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        final Instant now = Instant.now();
        Exception exception = expectThrows(IOException.class, () -> {
            startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), catClient);
        });
        Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

        // User Bob has AD read access, should not be able to modify a detector
        Assert.assertNotNull(aliceDetector.getId());
        exception = expectThrows(IOException.class, () -> { startAnomalyDetector(aliceDetector.getId(), null, bobClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/jobmanagement]"));

        // User elk shares backend_role with alice and has AD full access, it should be able to modify alice's detector
        Response response = startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), elkClient);
        MatcherAssert.assertThat(response.getStatusLine().toString(), CoreMatchers.containsString("200 OK"));
        // User elk should also be able to stop detector
        response = stopAnomalyDetector(aliceDetector.getId(), elkClient, true);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // User Cat has AD full access, but is part of different backend role so Cat should not be able to stop
        // Alice detector
        exception = expectThrows(IOException.class, () -> { stopAnomalyDetector(aliceDetector.getId(), catClient, true); });
        Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

        disableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        response = startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), catClient);
        assertEquals(200, response.getStatusLine().getStatusCode());
        // User Cat should also be able to stop Alice's detector
        response = stopAnomalyDetector(aliceDetector.getId(), catClient, true);
        assertEquals(200, response.getStatusLine().getStatusCode());

    }

    public void testPreviewAnomalyDetector() throws IOException {
        // User Alice has AD full access, should be able to create/preview a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            aliceDetector.getId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );

        Response response = previewAnomalyDetector(aliceDetector.getId(), aliceClient, input);
        Assert.assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { previewAnomalyDetector(aliceDetector.getId(), catClient, input); });
        Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

        // User bob has AD read access, should not be able to preview a detector
        exception = expectThrows(IOException.class, () -> { previewAnomalyDetector(aliceDetector.getId(), bobClient, input); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/preview]"));

        // User ocean has no read permission of index
        exception = expectThrows(Exception.class, () -> { previewAnomalyDetector(aliceDetector.getId(), oceanClient, input); });
        Assert
            .assertTrue(
                "actual msg: " + exception.getMessage(),
                exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/preview]")
            );

        disableFilterBy();
        // User Cat has AD full access, is part of different backend_role, but filter_by is disabled so Cat should be able to preview
        // Alice detector
        response = previewAnomalyDetector(aliceDetector.getId(), catClient, input);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    public void testValidateAnomalyDetector() throws IOException {
        // User Alice has AD full access, should be able to validate a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Response validateResponse = validateAnomalyDetector(aliceDetector, aliceClient);
        Assert.assertNotNull("User alice validated detector successfully", validateResponse);

        // User Lion has no AD access at all, should not be able to validate a detector
        Exception exception = expectThrows(IOException.class, () -> { validateAnomalyDetector(aliceDetector, lionClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/validate]"));

        // User Bob has AD read access, should still be able to validate a detector
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        validateResponse = validateAnomalyDetector(detector, bobClient);
        Assert.assertNotNull("User bob validated detector successfully", validateResponse);

        // User ocean has no read permission of index, can't validate detector
        exception = expectThrows(Exception.class, () -> { validateAnomalyDetector(detector, oceanClient); });
        Assert
            .assertTrue(
                "actual: " + exception.getMessage(),
                exception.getMessage().contains("no permissions for [indices:data/read/search]")
            );

        enableFilterBy();
        // User Dog has AD full access, but has no backend role
        // When filter by is enabled, we block validating Detectors
        exception = expectThrows(IOException.class, () -> { validateAnomalyDetector(detector, dogClient); });
        Assert
            .assertTrue(
                exception.getMessage().contains("Filter by backend roles is enabled and User dog does not have backend roles configured")
            );

        disableFilterBy();
        // User Dog has AD full access, but has no backend role
        // When filter by is disabled, we allow validating Detectors
        validateResponse = validateAnomalyDetector(detector, dogClient);
        Assert.assertNotNull("User dog validated detector successfully", validateResponse);
    }

    public void testDeleteDetector() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);

        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { deleteAnomalyDetector(aliceDetector.getId(), catClient); });
        Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));

        disableFilterBy();
        // User Cat has AD full access, should be able to access
        // Alice detector
        Response response = deleteAnomalyDetector(aliceDetector.getId(), catClient);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

}
