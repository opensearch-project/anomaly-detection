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
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
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

    /**
     * Create an unguessable password. Simple password are weak due to https://tinyurl.com/383em9zk
     * @return a random password.
     */
    public static String generatePassword(String username) {
        String upperCase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lowerCase = "abcdefghijklmnopqrstuvwxyz";
        String digits = "0123456789";
        String special = "_";

        // Remove characters from username (case-insensitive)
        String usernameLower = username.toLowerCase(Locale.ROOT);
        for (char c : usernameLower.toCharArray()) {
            upperCase = upperCase.replaceAll("(?i)" + c, "");
            lowerCase = lowerCase.replaceAll("(?i)" + c, "");
            digits = digits.replace(String.valueOf(c), "");
            special = special.replace(String.valueOf(c), "");
        }

        // Combine all remaining characters
        String characters = upperCase + lowerCase + digits + special;

        // Check if we have enough characters to proceed
        if (characters.length() < 4) {
            throw new IllegalArgumentException("Not enough characters to generate password without using username characters.");
        }

        SecureRandom rng = new SecureRandom();
        String password;

        do {
            // Ensure password includes at least one character from each set, if available
            StringBuilder passwordBuilder = new StringBuilder();
            if (!upperCase.isEmpty()) {
                passwordBuilder.append(upperCase.charAt(rng.nextInt(upperCase.length())));
            }
            if (!lowerCase.isEmpty()) {
                passwordBuilder.append(lowerCase.charAt(rng.nextInt(lowerCase.length())));
            }
            if (!digits.isEmpty()) {
                passwordBuilder.append(digits.charAt(rng.nextInt(digits.length())));
            }
            if (!special.isEmpty()) {
                passwordBuilder.append(special.charAt(rng.nextInt(special.length())));
            }

            // Fill the rest of the password length with random characters
            int remainingLength = 15 - passwordBuilder.length();
            for (int i = 0; i < remainingLength; i++) {
                passwordBuilder.append(characters.charAt(rng.nextInt(characters.length())));
            }

            // Convert to char array for shuffling
            char[] passwordChars = passwordBuilder.toString().toCharArray();

            // Shuffle the password characters
            for (int i = passwordChars.length - 1; i > 0; i--) {
                int index = rng.nextInt(i + 1);
                char temp = passwordChars[index];
                passwordChars[index] = passwordChars[i];
                passwordChars[i] = temp;
            }

            password = new String(passwordChars);

            // Repeat if password contains the username as a substring (case-insensitive)
        } while (password.toLowerCase(Locale.ROOT).contains(usernameLower.toLowerCase(Locale.ROOT)));

        return password;
    }

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
        createUser(oceanUser, elkPassword, new ArrayList<>(Arrays.asList("odfe")));
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

    public void testCreateAnomalyDetectorWithWriteAccess() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Assert.assertNotNull("User alice could not create detector", aliceDetector.getId());
    }

    public void testCreateAnomalyDetectorWithReadAccess() {
        // User Bob has AD read access, should not be able to create a detector
        Exception exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, bobClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]"));
    }

    public void testStartDetectorWithReadAccess() throws IOException {
        // User Bob has AD read access, should not be able to modify a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Assert.assertNotNull(aliceDetector.getId());
        Exception exception = expectThrows(IOException.class, () -> { startAnomalyDetector(aliceDetector.getId(), null, bobClient); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/jobmanagement]"));
    }

    public void testStartDetectorForWriteUser() throws IOException {
        // User Alice has AD full access, should be able to modify a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Assert.assertNotNull(aliceDetector.getId());
        Instant now = Instant.now();
        Response response = startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), aliceClient);
        MatcherAssert.assertThat(response.getStatusLine().toString(), CoreMatchers.containsString("200 OK"));
    }

    public void testFilterByDisabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        // User Cat has AD full access, should be able to get a detector
        AnomalyDetector detector = getConfig(aliceDetector.getId(), catClient);
        Assert.assertEquals(aliceDetector.getId(), detector.getId());
    }

    public void testGetApiFilterByEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { getConfig(aliceDetector.getId(), catClient); });
        Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));
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

    public void testGetApiFilterByEnabledForAdmin() throws IOException {
        // User Alice has AD full access, should be able to create a detector and has backend role "odfe"
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        enableFilterBy();
        confirmingClientIsAdmin();
        AnomalyDetector detector = getConfig(aliceDetector.getId(), client());
        Assert
            .assertArrayEquals(
                "User backend role of detector doesn't change",
                new String[] { "odfe" },
                detector.getUser().getBackendRoles().toArray(new String[0])
            );
    }

    public void testUpdateApiFilterByEnabledForAdmin() throws IOException {
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
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now()
        );
        // User client has admin all access, and has "opensearch" backend role so client should be able to update detector
        // But the detector's backend role should not be replaced as client's backend roles (all_access).
        Response response = updateAnomalyDetector(aliceDetector.getId(), newDetector, client());
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        AnomalyDetector anomalyDetector = getConfig(aliceDetector.getId(), aliceClient);
        Assert
            .assertArrayEquals(
                "odfe is still the backendrole, not opensearch",
                new String[] { "odfe" },
                anomalyDetector.getUser().getBackendRoles().toArray(new String[0])
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
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            null,
            null,
            null,
            null,
            Instant.now()
        );
        enableFilterBy();
        // User Fish has AD full access, and has "odfe" backend role which is one of Alice's backend role, so
        // Fish should be able to update detectors created by Alice. But the detector's backend role should
        // not be replaced as Fish's backend roles.
        Response response = updateAnomalyDetector(aliceDetector.getId(), newDetector, fishClient);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        AnomalyDetector anomalyDetector = getConfig(aliceDetector.getId(), aliceClient);
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
        Exception exception = expectThrows(IOException.class, () -> {
            startAnomalyDetector(aliceDetector.getId(), new DateRange(now.minus(10, ChronoUnit.DAYS), now), catClient);
        });
        Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));
    }

    public void testStopApiFilterByEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { stopAnomalyDetector(aliceDetector.getId(), catClient, true); });
        Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));
    }

    public void testDeleteApiFilterByEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { deleteAnomalyDetector(aliceDetector.getId(), catClient); });
        Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));
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
        Exception exception = expectThrows(IOException.class, () -> { createRandomAnomalyDetector(false, false, indexName, oceanClient); });
        Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains("Unauthorized"));
    }

    public void testCreateAnomalyDetectorWithCustomResultIndex() throws IOException {
        // User alice has AD full access and index permission, so can create detector
        AnomalyDetector anomalyDetector = createRandomAnomalyDetector(false, false, aliceClient);
        // User elk has AD full access, but has no read permission of index

        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test";
        AnomalyDetector detector = cloneDetector(anomalyDetector, resultIndex);
        // User goat has no permission to create index
        Exception exception = expectThrows(IOException.class, () -> { createAnomalyDetector(detector, true, goatClient); });
        Assert
            .assertTrue(
                "got " + exception.getMessage(),
                exception.getMessage().contains("no permissions for [indices:admin/aliases, indices:admin/create]")
            );

        // User cat has permission to create index
        resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "test2";
        TestHelpers.createIndexWithTimeField(client(), anomalyDetector.getIndices().get(0), anomalyDetector.getTimeField());
        AnomalyDetector detectorOfCat = createAnomalyDetector(cloneDetector(anomalyDetector, resultIndex), true, catClient);
        assertEquals(resultIndex, detectorOfCat.getCustomResultIndexOrAlias());
    }

    public void testPreviewAnomalyDetectorWithWriteAccess() throws IOException {
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
        Exception exception = expectThrows(IOException.class, () -> { previewAnomalyDetector(aliceDetector.getId(), bobClient, input); });
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/preview]"));
    }

    public void testPreviewAnomalyDetectorWithFilterEnabled() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            aliceDetector.getId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            null
        );
        enableFilterBy();
        // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
        // Alice detector
        Exception exception = expectThrows(IOException.class, () -> { previewAnomalyDetector(aliceDetector.getId(), catClient, input); });
        Assert.assertTrue(exception.getMessage().contains("User does not have permissions to access config: " + aliceDetector.getId()));
    }

    public void testPreviewAnomalyDetectorWithNoReadPermissionOfIndex() throws IOException {
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        AnomalyDetectorExecutionInput input = new AnomalyDetectorExecutionInput(
            aliceDetector.getId(),
            Instant.now().minusSeconds(60 * 10),
            Instant.now(),
            aliceDetector
        );
        enableFilterBy();
        // User elk has no read permission of index
        Exception exception = expectThrows(Exception.class, () -> { previewAnomalyDetector(aliceDetector.getId(), oceanClient, input); });
        Assert.assertTrue("actual msg: " + exception.getMessage(), exception.getMessage().contains("Unauthorized"));
    }

    public void testValidateAnomalyDetectorWithWriteAccess() throws IOException {
        // User Alice has AD full access, should be able to validate a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        Response validateResponse = validateAnomalyDetector(aliceDetector, aliceClient);
        Assert.assertNotNull("User alice validated detector successfully", validateResponse);
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
        Assert.assertNotNull("User bob validated detector successfully", validateResponse);
    }

    public void testValidateAnomalyDetectorWithNoReadPermissionOfIndex() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        enableFilterBy();
        // User elk has no read permission of index, can't validate detector
        Exception exception = expectThrows(Exception.class, () -> { validateAnomalyDetector(detector, oceanClient); });
        Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains("Unauthorized"));
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
