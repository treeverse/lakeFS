/*
 * lakeFS API
 * lakeFS HTTP API
 *
 * The version of the OpenAPI document: 0.1.0
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package io.lakefs.clients.sdk;

import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.model.AuthCapabilities;
import io.lakefs.clients.sdk.model.CommPrefsInput;
import io.lakefs.clients.sdk.model.CredentialsWithSecret;
import io.lakefs.clients.sdk.model.Error;
import io.lakefs.clients.sdk.model.Setup;
import io.lakefs.clients.sdk.model.SetupState;
import io.lakefs.clients.sdk.model.StatsEventsList;
import io.lakefs.clients.sdk.model.StorageURI;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for InternalApi
 */
@Disabled
public class InternalApiTest {

    private final InternalApi api = new InternalApi();

    /**
     * @throws ApiException if the Api call fails
     */
    @Test
    public void createBranchProtectionRulePreflightTest() throws ApiException {
        String repository = null;
        api.createBranchProtectionRulePreflight(repository)
                .execute();
        // TODO: test validations
    }

    /**
     * creates symlink files corresponding to the given directory
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void createSymlinkFileTest() throws ApiException {
        String repository = null;
        String branch = null;
        String location = null;
        StorageURI response = api.createSymlinkFile(repository, branch)
                .location(location)
                .execute();
        // TODO: test validations
    }

    /**
     * list authentication capabilities supported
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void getAuthCapabilitiesTest() throws ApiException {
        AuthCapabilities response = api.getAuthCapabilities()
                .execute();
        // TODO: test validations
    }

    /**
     * check if the lakeFS installation is already set up
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void getSetupStateTest() throws ApiException {
        SetupState response = api.getSetupState()
                .execute();
        // TODO: test validations
    }

    /**
     * post stats events, this endpoint is meant for internal use only
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void postStatsEventsTest() throws ApiException {
        StatsEventsList statsEventsList = null;
        api.postStatsEvents(statsEventsList)
                .execute();
        // TODO: test validations
    }

    /**
     * @throws ApiException if the Api call fails
     */
    @Test
    public void setGarbageCollectionRulesPreflightTest() throws ApiException {
        String repository = null;
        api.setGarbageCollectionRulesPreflight(repository)
                .execute();
        // TODO: test validations
    }

    /**
     * setup lakeFS and create a first user
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void setupTest() throws ApiException {
        Setup setup = null;
        CredentialsWithSecret response = api.setup(setup)
                .execute();
        // TODO: test validations
    }

    /**
     * setup communications preferences
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void setupCommPrefsTest() throws ApiException {
        CommPrefsInput commPrefsInput = null;
        api.setupCommPrefs(commPrefsInput)
                .execute();
        // TODO: test validations
    }

    /**
     * @throws ApiException if the Api call fails
     */
    @Test
    public void uploadObjectPreflightTest() throws ApiException {
        String repository = null;
        String branch = null;
        String path = null;
        api.uploadObjectPreflight(repository, branch, path)
                .execute();
        // TODO: test validations
    }

}