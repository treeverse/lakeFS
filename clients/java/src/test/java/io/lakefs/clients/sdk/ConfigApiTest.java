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
import io.lakefs.clients.sdk.model.Error;
import io.lakefs.clients.sdk.model.GarbageCollectionConfig;
import io.lakefs.clients.sdk.model.StorageConfig;
import io.lakefs.clients.sdk.model.VersionConfig;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for ConfigApi
 */
@Disabled
public class ConfigApiTest {

    private final ConfigApi api = new ConfigApi();

    /**
     * get information of gc settings
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void getGarbageCollectionConfigTest() throws ApiException {
        GarbageCollectionConfig response = api.getGarbageCollectionConfig()
                .execute();
        // TODO: test validations
    }

    /**
     * get version of lakeFS server
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void getLakeFSVersionTest() throws ApiException {
        VersionConfig response = api.getLakeFSVersion()
                .execute();
        // TODO: test validations
    }

    /**
     * retrieve lakeFS storage configuration
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void getStorageConfigTest() throws ApiException {
        StorageConfig response = api.getStorageConfig()
                .execute();
        // TODO: test validations
    }

}