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
import io.lakefs.clients.sdk.model.GarbageCollectionPrepareResponse;
import io.lakefs.clients.sdk.model.PrepareGCUncommittedRequest;
import io.lakefs.clients.sdk.model.PrepareGCUncommittedResponse;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for RetentionApi
 */
@Disabled
public class RetentionApiTest {

    private final RetentionApi api = new RetentionApi();

    /**
     * save lists of active commits for garbage collection
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void prepareGarbageCollectionCommitsTest() throws ApiException {
        String repository = null;
        GarbageCollectionPrepareResponse response = api.prepareGarbageCollectionCommits(repository)
                .execute();
        // TODO: test validations
    }

    /**
     * save repository uncommitted metadata for garbage collection
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void prepareGarbageCollectionUncommittedTest() throws ApiException {
        String repository = null;
        PrepareGCUncommittedRequest prepareGCUncommittedRequest = null;
        PrepareGCUncommittedResponse response = api.prepareGarbageCollectionUncommitted(repository)
                .prepareGCUncommittedRequest(prepareGCUncommittedRequest)
                .execute();
        // TODO: test validations
    }

}