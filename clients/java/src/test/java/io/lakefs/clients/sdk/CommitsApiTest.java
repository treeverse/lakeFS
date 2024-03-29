/*
 * lakeFS API
 * lakeFS HTTP API
 *
 * The version of the OpenAPI document: 1.0.0
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package io.lakefs.clients.sdk;

import io.lakefs.clients.sdk.ApiException;
import io.lakefs.clients.sdk.model.Commit;
import io.lakefs.clients.sdk.model.CommitCreation;
import io.lakefs.clients.sdk.model.Error;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for CommitsApi
 */
@Disabled
public class CommitsApiTest {

    private final CommitsApi api = new CommitsApi();

    /**
     * create commit
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void commitTest() throws ApiException {
        String repository = null;
        String branch = null;
        CommitCreation commitCreation = null;
        String sourceMetarange = null;
        Commit response = api.commit(repository, branch, commitCreation)
                .sourceMetarange(sourceMetarange)
                .execute();
        // TODO: test validations
    }

    /**
     * get commit
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void getCommitTest() throws ApiException {
        String repository = null;
        String commitId = null;
        Commit response = api.getCommit(repository, commitId)
                .execute();
        // TODO: test validations
    }

}
