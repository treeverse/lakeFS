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
import io.lakefs.clients.sdk.model.CommitList;
import io.lakefs.clients.sdk.model.DiffList;
import io.lakefs.clients.sdk.model.Error;
import io.lakefs.clients.sdk.model.FindMergeBaseResult;
import io.lakefs.clients.sdk.model.Merge;
import io.lakefs.clients.sdk.model.MergeResult;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API tests for RefsApi
 */
@Disabled
public class RefsApiTest {

    private final RefsApi api = new RefsApi();

    /**
     * diff references
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void diffRefsTest() throws ApiException {
        String repository = null;
        String leftRef = null;
        String rightRef = null;
        String after = null;
        Integer amount = null;
        String prefix = null;
        String delimiter = null;
        String type = null;
        Boolean includeRightStats = null;
        DiffList response = api.diffRefs(repository, leftRef, rightRef)
                .after(after)
                .amount(amount)
                .prefix(prefix)
                .delimiter(delimiter)
                .type(type)
                .includeRightStats(includeRightStats)
                .execute();
        // TODO: test validations
    }

    /**
     * find the merge base for 2 references
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void findMergeBaseTest() throws ApiException {
        String repository = null;
        String sourceRef = null;
        String destinationBranch = null;
        FindMergeBaseResult response = api.findMergeBase(repository, sourceRef, destinationBranch)
                .execute();
        // TODO: test validations
    }

    /**
     * get commit log from ref. If both objects and prefixes are empty, return all commits.
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void logCommitsTest() throws ApiException {
        String repository = null;
        String ref = null;
        String after = null;
        Integer amount = null;
        List<String> objects = null;
        List<String> prefixes = null;
        Boolean limit = null;
        Boolean firstParent = null;
        OffsetDateTime since = null;
        String stopAt = null;
        CommitList response = api.logCommits(repository, ref)
                .after(after)
                .amount(amount)
                .objects(objects)
                .prefixes(prefixes)
                .limit(limit)
                .firstParent(firstParent)
                .since(since)
                .stopAt(stopAt)
                .execute();
        // TODO: test validations
    }

    /**
     * merge references
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void mergeIntoBranchTest() throws ApiException {
        String repository = null;
        String sourceRef = null;
        String destinationBranch = null;
        Merge merge = null;
        MergeResult response = api.mergeIntoBranch(repository, sourceRef, destinationBranch)
                .merge(merge)
                .execute();
        // TODO: test validations
    }

}
