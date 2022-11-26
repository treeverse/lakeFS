package io.lakefs.utils;

import io.lakefs.LakeFSClient;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.model.ObjectStageCreation;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStatsList;
import io.lakefs.clients.api.model.Pagination;
import io.lakefs.clients.api.model.PathList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class BulkTest {
    protected Bulk bulk;

    protected LakeFSClient lfsClient;
    protected ObjectsApi objects;

    protected final String SCHEME = "pondfs";
    protected final String REPO = "repo";
    protected final String REF = "one";

    protected final ObjectStats stats1 = new ObjectStats()
        .path("foo/bar/_temp/task/baz/zoo")
        .physicalAddress("s3://zoo")
        .checksum("1")
        .sizeBytes(11L)
        .mtime(5432L)
        .metadata(null)
        .contentType("text/plain");
    protected final ObjectStats stats2 = new ObjectStats()
        .path("foo/bar/_temp/task/quux/boo")
        .physicalAddress("s3://garden")
        .checksum("2")
        .sizeBytes(22L)
        .mtime(54321L)
        .metadata(null)
        .contentType("text/complex");
    protected final ObjectStats stats3 = new ObjectStats()
        .path("foo/bar/_temp/task/quux/moo")
        .physicalAddress("s3://path")
        .checksum("3")
        .sizeBytes(33L)
        .mtime(543210L)
        .metadata(null)
        .contentType("x-app/app");

    protected static ObjectStageCreation creationFor(ObjectStats stats) {
        return new ObjectStageCreation()
            .physicalAddress(stats.getPhysicalAddress())
            .checksum(stats.getChecksum())
            .sizeBytes(stats.getSizeBytes())
            .mtime(stats.getMtime())
            .metadata(stats.getMetadata())
            .contentType(stats.getContentType());
    }

    @Before
    public void mockClient() {
        lfsClient = mock(LakeFSClient.class, Answers.RETURNS_SMART_NULLS);
        objects = mock(ObjectsApi.class, Answers.RETURNS_SMART_NULLS);
        when(lfsClient.getObjects()).thenReturn(objects);

        bulk = new Bulk(lfsClient);
    }

    protected void checkCopyPrefixSingleItemCase(String src, String dst) throws ApiException {
        when(objects.listObjects(REPO, REF, true, "", Bulk.BULK_SIZE, "", "foo/bar/_temp/task/"))
            .thenReturn(new ObjectStatsList()
                        .pagination(new Pagination().hasMore(false).nextOffset("unused"))
                        .results(ImmutableList.of(stats1)));
        bulk.copyPrefix(new ObjectLocation(SCHEME, REPO, REF, src),
                        new ObjectLocation(SCHEME, REPO, REF, dst));
        verify(objects).stageObject(REPO, REF, "foo/bar/baz/zoo", creationFor(stats1));
    }

    @Test
    public void copyPrefix() throws ApiException {
        checkCopyPrefixSingleItemCase("foo/bar/_temp/task/", "foo/bar/");
    }

    @Test
    public void copyPrefixSrcWithoutTrailingSlash() throws ApiException {
        checkCopyPrefixSingleItemCase("foo/bar/_temp/task", "foo/bar/");
    }

    @Test
    public void copyPrefixDstWithoutTrailingSlash() throws ApiException {
        checkCopyPrefixSingleItemCase("foo/bar/_temp/task/", "foo/bar");
    }

    @Test
    public void copyPrefixManyItemsListPaged() throws ApiException {
        String next1 = stats2.getPath() + "\0";
        ObjectStatsList osl1 = new ObjectStatsList()
            .pagination(new Pagination().hasMore(true).nextOffset(next1))
            .results(ImmutableList.of(stats1, stats2));
        when(objects.listObjects(REPO, REF, true, "", Bulk.BULK_SIZE, "", "foo/bar/_temp/task/"))
            .thenReturn(osl1);
        ObjectStatsList osl2 = new ObjectStatsList()
            .pagination(new Pagination().hasMore(false).nextOffset("unused"))
            .results(ImmutableList.of(stats3));
        when(objects.listObjects(REPO, REF, true, next1, Bulk.BULK_SIZE, "", "foo/bar/_temp/task/"))
            .thenReturn(osl2);
        bulk.copyPrefix(new ObjectLocation(SCHEME, REPO, REF, "foo/bar/_temp/task/"),
                        new ObjectLocation(SCHEME, REPO, REF, "foo/bar/"));
        verify(objects).stageObject(REPO, REF, "foo/bar/baz/zoo", creationFor(stats1));
        verify(objects).stageObject(REPO, REF, "foo/bar/quux/boo", creationFor(stats2));
        verify(objects).stageObject(REPO, REF, "foo/bar/quux/moo", creationFor(stats3));
    }

    @Test
    public void deletePrefix() throws ApiException {
        when(objects.listObjects(REPO, REF, false, "", Bulk.BULK_SIZE, "", "foo/bar/_temp/task/"))
            .thenReturn(new ObjectStatsList()
                        .pagination(new Pagination().hasMore(false).nextOffset("unused"))
                        .results(ImmutableList.of(stats1)));
        bulk.deletePrefix(new ObjectLocation(SCHEME, REPO, REF, "foo/bar/_temp/task/"));
        verify(objects).deleteObjects(REPO, REF, new PathList().
                                      paths(ImmutableList.of(stats1.getPath())));
    }

    @Test
    public void deletePrefixWithoutTrailingSlash() throws ApiException {
        when(objects.listObjects(REPO, REF, false, "", Bulk.BULK_SIZE, "", "foo/bar/_temp/task/"))
            .thenReturn(new ObjectStatsList()
                        .pagination(new Pagination().hasMore(false).nextOffset("unused"))
                        .results(ImmutableList.of(stats1)));
        bulk.deletePrefix(new ObjectLocation(SCHEME, REPO, REF, "foo/bar/_temp/task"));
        verify(objects).deleteObjects(REPO, REF, new PathList().
                                      paths(ImmutableList.of(stats1.getPath())));
    }

    @Test
    public void deletePrefixManyItemsListPaged() throws ApiException {
        String next1 = stats2.getPath() + "\0";
        ObjectStatsList osl1 = new ObjectStatsList()
            .pagination(new Pagination().hasMore(true).nextOffset(next1))
            .results(ImmutableList.of(stats1, stats2));
        when(objects.listObjects(REPO, REF, false, "", Bulk.BULK_SIZE, "", "foo/bar/_temp/task/"))
            .thenReturn(osl1);
        ObjectStatsList osl2 = new ObjectStatsList()
            .pagination(new Pagination().hasMore(false).nextOffset("unused"))
            .results(ImmutableList.of(stats3));
        when(objects.listObjects(REPO, REF, false, next1, Bulk.BULK_SIZE, "", "foo/bar/_temp/task/"))
            .thenReturn(osl2);
        bulk.deletePrefix(new ObjectLocation(SCHEME, REPO, REF, "foo/bar/_temp/task/"));
        verify(objects).deleteObjects(REPO, REF, new PathList().
                                      paths(ImmutableList.of(stats1.getPath(), stats2.getPath())));
        verify(objects).deleteObjects(REPO, REF, new PathList().
                                      paths(ImmutableList.of(stats3.getPath())));
    }
}
