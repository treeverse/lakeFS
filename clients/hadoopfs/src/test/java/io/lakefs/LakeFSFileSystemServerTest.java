package io.lakefs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lakefs.clients.api.*;
import io.lakefs.clients.api.model.*;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import io.lakefs.utils.ObjectLocation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

import org.hamcrest.core.StringContains;

import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.TimeToLive;
import org.mockserver.matchers.Times;
import org.mockserver.model.Cookie;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;

import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LakeFSFileSystemServerTest extends FSTestBase {
    static private final Logger LOG = LoggerFactory.getLogger(LakeFSFileSystemServerTest.class);

    protected String objectLocToS3ObjKey(ObjectLocation objectLoc) {
        return String.format("/%s/%s/%s",objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
    }

    @Test
    public void getUri() {
        URI u = fs.getUri();
        Assert.assertNotNull(u);
    }

    @Test
    public void testUnknownProperties() throws IOException {
        // Verify that a response with unknown properties is still parsed.
        // This allows backwards compatibility: old clients can work with
        // new servers.  It tests that the OpenAPI codegen gave the correct
        // result, but this is still important.
        //
        // TODO(ariels): This test is unrelated to LakeFSFileSystem.  it
        // should not be part of LakeFSFileSystemTest.
        Path path = new Path("lakefs://repo/main/file");
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath("/repositories/repo/refs/main/objects/stat")
                              .withQueryStringParameter("path", "file"))
            .respond(response()
                     .withStatusCode(200)
                     .withBody("{\"path\": \"file\", \"unknown-key\": \"ignored\"}"));
        LakeFSFileStatus fileStatus = fs.getFileStatus(path);
        Assert.assertEquals(path, fileStatus.getPath());
    }

    @Test
    public void testGetFileStatus_ExistingFile() throws IOException {
        Path path = new Path("lakefs://repo/main/mock/exists");
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath("/repositories/repo/refs/main/objects/stat")
                              .withQueryStringParameter("path", "mock/exists"))
            // TODO(ariels) mockStatObject()!
            .respond(response()
                     .withStatusCode(200)
                     .withBody(gson.toJson(new ObjectStats().path("mock/exists"))));
        LakeFSFileStatus fileStatus = fs.getFileStatus(path);
        Assert.assertTrue(fileStatus.isFile());
        Assert.assertEquals(path, fileStatus.getPath());
    }

    @Test
    public void testGetFileStatus_NoFile() {
        Path noFilePath = new Path("lakefs://repo/main/no.file");

        mockStatObjectNotFound("repo", "main", "no.file");
        mockStatObjectNotFound("repo", "main", "no.file/");
        mockListing("repo", "main", ImmutablePagination.builder().prefix("no.file/").amount(1).build());
        Assert.assertThrows(FileNotFoundException.class, () -> fs.getFileStatus(noFilePath));
    }

    @Test
    public void testGetFileStatus_DirectoryMarker() throws IOException {
        Path dirPath = new Path("lakefs://repo/main/dir1/dir2");
        mockStatObjectNotFound("repo", "main", "dir1/dir2");

        ObjectStats stats = new ObjectStats()
            .path("dir1/dir2/")
            .physicalAddress(s3Url("repo-base/dir12"));
        mockStatObject("repo", "main", "dir1/dir2/", stats);

        LakeFSFileStatus dirStatus = fs.getFileStatus(dirPath);
        Assert.assertTrue(dirStatus.isDirectory());
        Assert.assertEquals(dirPath, dirStatus.getPath());
    }

    @Test
    public void testExists_ExistsAsObject() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        ObjectStats stats = new ObjectStats()
            .path("exis.ts")
            .physicalAddress(s3Url("repo-base/o12"));
        mockListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts").build(), stats);
        Assert.assertTrue(fs.exists(path));
    }

    @Test
    public void testExists_ExistsAsDirectoryMarker() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        ObjectStats stats = new ObjectStats().path("exis.ts");

        mockListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts").build(),
                    stats);

        Assert.assertTrue(fs.exists(path));
    }

    @Test
    public void testExists_ExistsAsDirectoryContents() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        ObjectStats stats = new ObjectStats().path("exis.ts/object-inside-the-path");

        mockListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts").build(),
                    stats);
        Assert.assertTrue(fs.exists(path));
    }

    @Test
    public void testExists_ExistsAsDirectoryInSecondList() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        ObjectStats beforeStats1 = new ObjectStats().path("exis.ts!");
        ObjectStats beforeStats2 = new ObjectStats().path("exis.ts$x");
        ObjectStats indirStats = new ObjectStats().path("exis.ts/object-inside-the-path");

        // First listing returns irrelevant objects, _before_ "exis.ts/"
        mockListingWithHasMore("repo", "main",
                               ImmutablePagination.builder().prefix("exis.ts").build(),
                               true,
                               beforeStats1, beforeStats2);
        // Second listing tries to find an object inside "exis.ts/".
        mockListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts/").build(),
                    indirStats);
        Assert.assertTrue(fs.exists(path));
    }

    @Test
    public void testExists_NotExistsNoPrefix() throws IOException {
        Path path = new Path("lakefs://repo/main/doesNotExi.st");
        Object emptyBody = ImmutableMap.of("results", ImmutableList.of(),
                                           "pagination", ImmutablePagination.builder().build());
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath("/repositories/repo/refs/main/objects/ls"))
            .respond(response()
                     .withStatusCode(200)
                     .withBody(gson.toJson(emptyBody)));
        Assert.assertFalse(fs.exists(path));
    }

    @Test
    public void testExists_NotExistsPrefixWithNoSlash() {
        // TODO(ariels)
    }

    @Test
    public void testExists_NotExistsPrefixWithNoSlashTwoLists() {
        // TODO(ariels)
    }

    @Test
    public void testDelete_FileExists() throws IOException {
        mockStatObject("repo", "main", "no/place/file.txt", new ObjectStats()
                       .path("delete/sample/file.txt")
                       .pathType(PathTypeEnum.OBJECT)
                       .physicalAddress(s3Url("repo-base/delete")));
        String[] arrDirs = {"no/place", "no"};
        for (String dir: arrDirs) {
            mockStatObjectNotFound("repo", "main", dir);
            mockStatObjectNotFound("repo", "main", dir + "/");
            mockListing("repo", "main", ImmutablePagination.builder().build());
        }
        mockDeleteObject("repo", "main", "no/place/file.txt");
        mockUploadObject("repo", "main", "no/place/");

        Path path = new Path("lakefs://repo/main/no/place/file.txt");

        mockDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));

        Assert.assertTrue(fs.delete(path, false));
    }

    @Test
    public void testDelete_FileNotExists() throws IOException {
        mockDeleteObjectNotFound("repo", "main", "no/place/file.txt");
        mockStatObjectNotFound("repo", "main", "no/place/file.txt");
        mockStatObjectNotFound("repo", "main", "no/place/file.txt/");
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("no/place/file.txt/").build());

        // Should still create a directory marker!
        mockUploadObject("repo", "main", "no/place/");

        // return false because file not found
        Assert.assertFalse(fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), false));
    }

    @Test
    public void testDelete_EmptyDirectoryExists() throws IOException {
        ObjectLocation dirObjLoc = new ObjectLocation("lakefs", "repo", "main", "delete/me");
        String key = objectLocToS3ObjKey(dirObjLoc);

        mockStatObjectNotFound(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath());
        ObjectStats srcStats = new ObjectStats()
            .path(dirObjLoc.getPath() + Constants.SEPARATOR)
            .sizeBytes(0L)
            .mtime(UNUSED_MTIME)
            .pathType(PathTypeEnum.OBJECT)
            .physicalAddress(s3Url(key+Constants.SEPARATOR))
            .checksum(UNUSED_CHECKSUM);
        mockStatObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath() + Constants.SEPARATOR, srcStats);

        // Just a directory marker delete/me/, so nothing to delete.
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("delete/me/").build(),
                    srcStats);

        mockDirectoryMarker(dirObjLoc.getParent());
        mockStatObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath(), srcStats);
        mockDeleteObject("repo", "main", "delete/me/");
        // Now need to create the parent directory.
        mockUploadObject("repo", "main", "delete/");

        Path path = new Path("lakefs://repo/main/delete/me");

        Assert.assertTrue(fs.delete(path, false));
    }

    @Test
    public void testDelete_DirectoryWithFile() throws IOException {
        String directoryPath = "delete/sample";
        String existingPath = "delete/sample/file.txt";
        String directoryToDelete = "lakefs://repo/main/delete/sample";
        mockStatObjectNotFound("repo", "main", directoryPath);
        mockStatObjectNotFound("repo", "main", directoryPath + Constants.SEPARATOR);
        // Just a single object under delete/sample/, not even a directory
        // marker for delete/sample/.
        ObjectStats srcStats = new ObjectStats().
            path(existingPath).
            pathType(PathTypeEnum.OBJECT).
            physicalAddress(s3Url("/repo-base/delete")).
            checksum(UNUSED_CHECKSUM).
            mtime(UNUSED_MTIME).
            sizeBytes(UNUSED_FILE_SIZE);
        mockListing("repo", "main",
                    ImmutablePagination.builder()
                    .prefix(directoryPath + Constants.SEPARATOR)
                    .build(),
                    srcStats);

        // No deletes!
        mockServerClient.when(request()
                              .withMethod("DELETE"))
            .respond(response().withStatusCode(400).withBody("Should not delete anything"));

        // Can't delete a directory without recursive, and
        // delete/sample/file.txt is not deleted.
        Exception e =
            Assert.assertThrows(IOException.class,
                                () -> fs.delete(new Path(directoryToDelete), false));
        String failureMessage =
            String.format("Path is a non-empty directory: %s", directoryToDelete);
        Assert.assertThat(e.getMessage(), new StringContains(failureMessage));
    }

    @Test
    public void testDelete_NotExistsRecursive() throws IOException {
        // No objects to stat.
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath("/repositories/repo/refs/main/objects/stat"))
            .respond(response().withStatusCode(404));
        // No objects to list, either -- in directory.
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("no/place/file.txt/").build());
        Assert.assertFalse(fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), true));
    }

    @Test
    public void testDelete_DirectoryWithFileRecursive() throws IOException {
        mockStatObjectNotFound("repo", "main", "delete/sample");
        mockStatObjectNotFound("repo", "main", "delete/sample/");
        ObjectStats stats = new ObjectStats().
            path("delete/sample/file.txt").
            pathType(PathTypeEnum.OBJECT).
            physicalAddress(s3Url("/repo-base/delete")).
            checksum(UNUSED_CHECKSUM).
            mtime(UNUSED_MTIME).
            sizeBytes(UNUSED_FILE_SIZE);
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("delete/sample/").build(),
                    stats);

        mockDeleteObjects("repo", "main", "delete/sample/file.txt");

        // recursive will always end successfully
        Path path = new Path("lakefs://repo/main/delete/sample");

        mockDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));
        // Must create a parent directory marker: it wasn't deleted, and now
        // perhaps is empty.
        mockUploadObject("repo", "main", "delete/");

        boolean delete = fs.delete(path, true);
        Assert.assertTrue(delete);
    }

    protected void caseDeleteDirectoryRecursive(int bulkSize, int numObjects) throws IOException {
        conf.setInt(LakeFSFileSystem.LAKEFS_DELETE_BULK_SIZE, bulkSize);
        mockStatObjectNotFound("repo", "main", "delete/sample");
        mockStatObjectNotFound("repo", "main", "delete/sample/");

        ObjectStats[] objects = new ObjectStats[numObjects];
        for (int i = 0; i < numObjects; i++) {
            objects[i] = new ObjectStats().
                path(String.format("delete/sample/file%04d.txt", i)).
                pathType(PathTypeEnum.OBJECT).
                physicalAddress(s3Url(String.format("/repo-base/delete%04d", i))).
                checksum(UNUSED_CHECKSUM).
                mtime(UNUSED_MTIME).
                sizeBytes(UNUSED_FILE_SIZE);
        }
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("delete/sample/").build(),
                    objects);

        // Set up multiple deleteObjects expectations of bulkSize deletes
        // each (except for the last, which might be smaller).
        for (int start = 0; start < numObjects; start += bulkSize) {
            PathList pl = new PathList();
            for (int i = start; i < numObjects && i < start + bulkSize; i++) {
                pl.addPathsItem(String.format("delete/sample/file%04d.txt", i));
            }
            mockDeleteObjects("repo", "main", pl);
        }
        // Mock parent directory marker creation at end of fs.delete to show
        // the directory marker exists.
        mockUploadObject("repo", "main", "delete/");
        // recursive will always end successfully
        Assert.assertTrue(fs.delete(new Path("lakefs://repo/main/delete/sample"), true));
    }

    @Test
    public void testDeleteDirectoryRecursiveBatch1() throws IOException {
        caseDeleteDirectoryRecursive(1, 123);
    }

    @Test
    public void testDeleteDirectoryRecursiveBatch2() throws IOException {
        caseDeleteDirectoryRecursive(2, 123);
    }

    @Test
    public void testDeleteDirectoryRecursiveBatch3() throws IOException {
        caseDeleteDirectoryRecursive(3, 123);
    }
    @Test
    public void testDeleteDirectoryRecursiveBatch5() throws IOException {
        caseDeleteDirectoryRecursive(5, 123);
    }
    @Test
    public void testDeleteDirectoryRecursiveBatch120() throws IOException {
        caseDeleteDirectoryRecursive(120, 123);
    }
    @Test
    public void testDeleteDirectoryRecursiveBatch123() throws IOException {
        caseDeleteDirectoryRecursive(123, 123);
    }

    @Test
    public void testListStatusFile() throws IOException {
        ObjectStats objectStats = new ObjectStats().
            path("status/file").
            pathType(PathTypeEnum.OBJECT).
            physicalAddress(s3Url("/repo-base/status")).
            checksum(STATUS_CHECKSUM).
            mtime(STATUS_MTIME).
            sizeBytes(STATUS_FILE_SIZE);
        mockStatObject("repo", "main", "status/file", objectStats);
        Path path = new Path("lakefs://repo/main/status/file");
        FileStatus[] fileStatuses = fs.listStatus(path);
        LakeFSFileStatus expectedFileStatus = new LakeFSFileStatus.Builder(path)
            .length(STATUS_FILE_SIZE)
            .checksum(STATUS_CHECKSUM)
            .mTime(STATUS_MTIME)
            .physicalAddress(s3Url("/repo-base/status"))
            .blockSize(Constants.DEFAULT_BLOCK_SIZE)
            .build();
        LakeFSFileStatus[] expectedFileStatuses = new LakeFSFileStatus[]{expectedFileStatus};
        Assert.assertArrayEquals(expectedFileStatuses, fileStatuses);
    }

    @Test
    public void testListStatusNotFound() throws ApiException {
        mockStatObjectNotFound("repo", "main", "status/file");
        mockStatObjectNotFound("repo", "main", "status/file/");
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("status/file/").build());
        Path path = new Path("lakefs://repo/main/status/file");
        Assert.assertThrows(FileNotFoundException.class, () -> fs.listStatus(path));
    }

    @Test
    public void testListStatusDirectory() throws IOException {
        int totalObjectsCount = 3;
        ObjectStats[] objects = new ObjectStats[3];
        for (int i = 0; i < totalObjectsCount; i++) {
            objects[i] = new ObjectStats().
                path("status/file" + i).
                pathType(PathTypeEnum.OBJECT).
                physicalAddress(s3Url("/repo-base/status" + i)).
                checksum(STATUS_CHECKSUM).
                mtime(STATUS_MTIME).
                sizeBytes(STATUS_FILE_SIZE);
        }
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("status/").build(),
                    objects);
        mockStatObjectNotFound("repo", "main", "status");

        Path dir = new Path("lakefs://repo/main/status");
        FileStatus[] fileStatuses = fs.listStatus(dir);

        FileStatus[] expectedFileStatuses = new LocatedFileStatus[totalObjectsCount];
        for (int i = 0; i < totalObjectsCount; i++) {
            Path p = new Path(dir + "/file" + i);
            LakeFSFileStatus fileStatus = new LakeFSFileStatus.Builder(p)
                .length(STATUS_FILE_SIZE)
                .checksum(STATUS_CHECKSUM)
                .mTime(STATUS_MTIME)
                .blockSize(Constants.DEFAULT_BLOCK_SIZE)
                .physicalAddress(s3Url("/repo-base/status" + i))
                .build();
            expectedFileStatuses[i] = new LocatedFileStatus(fileStatus, null);
        }
        Assert.assertArrayEquals(expectedFileStatuses, fileStatuses);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAppend() throws IOException {
        fs.append(null, 0, null);
    }

    /**
     * rename(src.txt, non-existing-dst) -> non-existing/new - unsupported, should fail with false.  (Test was buggy, FIX!)
     */
    @Test
    public void testRename_existingFileToNonExistingDst() throws IOException, ApiException {
        Path src = new Path("lakefs://repo/main/existing.src");

        ObjectStats stats = new ObjectStats()
            .path("existing.src")
            .sizeBytes(STATUS_FILE_SIZE)
            .mtime(STATUS_MTIME)
            .pathType(PathTypeEnum.OBJECT)
            .physicalAddress(s3Url("existing.src"))
            .checksum(STATUS_CHECKSUM);

        mockStatObject("repo", "main", "existing.src", stats);

        Path dst = new Path("lakefs://repo/main/non-existing/new");

        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("non-existing/").build());
        mockStatObjectNotFound("repo", "main", "non-existing/new");
        mockStatObjectNotFound("repo", "main", "non-existing/new/");
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("non-existing/new/").build());
        mockStatObjectNotFound("repo", "main", "non-existing");
        mockStatObjectNotFound("repo", "main", "non-existing/");

        boolean renamed = fs.rename(src, dst);
        Assert.assertFalse(renamed);
    }

    @Test
    public void testRename_existingFileToExistingFileName() throws IOException {
        Path src = new Path("lakefs://repo/main/existing.src");
        ObjectStats srcStats = new ObjectStats()
            .path("existing.src")
            .pathType(PathTypeEnum.OBJECT)
            .physicalAddress("base/existing.src");
        mockStatObject("repo", "main", "existing.src", srcStats);

        Path dst = new Path("lakefs://repo/main/existing.dst");
        ObjectStats dstStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existing.dst")
            .physicalAddress(s3Url("existing.dst"));
        mockStatObject("repo", "main", "existing.dst", dstStats);

        mockServerClient.when(request()
                              .withMethod("POST")
                              .withPath("/repositories/repo/branches/main/objects/copy")
                              .withQueryStringParameter("dest_path", "existing.dst")
                              .withBody(json(gson.toJson(new ObjectCopyCreation()
                                                         .srcRef("main")
                                                         .srcPath("existing.src")))))
            .respond(response()
                     .withStatusCode(201)
                     // Actual new dstStats will be different... but lakeFSFS doesn't care.
                     .withBody(json(gson.toJson(dstStats))));

        mockDeleteObject("repo", "main", "existing.src");

        Assert.assertTrue(fs.rename(src, dst));
    }

    @Test
    public void testRename_existingDirToExistingFileName() throws IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir/existing.src");
        ObjectStats srcStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existing-dir/existing.src");
        Path srcDir = new Path("lakefs://repo/main/existing-dir");
        mockStatObjectNotFound("repo", "main", "existing-dir");
        mockStatObjectNotFound("repo", "main", "existing-dir/");
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("existing-dir/").build(),
                    srcStats);

        Path dst = new Path("lakefs://repo/main/existingdst.file");
        ObjectStats dstStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existingdst.file");
        mockStatObject("repo", "main", "existingdst.file", dstStats);

        Assert.assertFalse(fs.rename(srcDir, dst));
    }

    /**
     * file -> existing-directory-name: rename(src.txt, existing-dstdir) -> existing-dstdir/src.txt
     */
    @Test
    public void testRename_existingFileToExistingDirName() throws ApiException, IOException {
        Path src = new Path("lakefs://repo/main/existing-dir1/existing.src");
        ObjectStats srcStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existing-dir1/existing.src");
        mockStatObject("repo", "main", "existing-dir1/existing.src", srcStats);

        ObjectStats dstStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existing-dir2/existing.src");
        mockFileDoesNotExist("repo", "main", "existing-dir2");
        mockFileDoesNotExist("repo", "main", "existing-dir2/");
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("existing-dir2/").build(),
                    dstStats);

        Path dst = new Path("lakefs://repo/main/existing-dir2/");

        mockServerClient.when(request()
                              .withMethod("POST")
                              .withPath("/repositories/repo/branches/main/objects/copy")
                              .withQueryStringParameter("dest_path", "existing-dir2/existing.src")
                              .withBody(json(gson.toJson(new ObjectCopyCreation()
                                                         .srcRef("main")
                                                         .srcPath("existing-dir1/existing.src")))))
            .respond(response()
                     .withStatusCode(201)
                     // Actual new dstStats will be different... but lakeFSFS doesn't care.
                     .withBody(json(gson.toJson(dstStats))));
        mockGetBranch("repo", "main");
        mockDeleteObject("repo", "main", "existing-dir1/existing.src");

        // Need a directory marker at the source because it's now empty!
        mockUploadObject("repo", "main", "existing-dir1/");

        Assert.assertTrue(fs.rename(src, dst));
    }

    /**
     * rename(srcDir(containing srcDir/a.txt, srcDir/b.txt), non-existing-dir/new) -> unsupported, rename should fail by returning false
     */
    @Test
    public void testRename_existingDirToNonExistingDirWithoutParent() throws IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir/existing.src");
        Path srcDir = new Path("lakefs://repo/main/existing-dir");

        mockFilesInDir("repo", "main", "existing-dir", "existing.src");

        mockFileDoesNotExist("repo", "main", "x/non-existing-dir");
        mockFileDoesNotExist("repo", "main", "x/non-existing-dir/new");
        // Will also check if parent of destination is a directory (it isn't).
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("x/non-existing-dir/").build());
        mockListing("repo", "main",
                    ImmutablePagination.builder().prefix("x/non-existing-dir/new/").build());

        // Keep a directory marker, or rename will try to create one because
        // it emptied the existing directory.
        mockStatObject("repo", "main", "x",
                       new ObjectStats().pathType(PathTypeEnum.OBJECT).path("x"));

        Path dst = new Path("lakefs://repo/main/x/non-existing-dir/new");

        Assert.assertFalse(fs.rename(srcDir, dst));
    }

    /**
     * rename(srcDir(containing srcDir/a.txt, srcDir/b.txt), non-existing-dir/new) -> unsupported, rename should fail by returning false
     */
    @Test
    public void testRename_existingDirToNonExistingDirWithParent() throws ApiException, IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir/existing.src");
        Path srcDir = new Path("lakefs://repo/main/existing-dir");
        Path dst = new Path("lakefs://repo/main/existing-dir2/new");

        ObjectStats srcStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existing-dir/existing.src");

        mockStatObjectNotFound("repo", "main", "existing-dir");
        mockStatObjectNotFound("repo", "main", "existing-dir/");
        mockListing("repo", "main", ImmutablePagination.builder().prefix("existing-dir/").build(),
                    srcStats);

        mockStatObjectNotFound("repo", "main", "existing-dir2");
        mockStatObject("repo", "main", "existing-dir2/",
                       new ObjectStats().pathType(PathTypeEnum.OBJECT).path("existing-dir2/"));

        mockStatObjectNotFound("repo", "main", "existing-dir2/new");
        mockStatObjectNotFound("repo", "main", "existing-dir2/new/");
        mockListing("repo", "main", ImmutablePagination.builder().prefix("existing-dir2/new/").build());

        ObjectStats dstStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existing-dir2/new/existing.src");

        mockServerClient.when(request()
                              .withMethod("POST")
                              .withPath("/repositories/repo/branches/main/objects/copy")
                              .withQueryStringParameter("dest_path", "existing-dir2/new/existing.src")
                              .withBody(json(gson.toJson(new ObjectCopyCreation()
                                                         .srcRef("main")
                                                         .srcPath("existing-dir/existing.src")))))
            .respond(response()
                     .withStatusCode(201)
                     .withBody(json(gson.toJson(dstStats))));
        mockDeleteObject("repo", "main", "existing-dir/existing.src");
        // Directory marker no longer required.
        mockDeleteObject("repo", "main", "existing-dir2/");

        boolean renamed = fs.rename(srcDir, dst);
        Assert.assertTrue(renamed);
    }

    // /**
    //  * rename(srcDir(containing srcDir/a.txt), existing-nonempty-dstdir) -> unsupported, rename should fail by returning false.
    //  */
    // @Test
    // public void testRename_existingDirToExistingNonEmptyDirName() throws ApiException, IOException {
    //     Path firstSrcFile = new Path("lakefs://repo/main/existing-dir1/a.src");
    //     ObjectLocation firstObjLoc = fs.pathToObjectLocation(firstSrcFile);
    //     Path secSrcFile = new Path("lakefs://repo/main/existing-dir1/b.src");
    //     ObjectLocation secObjLoc = fs.pathToObjectLocation(secSrcFile);

    //     Path srcDir = new Path("lakefs://repo/main/existing-dir1");
    //     ObjectLocation srcDirObjLoc = fs.pathToObjectLocation(srcDir);
    //     mockExistingDirPath(srcDirObjLoc, ImmutableList.of(firstObjLoc, secObjLoc));

    //     Path fileInDstDir = new Path("lakefs://repo/main/existing-dir2/file.dst");
    //     ObjectLocation dstFileObjLoc = fs.pathToObjectLocation(fileInDstDir);
    //     Path dstDir = new Path("lakefs://repo/main/existing-dir2");
    //     ObjectLocation dstDirObjLoc = fs.pathToObjectLocation(dstDir);
    //     mockExistingDirPath(dstDirObjLoc, ImmutableList.of(dstFileObjLoc));

    //     boolean renamed = fs.rename(srcDir, dstDir);
    //     Assert.assertFalse(renamed);
    // }

    // /**
    //  * Check that a file is renamed when working against a lakeFS version
    //  * where CopyObject API doesn't exist
    //  */
    // @Test
    // public void testRename_fallbackStageAPI() throws ApiException, IOException {
    //     Path src = new Path("lakefs://repo/main/existing-dir1/existing.src");
    //     ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);
    //     mockExistingFilePath(srcObjLoc);

    //     Path fileInDstDir = new Path("lakefs://repo/main/existing-dir2/existing.src");
    //     ObjectLocation fileObjLoc = fs.pathToObjectLocation(fileInDstDir);
    //     Path dst = new Path("lakefs://repo/main/existing-dir2");
    //     ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);

    //     mockExistingDirPath(dstObjLoc, ImmutableList.of(fileObjLoc));
    //     mockDirectoryMarker(fs.pathToObjectLocation(src.getParent()));
    //     mockMissingCopyAPI();

    //     boolean renamed = fs.rename(src, dst);
    //     Assert.assertTrue(renamed);
    //     Path expectedDstPath = new Path("lakefs://repo/main/existing-dir2/existing.src");
    //     Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(srcObjLoc, fs.pathToObjectLocation(expectedDstPath)));
    //     verifyObjDeletion(srcObjLoc);
    // }

    // @Test
    // public void testRename_srcAndDstOnDifferentBranch() throws IOException, ApiException {
    //     Path src = new Path("lakefs://repo/branch/existing.src");
    //     Path dst = new Path("lakefs://repo/another-branch/existing.dst");
    //     boolean renamed = fs.rename(src, dst);
    //     Assert.assertFalse(renamed);
    //     Mockito.verify(objectsApi, never()).statObject(any(), any(), any(), any(), any());
    //     Mockito.verify(objectsApi, never()).copyObject(any(), any(), any(), any());
    //     Mockito.verify(objectsApi, never()).deleteObject(any(), any(), any());
    // }

    // /**
    //  * no-op. rename is expected to succeed.
    //  */
    // @Test
    // public void testRename_srcEqualsDst() throws IOException, ApiException {
    //     Path src = new Path("lakefs://repo/main/existing.src");
    //     Path dst = new Path("lakefs://repo/main/existing.src");
    //     boolean renamed = fs.rename(src, dst);
    //     Assert.assertTrue(renamed);
    //     Mockito.verify(objectsApi, never()).statObject(any(), any(), any(), any(), any());
    //     Mockito.verify(objectsApi, never()).copyObject(any(), any(), any(), any());
    //     Mockito.verify(objectsApi, never()).deleteObject(any(), any(), any());
    // }

    // @Test
    // public void testRename_nonExistingSrcFile() throws ApiException, IOException {
    //     Path src = new Path("lakefs://repo/main/non-existing.src");
    //     ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);
    //     mockNonExistingPath(srcObjLoc);

    //     Path dst = new Path("lakefs://repo/main/existing.dst");
    //     ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);
    //     mockExistingFilePath(dstObjLoc);

    //     boolean success = fs.rename(src, dst);
    //     Assert.assertFalse(success);
    // }

    // /**
    //  * globStatus is used only by the Hadoop CLI where the pattern is always the exact file.
    //  */
    // @Test
    // public void testGlobStatus_SingleFile() throws ApiException, IOException {
    //     Path path = new Path("lakefs://repo/main/existing.dst");
    //     ObjectLocation dstObjLoc = fs.pathToObjectLocation(path);
    //     mockExistingFilePath(dstObjLoc);

    //     FileStatus[] statuses = fs.globStatus(path);
    //     Assert.assertArrayEquals(new FileStatus[]{
    //             new LakeFSFileStatus.Builder(path).build()
    //     }, statuses);
    // }
}
