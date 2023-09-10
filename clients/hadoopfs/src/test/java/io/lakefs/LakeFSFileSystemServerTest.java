package io.lakefs;

import io.lakefs.clients.api.*;
import io.lakefs.clients.api.model.*;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import io.lakefs.utils.ObjectLocation;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.matchers.Times;
import org.mockserver.model.Cookie;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;

import static org.mockserver.model.HttpResponse.response;

import com.google.common.base.Optional;
import org.immutables.value.Value;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LakeFSFileSystemServerTest {
    static final Long UNUSED_FILE_SIZE = 1L;
    static final Long UNUSED_MTIME = 0L;
    static final String UNUSED_CHECKSUM = "unused";

    static final Long STATUS_FILE_SIZE = 2L;
    static final Long STATUS_MTIME = 0L;
    static final String STATUS_CHECKSUM = "status";

    protected Configuration conf;
    protected final LakeFSFileSystem fs = new LakeFSFileSystem();

    protected AmazonS3 s3Client;

    protected String s3Base;
    protected String s3Bucket;

    private static final DockerImageName MINIO = DockerImageName.parse("minio/minio:RELEASE.2021-06-07T21-40-51Z");
    protected static final String S3_ACCESS_KEY_ID = "AKIArootkey";
    protected static final String S3_SECRET_ACCESS_KEY = "secret/minio/key=";

    protected static final ApiException noSuchFile = new ApiException(HttpStatus.SC_NOT_FOUND, "no such file");

    protected final Gson gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();

    @Rule
    public final GenericContainer s3 = new GenericContainer(MINIO.toString()).
        withCommand("minio", "server", "/data").
        withEnv("MINIO_ROOT_USER", S3_ACCESS_KEY_ID).
        withEnv("MINIO_ROOT_PASSWORD", S3_SECRET_ACCESS_KEY).
        withEnv("MINIO_DOMAIN", "s3.local.lakefs.io").
        withEnv("MINIO_UPDATE", "off").
        withExposedPorts(9000);

    // TODO(ariels): Include in resources or otherwise package nicely.
    // TODO(ariels): Read, parse the spec once!
    String openAPISpec = "../../api/swagger.yml";

    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);
    protected MockServerClient mockServerClient;

    @Rule
    public TestName name = new TestName();

    protected String sessionId() {
        return name.getMethodName();
    }

    protected HttpRequest request() {
        return HttpRequest.request().withCookie(new Cookie("sessionId", sessionId()));
    }

    //    abstract void initConfiguration();
    
    //    abstract void mockStatObject(String repo, String branch, String key, String physicalKey, Long sizeBytes) throws ApiException;

    //    abstract StagingLocation mockGetPhysicalAddress(String repo, String branch, String key, String physicalKey) throws ApiException;

    protected static String makeS3BucketName() {
        String slug = NanoIdUtils.randomNanoId(NanoIdUtils.DEFAULT_NUMBER_GENERATOR,
                                               "abcdefghijklmnopqrstuvwxyz-0123456789".toCharArray(), 14);
        return String.format("bucket-%s-x", slug);
    }

    /** @return "s3://..." URL to use for s3Path (which does not start with a slash) on bucket */
    protected String s3Url(String s3Path) {
        return s3Base + s3Path;
    }

    protected String objectLocToS3ObjKey(ObjectLocation objectLoc) {
        return String.format("/%s/%s/%s",objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
    }

    @Before
    public void logS3Container() {
        Logger s3Logger = LoggerFactory.getLogger("s3 container");
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(s3Logger)
            .withMdc("container", "s3")
            .withSeparateOutputStreams();
        s3.followOutput(logConsumer);
    }

    @Before
    public void setUp() throws Exception {
        AWSCredentials creds = new BasicAWSCredentials(S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withSignerOverride("AWSS3V4SignerType");
        String s3Endpoint = String.format("http://s3.local.lakefs.io:%d", s3.getMappedPort(9000));

        s3Client = new AmazonS3Client(creds, clientConfiguration);

        S3ClientOptions s3ClientOptions = new S3ClientOptions()
            .withPathStyleAccess(true);
        s3Client.setS3ClientOptions(s3ClientOptions);
        s3Client.setEndpoint(s3Endpoint);

        s3Bucket = makeS3BucketName();
        s3Base = String.format("s3://%s", s3Bucket);
        CreateBucketRequest cbr = new CreateBucketRequest(s3Bucket);
        s3Client.createBucket(cbr);

        conf = new Configuration(false);
        //initConfiguration();
        conf.set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem");

        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set(org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY, S3_ACCESS_KEY_ID);
        conf.set(org.apache.hadoop.fs.s3a.Constants.SECRET_KEY, S3_SECRET_ACCESS_KEY);
        conf.set(org.apache.hadoop.fs.s3a.Constants.ENDPOINT, s3Endpoint);
        conf.set(org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR, "/tmp/s3a");

        conf.set("fs.lakefs.access.key", "unused-but-checked");
        conf.set("fs.lakefs.secret.key", "unused-but-checked");
        conf.set("fs.lakefs.endpoint", String.format("http://localhost:%d/", mockServerClient.getPort()));
        conf.set("fs.lakefs.session_id", sessionId());

        System.setProperty("hadoop.home.dir", "/");

        // lakeFSFS initialization requires a blockstore.
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath("/config/storage"),
                              Times.once())
            .respond(response()
                     .withStatusCode(200)
                     .withBody(gson.toJson(ImmutableMap.of("blockstore_type", "s3",
                                                           // TODO(ariels): Change for presigned?
                                                           "pre_sign_support", false))));

        fs.initialize(new URI("lakefs://repo/main/file.txt"), conf);
    }

    /**
     * @return all pathnames under s3Prefix that start with prefix.  (Obvious not scalable!)
     */
    protected List<String> getS3FilesByPrefix(String prefix) {
        final int maxKeys = 1500;

        ListObjectsRequest req = new ListObjectsRequest()
            .withBucketName(s3Bucket)
            .withPrefix(prefix)
            .withMaxKeys(maxKeys);
        ObjectListing listing = s3Client.listObjects(req);
        if (listing.isTruncated()) {
            Assert.fail(String.format("[internal] no support for test that creates >%d S3 objects", maxKeys));
        }

        return Lists.transform(listing.getObjectSummaries(), S3ObjectSummary::getKey);
    }

    @Test
    public void getUri() {
        URI u = fs.getUri();
        Assert.assertNotNull(u);
    }

    @Value.Immutable static public interface Stats {
        @Value.Parameter abstract String path();
        @Value.Default default String pathType() { return "object"; }
        @Value.Default default String physicalAddress() { return "/"; }
        @Value.Default default String checksum() { return UNUSED_CHECKSUM; }
        @Value.Default default long mtime() { return UNUSED_MTIME; }
        @Value.Default default long size() { return UNUSED_FILE_SIZE; }
    }

    @Value.Immutable static public interface Pagination {
        @Value.Parameter Optional<Integer> amount();
        @Value.Parameter Optional<String> after();
        @Value.Parameter Optional<String> prefix();
        @Value.Default default boolean hasMore() { return false; }
    }

    // Return response for these stats
    protected HttpResponse makeStats(Stats stats) {
        return response()
            .withStatusCode(200)
            .withBody(gson.toJson(ImmutableMap.of("path", stats.path(),
                                                  "path_type", stats.pathType(),
                                                  "physical_address",  stats.physicalAddress(),
                                                  "checksum", stats.checksum(),
                                                  "mtime", stats.mtime())));
    }

    // Expect this statObject to be not found
    protected void expectStatObjectNotFound(String repo, String ref, String path) {
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath(String.format("/repositories/%s/refs/%s/objects/stat", repo, ref))
                              .withQueryStringParameter("path", path))
            .respond(response().withStatusCode(404)
                     .withBody(String.format("{message: \"%s/%s/%s not found\"}",
                                             repo, ref, path, sessionId())));
    }

    protected void expectStatObject(String repo, String ref, String path, Object stats) {
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath(String.format("/repositories/%s/refs/%s/objects/stat", repo, ref))
                              .withQueryStringParameter("path", path))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(stats)));
    }

    protected void expectUploadObject(String repo, String branch, String path) {
        StagingLocation stagingLocation = new StagingLocation()
            .token("token:foo:" + sessionId())
            .physicalAddress(s3Url(String.format("/repo-base/dir-marker/%s/%s/%s/%s",
                                                 sessionId(), repo, branch, path)));
        mockServerClient.when(request()
                              .withMethod("POST")
                              .withPath(String.format("/repositories/%s/branches/%s/objects", repo, branch))
                              .withQueryStringParameter("path", path))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(stagingLocation)));
    }

    protected void expectDeleteObject(String repo, String branch, String path) {
        mockServerClient.when(request()
                              .withMethod("DELETE")
                              .withPath(String.format("/repositories/%s/branches/%s/objects", repo, branch))
                              .withQueryStringParameter("path", path))
            .respond(response().withStatusCode(204));
    }

    protected void expectDeleteObjectNotFound(String repo, String branch, String path) {
        mockServerClient.when(request()
                              .withMethod("DELETE")
                              .withPath(String.format("/repositories/%s/branches/%s/objects", repo, branch))
                              .withQueryStringParameter("path", path))
            .respond(response().withStatusCode(404));
    }

    protected void expectDirectoryMarker(ObjectLocation objectLoc) {
        // Mock parent directory to show the directory marker exists.
        ObjectStats markerStats = new ObjectStats().path(objectLoc.getPath()).pathType(PathTypeEnum.OBJECT);
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath(String.format("/repositories/%s/refs/%s/objects/stat", objectLoc.getRepository(), objectLoc.getRef()))
                              .withQueryStringParameter("path", objectLoc.getPath()))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(new ObjectStatsList().results(ImmutableList.of(markerStats)))));
    }

    // Expect this listing and return these stats.
    protected void expectListing(String repo, String ref, Pagination pagination, boolean hasMore, Object... stats) {
        HttpRequest req = request()
            .withMethod("GET")
            .withPath(String.format("/repositories/%s/refs/%s/objects/ls", repo, ref))
            // Validate prefix, it matters!
            .withQueryStringParameter("prefix", pagination.prefix().or(""))
            .withQueryStringParameter("after", pagination.after().or(""));
        if (pagination.amount().isPresent()) {
            // Validate amount only if requested.
            req = req.withQueryStringParameter("amount", pagination.amount().get().toString());
        }
        mockServerClient.when(req)
            .respond(response()
                     .withStatusCode(200)
                     .withBody(gson.toJson(ImmutableMap.of("results", Arrays.asList(stats),
                                                           "pagination", ImmutablePagination.builder().hasMore(hasMore).build()))));
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
            .respond(makeStats(ImmutableStats.builder().path("mock/exists").build()));
        LakeFSFileStatus fileStatus = fs.getFileStatus(path);
        Assert.assertTrue(fileStatus.isFile());
        Assert.assertEquals(path, fileStatus.getPath());
    }

    @Test
    public void testGetFileStatus_NoFile() {
        Path noFilePath = new Path("lakefs://repo/main/no.file");

        expectStatObjectNotFound("repo", "main", "no.file");
        expectStatObjectNotFound("repo", "main", "no.file/");
        expectListing("repo", "main", ImmutablePagination.builder().prefix("no.file/").amount(1).build(), false);
        Assert.assertThrows(FileNotFoundException.class, () -> fs.getFileStatus(noFilePath));
    }

    @Test
    public void testGetFileStatus_DirectoryMarker() throws IOException {
        Path dirPath = new Path("lakefs://repo/main/dir1/dir2");
        expectStatObjectNotFound("repo", "main", "dir1/dir2");

        HttpResponse res = makeStats(ImmutableStats.builder()
                                     .path("dir1/dir2/")
                                     .physicalAddress(s3Url("/repo-base/dir12"))
                                     .build());

        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath("/repositories/repo/refs/main/objects/stat")
                              .withQueryStringParameter("path", "dir1/dir2/"))
            .respond(res);

        LakeFSFileStatus dirStatus = fs.getFileStatus(dirPath);
        Assert.assertTrue(dirStatus.isDirectory());
        Assert.assertEquals(dirPath, dirStatus.getPath());
    }

    @Test
    public void testExists_ExistsAsObject() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        Object body = ImmutableMap.of("results",
                                      new Stats[]{ImmutableStats.builder()
                                                  .path("exis.ts")
                                                  .physicalAddress(s3Url("/repo-base/o12"))
                                                  .build()},
                                      "pagination", ImmutablePagination.builder().build());
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath("/repositories/repo/refs/main/objects/ls")
                              .withQueryStringParameter("prefix", "exis.ts"))
            .respond(response()
                     .withStatusCode(200)
                     .withBody(gson.toJson(body)));
        Assert.assertTrue(fs.exists(path));
    }

    @Test
    public void testExists_ExistsAsDirectoryMarker() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        Stats stats = ImmutableStats.builder()
            .path("exis.ts").build();

        expectListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts").build(), false,
                      stats);

        Assert.assertTrue(fs.exists(path));
    }

    @Test
    public void testExists_ExistsAsDirectoryContents() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        Stats stats = ImmutableStats.builder()
            .path("exis.ts/object-inside-the-path").build();

        expectListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts").build(), false,
                      stats);
        Assert.assertTrue(fs.exists(path));
    }

    @Test
    public void testExists_ExistsAsDirectoryInSecondList() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        Stats beforeStats1 = ImmutableStats.builder()
            .path("exis.ts!").build();
        Stats beforeStats2 = ImmutableStats.builder()
            .path("exis.ts$x").build();
        Stats indirStats = ImmutableStats.builder()
            .path("exis.ts/object-inside-the-path").build();

        // First listing returns irrelevant objects, _before_ "exis.ts/"
        expectListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts").build(), true,
                      beforeStats1, beforeStats2);
        // Second listing tries to find an object inside "exis.ts/".
        expectListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts/").build(), false,
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
    public void testDelete_FileExists() throws ApiException, IOException {
        expectStatObject("repo", "main", "no/place/file.txt", new ObjectStats()
                         .path("delete/sample/file.txt")
                         .pathType(PathTypeEnum.OBJECT)
                         .physicalAddress(s3Url("/repo-base/delete")));
        String[] arrDirs = {"no/place", "no"};
        for (String dir: arrDirs) {
            expectStatObjectNotFound("repo", "main", dir);
            expectStatObjectNotFound("repo", "main", dir + "/");
            expectListing("repo", "main", ImmutablePagination.builder().build(), false);
        }
        expectDeleteObject("repo", "main", "no/place/file.txt");
        expectUploadObject("repo", "main", "no/place/");

        Path path = new Path("lakefs://repo/main/no/place/file.txt");

        expectDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));

        Assert.assertTrue(fs.delete(path, false));
    }

    @Test
    public void testDelete_FileNotExists() throws ApiException, IOException {
        expectDeleteObjectNotFound("repo", "main", "no/place/file.txt");
        expectStatObjectNotFound("repo", "main", "no/place/file.txt");
        expectStatObjectNotFound("repo", "main", "no/place/file.txt/");
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("no/place/file.txt/").build(), false);

        // Should still create a directory marker!
        expectUploadObject("repo", "main", "no/place/");

        // return false because file not found
        Assert.assertFalse(fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), false));
    }

    @Test
    public void testDelete_EmptyDirectoryExists() throws ApiException, IOException {
        ObjectLocation dirObjLoc = new ObjectLocation("lakefs", "repo", "main", "delete/me");
        String key = objectLocToS3ObjKey(dirObjLoc);

        expectStatObjectNotFound(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath());
        ObjectStats srcStats = new ObjectStats()
                .path(dirObjLoc.getPath() + Constants.SEPARATOR)
                .sizeBytes(0L)
                .mtime(UNUSED_MTIME)
                .pathType(PathTypeEnum.OBJECT)
                .physicalAddress(s3Url(key+Constants.SEPARATOR))
                .checksum(UNUSED_CHECKSUM);
        expectStatObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath() + Constants.SEPARATOR, srcStats);

        // Just a directory marker delete/me/, so nothing to delete.
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("delete/me/").build(), false,
                      srcStats);

        expectDirectoryMarker(dirObjLoc.getParent());
        expectStatObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath(), srcStats);
        expectDeleteObject("repo", "main", "delete/me/");
        // Now need to create the parent directory.
        expectUploadObject("repo", "main", "delete/");

        Path path = new Path("lakefs://repo/main/delete/me");

        //expectDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));

        Assert.assertTrue(fs.delete(path, false));
    }

    // @Test(expected = IOException.class)
    // public void testDelete_DirectoryWithFile() throws ApiException, IOException {
    //     when(objectsApi.statObject("repo", "main", "delete/sample", false, false))
    //             .thenThrow(noSuchFile);
    //     when(objectsApi.statObject("repo", "main", "delete/sample/", false, false))
    //             .thenThrow(noSuchFile);
    //     when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""), any(), eq(""), eq("delete/sample/")))
    //             .thenReturn(new ObjectStatsList().results(Collections.singletonList(new ObjectStats().
    //                     path("delete/sample/file.txt").
    //                     pathType(PathTypeEnum.OBJECT).
    //                     physicalAddress(s3Url("/repo-base/delete")).
    //                     checksum(UNUSED_CHECKSUM).
    //                     mtime(UNUSED_MTIME).
    //                     sizeBytes(UNUSED_FILE_SIZE))).pagination(new Pagination().hasMore(false)));
    //     doThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "not found"))
    //             .when(objectsApi).deleteObject("repo", "main", "delete/sample/");
    //     // return false because we can't delete a directory without recursive
    //     fs.delete(new Path("lakefs://repo/main/delete/sample"), false);
    // }

    // @Test
    // public void testDelete_NotExistsRecursive() throws ApiException, IOException {
    //     when(objectsApi.statObject(eq("repo"), eq("main"), any(), eq(false), eq(false)))
    //             .thenThrow(noSuchFile);
    //     when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false),eq(false), eq(""), any(), eq(""), eq("no/place/file.txt/")))
    //             .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));
    //     boolean delete = fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), true);
    //     Assert.assertFalse(delete);
    // }

    // private PathList newPathList(String... paths) {
    //     return new PathList().paths(Arrays.asList(paths));
    // }

    // @Test
    // public void testDelete_DirectoryWithFileRecursive() throws ApiException, IOException {
    //     when(objectsApi.statObject("repo", "main", "delete/sample", false, false))
    //             .thenThrow(noSuchFile);
    //     when(objectsApi.statObject("repo", "main", "delete/sample/", false, false))
    //             .thenThrow(noSuchFile);
    //     when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""), any(), eq(""), eq("delete/sample/")))
    //             .thenReturn(new ObjectStatsList().results(Collections
    //                     .singletonList(new ObjectStats().
    //                             path("delete/sample/file.txt").
    //                             pathType(PathTypeEnum.OBJECT).
    //                             physicalAddress(s3Url("/repo-base/delete")).
    //                             checksum(UNUSED_CHECKSUM).
    //                             mtime(UNUSED_MTIME).
    //                             sizeBytes(UNUSED_FILE_SIZE)))
    //                     .pagination(new Pagination().hasMore(false)));
    //     when(objectsApi.deleteObjects("repo", "main", newPathList("delete/sample/file.txt")))
    //         .thenReturn(new ObjectErrorList());
    //     // recursive will always end successfully
    //     Path path = new Path("lakefs://repo/main/delete/sample");

    //     expectDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));

    //     boolean delete = fs.delete(path, true);
    //     Assert.assertTrue(delete);
    //}
}
