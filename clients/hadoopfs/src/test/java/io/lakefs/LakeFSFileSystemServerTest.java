package io.lakefs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import org.hamcrest.core.StringContains;

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

import org.mockserver.matchers.MatchType;
import org.mockserver.matchers.TimeToLive;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

import static org.apache.commons.lang3.StringUtils.removeStart;
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
    static private final Logger LOG = LoggerFactory.getLogger(LakeFSFileSystemServerTest.class);

    static final Long UNUSED_FILE_SIZE = 1L;
    static final Long UNUSED_MTIME = 0L;
    static final String UNUSED_CHECKSUM = "unused";

    static final Long STATUS_FILE_SIZE = 2L;
    static final Long STATUS_MTIME = 123456789L;
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

    // TODO(ariels): Remove!
    @Value.Immutable static public interface Pagination {
        @Value.Parameter Optional<Integer> amount();
        @Value.Parameter Optional<String> after();
        @Value.Parameter Optional<String> prefix();
    }

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

    // TODO(ariels): Override and make abstract!
    protected String createPhysicalAddress(String key) {
        return s3Url(key);
    }

    protected static String makeS3BucketName() {
        String slug = NanoIdUtils.randomNanoId(NanoIdUtils.DEFAULT_NUMBER_GENERATOR,
                                               "abcdefghijklmnopqrstuvwxyz-0123456789".toCharArray(), 14);
        return String.format("bucket-%s-x", slug);
    }

    /** @return "s3://..." URL to use for s3Path (which does not start with a slash) on bucket */
    protected String s3Url(String s3Path) {
        return s3Base + s3Path;
    }

    protected String getS3Key(StagingLocation stagingLocation) {
        return removeStart(stagingLocation.getPhysicalAddress(), s3Base);
    }

    protected void assertS3Object(StagingLocation stagingLocation, String contents) {
        String s3Key = getS3Key(stagingLocation);
        List<String> actualFiles = ImmutableList.of("<not yet listed>");
        try (S3Object obj =
             s3Client.getObject(new GetObjectRequest(s3Bucket, "/" + s3Key))) {
            actualFiles = getS3FilesByPrefix("");
            String actual = IOUtils.toString(obj.getObjectContent());
            Assert.assertEquals(contents, actual);

            Assert.assertEquals(ImmutableList.of(s3Key), actualFiles);
        } catch (Exception e) {
            throw new RuntimeException("Files " + actualFiles +
                                       "; read key " + s3Key + " failed", e);
        }
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
        s3Base = String.format("s3://%s/", s3Bucket);
        LOG.info("S3: bucket {} => base URL {}", s3Bucket, s3Base);

        // Always expect repo "repo" to be found, it's used in all tests.
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath("/repositories/repo"))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(new Repository().id("repo")
                                           .creationDate(1234L)
                                           .defaultBranch("main")
                                           .storageNamespace(s3Base))));

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
                     .withBody(gson.toJson(new StorageConfig()
                                           .blockstoreType("s3")
                                           .blockstoreNamespaceValidityRegex(".*")
                                           // TODO(ariels): Change for presigned?
                                           .preSignSupport(false))));

        // Don't return 404s for unknown paths - they will be emitted for
        // many bad requests or mocks, and make our life difficult.  Instead
        // fail using a unique error code.  This has very low priority.
        mockServerClient.when(request(), Times.unlimited(), TimeToLive.unlimited(), -10000)
            .respond(response().withStatusCode(418));
        // TODO(ariels): No tests mock "get underlying filesystem", so this
        //     also catches its "get repo" call.  Nothing bad happens, but
        //     this response does show up in logs.

        fs.initialize(new URI("lakefs://repo/main/file.txt"), conf);
    }

    /**
     * @return all pathnames under s3Prefix that start with prefix.  (Obvious not scalable!)
     */
    protected List<String> getS3FilesByPrefix(String prefix) {

        ListObjectsRequest req = new ListObjectsRequest()
            .withBucketName(s3Bucket)
            .withPrefix(prefix)
            .withDelimiter(null);

        ObjectListing listing = s3Client.listObjects(req);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        if (listing.isTruncated()) {
            Assert.fail(String.format("[internal] no support for test that creates >%d S3 objects", listing.getMaxKeys()));
        }

        return Lists.transform(summaries, S3ObjectSummary::getKey);
    }

    @Test
    public void getUri() {
        URI u = fs.getUri();
        Assert.assertNotNull(u);
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

    protected void expectStatObject(String repo, String ref, String path, ObjectStats stats) {
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath(String.format("/repositories/%s/refs/%s/objects/stat", repo, ref))
                              .withQueryStringParameter("path", path))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(stats)));
    }

    // Expect this lakeFSFS path not to exist.  You may still need to
    // expectListing for the directory that will not contain this pagth.
    protected void expectFileDoesNotExist(String repo, String ref, String path) {
        expectStatObjectNotFound(repo, ref, path);
        expectStatObjectNotFound(repo, ref, path + Constants.SEPARATOR);
    }

    protected void expectFilesInDir(String repo, String main, String dir, String... files) {
        ObjectStats[] allStats;
        if (files.length == 0) {
            // Fake a directory marker
            Path dirPath = new Path(String.format("lakefs://%s/%s/%s", repo, main, dir));
            ObjectLocation dirLoc = ObjectLocation.pathToObjectLocation(dirPath);
            ObjectStats dirStats = expectDirectoryMarker(dirLoc);
            allStats = new ObjectStats[1];
            allStats[0] = dirStats;
        } else {
            expectStatObjectNotFound(repo, main, dir);
            expectStatObjectNotFound(repo, main, dir + Constants.SEPARATOR);

            allStats = new ObjectStats[files.length];
            for (int i = 0; i < files.length; i++) {
                allStats[i] = new ObjectStats()
                    .pathType(PathTypeEnum.OBJECT)
                    .path(dir + Constants.SEPARATOR + files[i]);
            }
        }

        // Directory can be listed!
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix(dir + Constants.SEPARATOR).build(),
                      allStats);
    }

    protected void expectUploadObject(String repo, String branch, String path) {
        StagingLocation stagingLocation = new StagingLocation()
            .token("token:foo:" + sessionId())
            .physicalAddress(s3Url(String.format("repo-base/dir-marker/%s/%s/%s/%s",
                                                 sessionId(), repo, branch, path)));
        mockServerClient.when(request()
                              .withMethod("POST")
                              .withPath(String.format("/repositories/%s/branches/%s/objects", repo, branch))
                              .withQueryStringParameter("path", path))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(stagingLocation)));
    }

    protected void expectGetBranch(String repo, String branch) {
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath(String.format("/repositories/%s/branches/%s", repo, branch)))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(new Ref().id("123").commitId("456"))));
    }

    // Return a location under namespace for this getPhysicalAddress call.
    //
    // TODO(ariels): abstract, overload separately for direct and pre-signed.
    protected StagingLocation expectGetPhysicalAddress(String repo, String branch, String path, String namespace) {
        StagingLocation stagingLocation = new StagingLocation()
            .token("token:foo:" + sessionId())
            .physicalAddress(s3Url(String.format("%s/%s/%s/%s/%s-object",
                                                 sessionId(), namespace, repo, branch, path)));
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath(String.format("/repositories/%s/branches/%s/staging/backing", repo, branch))
                              .withQueryStringParameter("path", path))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(stagingLocation)));
        return stagingLocation;
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

    // Expects a single deleteObjects call to succeed, returning list of failures.
    protected void expectDeleteObjects(String repo, String branch, String path, ObjectError... errors) {
        PathList pathList = new PathList().addPathsItem(path);
        expectDeleteObjects(repo, branch, pathList, errors);
    }

    // Expects a single deleteObjects call to succeed, returning list of failures.
    protected void expectDeleteObjects(String repo, String branch, PathList pathList, ObjectError... errors) {
        mockServerClient.when(request()
                              .withMethod("POST")
                              .withPath(String.format("/repositories/%s/branches/%s/objects/delete", repo, branch))
                              .withBody(gson.toJson(pathList)),
                              Times.once())
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(new ObjectErrorList()
                                           .errors(Arrays.asList(errors)))));
    }

    protected ObjectStats expectDirectoryMarker(ObjectLocation objectLoc) {
        // Mock parent directory to show the directory marker exists.
        ObjectStats markerStats = new ObjectStats()
            .path(objectLoc.getPath())
            .pathType(PathTypeEnum.OBJECT);
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath(String.format("/repositories/%s/refs/%s/objects/stat", objectLoc.getRepository(), objectLoc.getRef()))
                              .withQueryStringParameter("path", objectLoc.getPath()))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(markerStats)));
        return markerStats;
    }

    // Expect this listing and return these stats.
    protected void expectListing(String repo, String ref, ImmutablePagination pagination, ObjectStats... stats) {
        expectListingWithHasMore(repo, ref, pagination, false, stats);
    }

    protected void expectListingWithHasMore(String repo, String ref, ImmutablePagination pagination, boolean hasMore, ObjectStats... stats) {
        HttpRequest req = request()
            .withMethod("GET")
            .withPath(String.format("/repositories/%s/refs/%s/objects/ls", repo, ref));
        // Validate elements of pagination only if present.
        if (pagination.after().isPresent()) {
            req = req.withQueryStringParameter("after", pagination.after().or(""));
        }
        if (pagination.amount().isPresent()) {
            req = req.withQueryStringParameter("amount", pagination.amount().get().toString());
        }
        if (pagination.prefix().isPresent()) {
            req = req.withQueryStringParameter("prefix", pagination.prefix().or(""));
        }
        mockServerClient.when(req)
            .respond(response()
                     .withStatusCode(200)
                     .withBody(gson.toJson(ImmutableMap.of("results", Arrays.asList(stats),
                                                           "pagination",
                                                           new io.lakefs.clients.api.model.Pagination().hasMore(hasMore)))));
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
            // TODO(ariels) expectStatObject()!
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

        expectStatObjectNotFound("repo", "main", "no.file");
        expectStatObjectNotFound("repo", "main", "no.file/");
        expectListing("repo", "main", ImmutablePagination.builder().prefix("no.file/").amount(1).build());
        Assert.assertThrows(FileNotFoundException.class, () -> fs.getFileStatus(noFilePath));
    }

    @Test
    public void testGetFileStatus_DirectoryMarker() throws IOException {
        Path dirPath = new Path("lakefs://repo/main/dir1/dir2");
        expectStatObjectNotFound("repo", "main", "dir1/dir2");

        ObjectStats stats = new ObjectStats()
            .path("dir1/dir2/")
            .physicalAddress(s3Url("repo-base/dir12"));
        expectStatObject("repo", "main", "dir1/dir2/", stats);

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
        expectListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts").build(), stats);
        Assert.assertTrue(fs.exists(path));
    }

    @Test
    public void testExists_ExistsAsDirectoryMarker() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        ObjectStats stats = new ObjectStats().path("exis.ts");

        expectListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts").build(),
                      stats);

        Assert.assertTrue(fs.exists(path));
    }

    @Test
    public void testExists_ExistsAsDirectoryContents() throws IOException {
        Path path = new Path("lakefs://repo/main/exis.ts");
        ObjectStats stats = new ObjectStats().path("exis.ts/object-inside-the-path");

        expectListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts").build(),
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
        expectListingWithHasMore("repo", "main",
                                 ImmutablePagination.builder().prefix("exis.ts").build(),
                                 false,
                                 beforeStats1, beforeStats2);
        // Second listing tries to find an object inside "exis.ts/".
        expectListing("repo", "main", ImmutablePagination.builder().prefix("exis.ts/").build(),
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
        expectStatObject("repo", "main", "no/place/file.txt", new ObjectStats()
                         .path("delete/sample/file.txt")
                         .pathType(PathTypeEnum.OBJECT)
                         .physicalAddress(s3Url("repo-base/delete")));
        String[] arrDirs = {"no/place", "no"};
        for (String dir: arrDirs) {
            expectStatObjectNotFound("repo", "main", dir);
            expectStatObjectNotFound("repo", "main", dir + "/");
            expectListing("repo", "main", ImmutablePagination.builder().build());
        }
        expectDeleteObject("repo", "main", "no/place/file.txt");
        expectUploadObject("repo", "main", "no/place/");

        Path path = new Path("lakefs://repo/main/no/place/file.txt");

        expectDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));

        Assert.assertTrue(fs.delete(path, false));
    }

    @Test
    public void testDelete_FileNotExists() throws IOException {
        expectDeleteObjectNotFound("repo", "main", "no/place/file.txt");
        expectStatObjectNotFound("repo", "main", "no/place/file.txt");
        expectStatObjectNotFound("repo", "main", "no/place/file.txt/");
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("no/place/file.txt/").build());

        // Should still create a directory marker!
        expectUploadObject("repo", "main", "no/place/");

        // return false because file not found
        Assert.assertFalse(fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), false));
    }

    @Test
    public void testDelete_EmptyDirectoryExists() throws IOException {
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
                      ImmutablePagination.builder().prefix("delete/me/").build(),
                      srcStats);

        expectDirectoryMarker(dirObjLoc.getParent());
        expectStatObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath(), srcStats);
        expectDeleteObject("repo", "main", "delete/me/");
        // Now need to create the parent directory.
        expectUploadObject("repo", "main", "delete/");

        Path path = new Path("lakefs://repo/main/delete/me");

        Assert.assertTrue(fs.delete(path, false));
    }

    @Test
    public void testDelete_DirectoryWithFile() throws IOException {
        String directoryPath = "delete/sample";
        String existingPath = "delete/sample/file.txt";
        String directoryToDelete = "lakefs://repo/main/delete/sample";
        expectStatObjectNotFound("repo", "main", directoryPath);
        expectStatObjectNotFound("repo", "main", directoryPath + Constants.SEPARATOR);
        // Just a single object under delete/sample/, not even a directory
        // marker for delete/sample/.
        ObjectStats srcStats = new ObjectStats().
            path(existingPath).
            pathType(PathTypeEnum.OBJECT).
            physicalAddress(s3Url("/repo-base/delete")).
            checksum(UNUSED_CHECKSUM).
            mtime(UNUSED_MTIME).
            sizeBytes(UNUSED_FILE_SIZE);
        expectListing("repo", "main",
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
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("no/place/file.txt/").build());
        Assert.assertFalse(fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), true));
    }

    @Test
    public void testDelete_DirectoryWithFileRecursive() throws IOException {
        expectStatObjectNotFound("repo", "main", "delete/sample");
        expectStatObjectNotFound("repo", "main", "delete/sample/");
        ObjectStats stats = new ObjectStats().
            path("delete/sample/file.txt").
            pathType(PathTypeEnum.OBJECT).
            physicalAddress(s3Url("/repo-base/delete")).
            checksum(UNUSED_CHECKSUM).
            mtime(UNUSED_MTIME).
            sizeBytes(UNUSED_FILE_SIZE);
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("delete/sample/").build(),
                      stats);

        expectDeleteObjects("repo", "main", "delete/sample/file.txt");

        // recursive will always end successfully
        Path path = new Path("lakefs://repo/main/delete/sample");

        expectDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));
        // Must create a parent directory marker: it wasn't deleted, and now
        // perhaps is empty.
        expectUploadObject("repo", "main", "delete/");

        boolean delete = fs.delete(path, true);
        Assert.assertTrue(delete);
    }

    protected void caseDeleteDirectoryRecursive(int bulkSize, int numObjects) throws IOException {
        conf.setInt(LakeFSFileSystem.LAKEFS_DELETE_BULK_SIZE, bulkSize);
        expectStatObjectNotFound("repo", "main", "delete/sample");
        expectStatObjectNotFound("repo", "main", "delete/sample/");

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
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("delete/sample/").build(),
                      objects);

        // Set up multiple deleteObjects expectations of bulkSize deletes
        // each (except for the last, which might be smaller).
        for (int start = 0; start < numObjects; start += bulkSize) {
            PathList pl = new PathList();
            for (int i = start; i < numObjects && i < start + bulkSize; i++) {
                pl.addPathsItem(String.format("delete/sample/file%04d.txt", i));
            }
            expectDeleteObjects("repo", "main", pl);
        }
        // Mock parent directory marker creation at end of fs.delete to show
        // the directory marker exists.
        expectUploadObject("repo", "main", "delete/");
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
    public void testCreate() throws IOException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        long contentsLength = (long) contents.getBytes().length;
        Path path = new Path("lakefs://repo/main/sub1/sub2/create.me");

        expectDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path));

        StagingLocation stagingLocation =
            expectGetPhysicalAddress("repo", "main", "sub1/sub2/create.me", "repo-base/create");

        // nothing at path
        expectFileDoesNotExist("repo", "main", "sub1/sub2/create.me");
        // sub1/sub2 was an empty directory with no marker.
        expectStatObjectNotFound("repo", "main", "sub1/sub2/");

        ObjectStats newStats = new ObjectStats()
            .path("sub1/sub2/create.me")
            .pathType(PathTypeEnum.OBJECT)
            .physicalAddress(stagingLocation.getPhysicalAddress()).
            checksum(UNUSED_CHECKSUM).
            mtime(UNUSED_MTIME).
            sizeBytes(UNUSED_FILE_SIZE);

        mockServerClient.when(request()
                              .withMethod("PUT")
                              .withPath("/repositories/repo/branches/main/staging/backing")
                              .withBody(json(gson.toJson(new StagingMetadata()
                                                    .staging(stagingLocation)
                                                    .sizeBytes(contentsLength)),
                                             MatchType.ONLY_MATCHING_FIELDS)))
            .respond(response()
                     .withStatusCode(200)
                     .withBody(gson.toJson(newStats)));

        // Empty dir marker should be deleted.
        expectDeleteObject("repo", "main", "sub1/sub2/");

        OutputStream out = fs.create(path);
        out.write(contents.getBytes());
        out.close();

        // Write succeeded, verify physical file on S3.
        assertS3Object(stagingLocation, contents);
    }

    @Test
    public void testCreateExistingDirectory() throws IOException {
        Path path = new Path("lakefs://repo/main/sub1/sub2/create.me");
        // path is a directory -- so cannot be created as a file.

        expectStatObjectNotFound("repo", "main", "sub1/sub2/create.me");
        ObjectStats stats = new ObjectStats()
            .path("sub1/sub2/create.me/")
            .physicalAddress(s3Url("repo-base/sub1/sub2/create.me"));
        expectStatObject("repo", "main", "sub1/sub2/create.me/", stats);

        Exception e =
            Assert.assertThrows(FileAlreadyExistsException.class, () -> fs.create(path, false));
        Assert.assertThat(e.getMessage(), new StringContains("is a directory"));
    }

    @Test
    public void testCreateExistingFile() throws IOException {
        Path path = new Path("lakefs://repo/main/sub1/sub2/create.me");

        ObjectLocation dir = new ObjectLocation("lakefs", "repo", "main", "sub1/sub2");
        expectStatObject("repo", "main", "sub1/sub2/create.me",
                         new ObjectStats().path("sub1/sub2/create.me"));
        Exception e = Assert.assertThrows(FileAlreadyExistsException.class,
                            () -> fs.create(path, false));
        Assert.assertThat(e.getMessage(), new StringContains("already exists"));
    }

    @Test
    public void testMkdirs() throws IOException {
        // setup empty folder checks
        Path path = new Path("dir1/dir2/dir3");
        for (Path p = new Path(path.toString()); p != null && !p.isRoot(); p = p.getParent()) {
            expectStatObjectNotFound("repo", "main", p.toString());
            expectStatObjectNotFound("repo", "main", p+"/");
            expectListing("repo", "main", ImmutablePagination.builder().prefix(p+"/").build());
        }

        // physical address to directory marker object
        StagingLocation stagingLocation =
            expectGetPhysicalAddress("repo", "main", "dir1/dir2/dir3/", "repo-base/emptyDir");

        ObjectStats newStats = new ObjectStats()
            .path("dir1/dir2/dir3/")
            .physicalAddress(s3Url("repo-base/dir12"));
        expectStatObject("repo", "main", "dir1/dir2/dir3/", newStats);

        mockServerClient.when(request()
                              .withMethod("PUT")
                              .withPath("/repositories/repo/branches/main/staging/backing")
                              .withQueryStringParameter("path", "dir1/dir2/dir3/")
                              .withBody(json(gson.toJson(new StagingMetadata()
                                                         .staging(stagingLocation)
                                                         .sizeBytes(0L)),
                                             MatchType.ONLY_MATCHING_FIELDS)))
            .respond(response()
                     .withStatusCode(200)
                     .withBody(gson.toJson(newStats)));

        // call mkdirs
        Assert.assertTrue(fs.mkdirs(new Path("lakefs://repo/main/", path)));

        // verify file exists on s3
        assertS3Object(stagingLocation, "");
    }

    @Test
    public void testOpen() throws IOException, ApiException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        byte[] contentsBytes = contents.getBytes();
        String physicalPath = sessionId() + "/repo-base/open";
        String physicalKey = createPhysicalAddress(physicalPath);
        int readBufferSize = 5;
        Path path = new Path("lakefs://repo/main/read.me");

        expectStatObject("repo", "main", "read.me",
                         new ObjectStats()
                         .physicalAddress(physicalKey)
                         .checksum(UNUSED_CHECKSUM)
                         .mtime(UNUSED_MTIME)
                         .sizeBytes((long) contentsBytes.length));

        // Write physical file to S3.
        ObjectMetadata s3Metadata = new ObjectMetadata();
        s3Metadata.setContentLength(contentsBytes.length);
        s3Client.putObject(s3Bucket,
                           physicalPath,
                           new ByteArrayInputStream(contentsBytes),
                           s3Metadata);

        try (InputStream in = fs.open(path, readBufferSize)) {
            String actual = IOUtils.toString(in);
            Assert.assertEquals(contents, actual);
        } catch (Exception e) {
            String actualFiles = String.join(", ", getS3FilesByPrefix(""));
            throw new RuntimeException("Files " + actualFiles + "; read " + path.toString() + " from " + physicalKey, e);
        }
    }

    // TODO(ariels): Rename test to "testOpenWithNonAsciiUriChars".
    @Test
    public void testOpenWithInvalidUriChars() throws IOException, ApiException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        byte[] contentsBytes = contents.getBytes();
        int readBufferSize = 5;

        String[] suffixes = {
                "with space/open",
                "wi:th$cha&rs#/%op;e?n",
                "עכשיו/בעברית/open",
                "\uD83E\uDD2F/imoji/open",
        };
        for (String suffix : suffixes) {
            String key = "/repo-base/" + suffix;

            // Write physical file to S3.
            ObjectMetadata s3Metadata = new ObjectMetadata();
            s3Metadata.setContentLength(contentsBytes.length);
            s3Client.putObject(new PutObjectRequest(s3Bucket, key, new ByteArrayInputStream(contentsBytes), s3Metadata));

            String path = String.format("lakefs://repo/main/%s-x", suffix);
            ObjectStats stats = new ObjectStats()
                .physicalAddress(s3Url(key))
                .sizeBytes((long) contentsBytes.length);
            expectStatObject("repo", "main", suffix + "-x", stats);

            try (InputStream in = fs.open(new Path(path), readBufferSize)) {
                String actual = IOUtils.toString(in);
                Assert.assertEquals(contents, actual);
            }
        }
    }

    @Test
    public void testOpen_NotExists() throws IOException, ApiException {
        Path path = new Path("lakefs://repo/main/doesNotExi.st");
        expectStatObjectNotFound("repo", "main", "doesNotExi.st");
        Assert.assertThrows(FileNotFoundException.class,
                            () -> fs.open(path));
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
        expectStatObject("repo", "main", "status/file", objectStats);
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
        expectStatObjectNotFound("repo", "main", "status/file");
        expectStatObjectNotFound("repo", "main", "status/file/");
        expectListing("repo", "main",
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
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("status/").build(),
                      objects);
        expectStatObjectNotFound("repo", "main", "status");

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

        expectStatObject("repo", "main", "existing.src", stats);

        Path dst = new Path("lakefs://repo/main/non-existing/new");

        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("non-existing/").build());
        expectStatObjectNotFound("repo", "main", "non-existing/new");
        expectStatObjectNotFound("repo", "main", "non-existing/new/");
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("non-existing/new/").build());
        expectStatObjectNotFound("repo", "main", "non-existing");
        expectStatObjectNotFound("repo", "main", "non-existing/");

        boolean renamed = fs.rename(src, dst);
        Assert.assertFalse(renamed);
    }

    @Test
    public void testRename_existingFileToExistingFileName() throws IOException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        byte[] contentsBytes = contents.getBytes();

        Path src = new Path("lakefs://repo/main/existing.src");
        ObjectStats srcStats = new ObjectStats()
            .path("existing.src")
            .pathType(PathTypeEnum.OBJECT)
            .physicalAddress("base/existing.src");
        expectStatObject("repo", "main", "existing.src", srcStats);

        Path dst = new Path("lakefs://repo/main/existing.dst");
        ObjectStats dstStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existing.dst")
            .physicalAddress(s3Url("existing.dst"));
        expectStatObject("repo", "main", "existing.dst", dstStats);

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

        expectDeleteObject("repo", "main", "existing.src");

        Assert.assertTrue(fs.rename(src, dst));
    }

    @Test
    public void testRename_existingDirToExistingFileName() throws IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir/existing.src");
        ObjectStats srcStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existing-dir/existing.src");
        Path srcDir = new Path("lakefs://repo/main/existing-dir");
        expectStatObjectNotFound("repo", "main", "existing-dir");
        expectStatObjectNotFound("repo", "main", "existing-dir/");
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("existing-dir/").build(),
                      srcStats);

        Path dst = new Path("lakefs://repo/main/existingdst.file");
        ObjectStats dstStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existingdst.file");
        expectStatObject("repo", "main", "existingdst.file", dstStats);

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
        expectStatObject("repo", "main", "existing-dir1/existing.src", srcStats);

        ObjectStats dstStats = new ObjectStats()
            .pathType(PathTypeEnum.OBJECT)
            .path("existing-dir2/existing.src");
        expectFileDoesNotExist("repo", "main", "existing-dir2");
        expectFileDoesNotExist("repo", "main", "existing-dir2/");
        expectListing("repo", "main",
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
        expectGetBranch("repo", "main");
        expectDeleteObject("repo", "main", "existing-dir1/existing.src");

        // Need a directory marker at the source because it's now empty!
        expectUploadObject("repo", "main", "existing-dir1/");

        Assert.assertTrue(fs.rename(src, dst));
    }

    /**
     * rename(srcDir(containing srcDir/a.txt, srcDir/b.txt), non-existing-dir/new) -> unsupported, rename should fail by returning false
     */
    @Test
    public void testRename_existingDirToNonExistingDirWithoutParent() throws IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir/existing.src");
        Path srcDir = new Path("lakefs://repo/main/existing-dir");

        expectFilesInDir("repo", "main", "existing-dir", "existing.src");

        expectFileDoesNotExist("repo", "main", "x/non-existing-dir");
        expectFileDoesNotExist("repo", "main", "x/non-existing-dir/new");
        // Will also check if parent of destination is a directory (it isn't).
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("x/non-existing-dir/").build());
        expectListing("repo", "main",
                      ImmutablePagination.builder().prefix("x/non-existing-dir/new/").build());

        // Keep a directory marker, or rename will try to create one because
        // it emptied the existing directory.
        expectStatObject("repo", "main", "x",
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

        expectStatObjectNotFound("repo", "main", "existing-dir");
        expectStatObjectNotFound("repo", "main", "existing-dir/");
        expectListing("repo", "main", ImmutablePagination.builder().prefix("existing-dir/").build(),
                      srcStats);

        expectStatObjectNotFound("repo", "main", "existing-dir2");
        expectStatObject("repo", "main", "existing-dir2/",
                         new ObjectStats().pathType(PathTypeEnum.OBJECT).path("existing-dir2/"));

        expectStatObjectNotFound("repo", "main", "existing-dir2/new");
        expectStatObjectNotFound("repo", "main", "existing-dir2/new/");
        expectListing("repo", "main", ImmutablePagination.builder().prefix("existing-dir2/new/").build());

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
        expectDeleteObject("repo", "main", "existing-dir/existing.src");
        // Directory marker no longer required.
        expectDeleteObject("repo", "main", "existing-dir2/");

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
