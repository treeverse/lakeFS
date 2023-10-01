package io.lakefs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.model.*;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import io.lakefs.utils.ObjectLocation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpStatus;

import org.immutables.value.Value;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.matchers.MatchType;
import org.mockserver.matchers.TimeToLive;
import org.mockserver.matchers.Times;
import org.mockserver.model.Cookie;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;

import static org.apache.commons.lang3.StringUtils.removeStart;

import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Base for all LakeFSFilesystem tests.  Helps set common components up but
 * contains no tests of its own.
 *
 * See e.g. "Base Test Class Testing Pattern: Why and How to use",
 * https://eliasnogueira.com/base-test-class-testing-pattern-why-and-how-to-use/
 */
abstract class FSTestBase {
    static protected final Long UNUSED_FILE_SIZE = 1L;
    static protected final Long UNUSED_MTIME = 0L;
    static protected final String UNUSED_CHECKSUM = "unused";

    static protected final Long STATUS_FILE_SIZE = 2L;
    static protected final Long STATUS_MTIME = 123456789L;
    static protected final String STATUS_CHECKSUM = "status";

    protected Configuration conf;
    protected final LakeFSFileSystem fs = new LakeFSFileSystem();

    protected String s3Base;
    protected String s3Bucket;

    protected static final String S3_ACCESS_KEY_ID = "AKIArootkey";
    protected static final String S3_SECRET_ACCESS_KEY = "secret/minio/key=";

    protected static final ApiException noSuchFile = new ApiException(HttpStatus.SC_NOT_FOUND, "no such file");

    protected final Gson gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();

    @Value.Immutable static public interface Pagination {
        @Value.Parameter Optional<Integer> amount();
        @Value.Parameter Optional<String> after();
        @Value.Parameter Optional<String> prefix();
    }

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

    /**
     * Override to add to Hadoop configuration.
     */
    protected void addHadoopConfiguration(Configuration conf) {
    }

    @Before
    public void hadoopSetup() throws IOException, URISyntaxException {
        conf = new Configuration(false);

        addHadoopConfiguration(conf);

        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem");

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

        // Always expect repo "repo" to be found, it's used in all tests.
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath("/repositories/repo"))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(new Repository().id("repo")
                                           .creationDate(1234L)
                                           .defaultBranch("main")
                                           // Not really needed, just put something that works.
                                           .storageNamespace("s3a://FIX/ME?"))));

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
}
