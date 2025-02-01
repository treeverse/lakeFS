package io.lakefs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lakefs.clients.sdk.model.*;
import io.lakefs.clients.sdk.model.ObjectStats.PathTypeEnum;
import io.lakefs.clients.sdk.ApiException;
import io.lakefs.utils.ObjectLocation;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.*;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;import org.hamcrest.core.StringContains;

import org.mockserver.matchers.MatchType;

import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

import java.io.*;
import java.net.URL;
import java.util.Date;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class LakeFSFileSystemServerS3Test extends S3FSTestBase {
    static private final Logger LOG = LoggerFactory.getLogger(LakeFSFileSystemServerS3Test.class);

    public static interface PhysicalAddressCreator {
        default void initConfiguration(Configuration conf) {}
        String createGetPhysicalAddress(S3FSTestBase o, String key);
        StagingLocation createPutStagingLocation(S3FSTestBase o, String namespace, String repo, String branch, String path);
    }

    @Parameters(name="{1}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {new SimplePhysicalAddressCreator(), "simple"},
                {new PresignedPhysicalAddressCreator(), "presigned"}});
    }

    @Parameter(1)
    public String unusedAddressCreatorType;

    @Parameter(0)
    public PhysicalAddressCreator pac;

    static private class SimplePhysicalAddressCreator implements PhysicalAddressCreator {
        public String createGetPhysicalAddress(S3FSTestBase o, String key) {
            return o.s3Url(key);
        }

        public StagingLocation createPutStagingLocation(S3FSTestBase o, String namespace, String repo, String branch, String path) {
            String fullPath = String.format("%s/%s/%s/%s/%s-object",
                                            o.sessionId(), namespace, repo, branch, path);
            return new StagingLocation().physicalAddress(o.s3Url(fullPath));
        }
    }

    static private class PresignedPhysicalAddressCreator implements PhysicalAddressCreator {
        public void initConfiguration(Configuration conf) {
            conf.set("fs.lakefs.access.mode", "presigned");
        }

        protected Date getExpiration() {
            return new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
        }

        public String createGetPhysicalAddress(S3FSTestBase o, String key) {
            Date expiration = getExpiration();
            URL presignedUrl =
                o.s3Client.generatePresignedUrl(new GeneratePresignedUrlRequest(o.s3Bucket, key)
                                              .withMethod(HttpMethod.GET)
                                              .withExpiration(expiration));
            return presignedUrl.toString();
        }

        public StagingLocation createPutStagingLocation(S3FSTestBase o, String namespace, String repo, String branch, String path) {
            String fullPath = String.format("%s/%s/%s/%s/%s-object",
                                            o.sessionId(), namespace, repo, branch, path);
            Date expiration = getExpiration();
            URL presignedUrl =
                o.s3Client.generatePresignedUrl(new GeneratePresignedUrlRequest(o.s3Bucket, fullPath)
                                              .withMethod(HttpMethod.PUT)
                                              .withExpiration(expiration));
            return new StagingLocation()
                .physicalAddress(o.s3Url(fullPath))
                .presignedUrl(presignedUrl.toString());
        }
    }

    @Override
    protected void moreHadoopSetup() {
        super.moreHadoopSetup();
        pac.initConfiguration(conf);
    }

    // Return a location under namespace for this getPhysicalAddress call.
    protected StagingLocation mockGetPhysicalAddress(String repo, String branch, String path, String namespace) {
        StagingLocation stagingLocation =
            pac.createPutStagingLocation(this, namespace, repo, branch, path);
        mockServerClient.when(request()
                              .withMethod("GET")
                              .withPath(String.format("/repositories/%s/branches/%s/staging/backing", repo, branch))
                              .withQueryStringParameter("path", path))
            .respond(response().withStatusCode(200)
                     .withBody(gson.toJson(stagingLocation)));
        return stagingLocation;
    }

    @Test
    public void testCreate() throws IOException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        long contentsLength = (long) contents.getBytes().length;
        Path path = new Path("lakefs://repo/main/sub1/sub2/create.me");

        mockDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path));

        StagingLocation stagingLocation =
            mockGetPhysicalAddress("repo", "main", "sub1/sub2/create.me", "repo-base/create");

        // nothing at path
        mockFileDoesNotExist("repo", "main", "sub1/sub2/create.me");
        // sub1/sub2 was an empty directory with no marker.
        mockStatObjectNotFound("repo", "main", "sub1/sub2/");

        ObjectStats newStats = makeObjectStats("sub1/sub2/create.me")
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
        mockDeleteObject("repo", "main", "sub1/sub2/");

        OutputStream out = fs.create(path);
        out.write(contents.getBytes());
        out.close();

        // Write succeeded, verify physical file on S3.
        assertS3Object(stagingLocation, contents);
    }

    @Test
    public void testMkdirs() throws IOException {
        // setup empty folder checks
        Path path = new Path("dir1/dir2/dir3");
        for (Path p = new Path(path.toString()); p != null && !p.isRoot(); p = p.getParent()) {
            mockStatObjectNotFound("repo", "main", p.toString());
            mockStatObjectNotFound("repo", "main", p+"/");
            mockListing("repo", "main", Pagination.builder().prefix(p+"/").build());
        }

        // physical address to directory marker object
        StagingLocation stagingLocation =
            mockGetPhysicalAddress("repo", "main", "dir1/dir2/dir3/", "repo-base/emptyDir");

        ObjectStats newStats = makeObjectStats("dir1/dir2/dir3/")
            .physicalAddress(pac.createGetPhysicalAddress(this, "repo-base/dir12"));
        mockStatObject("repo", "main", "dir1/dir2/dir3/", newStats);

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
    public void testCreateExistingDirectory() throws IOException {
        Path path = new Path("lakefs://repo/main/sub1/sub2/create.me");
        // path is a directory -- so cannot be created as a file.

        mockStatObjectNotFound("repo", "main", "sub1/sub2/create.me");
        ObjectStats stats = makeObjectStats("sub1/sub2/create.me/")
            .physicalAddress(pac.createGetPhysicalAddress(this, "repo-base/sub1/sub2/create.me"));
        mockStatObject("repo", "main", "sub1/sub2/create.me/", stats);

        Exception e =
            Assert.assertThrows(FileAlreadyExistsException.class, () -> fs.create(path, false));
        Assert.assertThat(e.getMessage(), new StringContains("is a directory"));
    }

    @Test
    public void testCreateExistingFile() throws IOException {
        Path path = new Path("lakefs://repo/main/sub1/sub2/create.me");

        ObjectLocation dir = new ObjectLocation("lakefs", "repo", "main", "sub1/sub2");
        mockStatObject("repo", "main", "sub1/sub2/create.me",
                       makeObjectStats("sub1/sub2/create.me"));
        Exception e = Assert.assertThrows(FileAlreadyExistsException.class,
                            () -> fs.create(path, false));
        Assert.assertThat(e.getMessage(), new StringContains("already exists"));
    }

    @Test
    public void testOpen() throws IOException, ApiException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        byte[] contentsBytes = contents.getBytes();
        String physicalPath = sessionId() + "/repo-base/open";
        String physicalKey = pac.createGetPhysicalAddress(this, physicalPath);
        int readBufferSize = 5;
        Path path = new Path("lakefs://repo/main/read.me");

        mockStatObject("repo", "main", "read.me",
                       makeObjectStats("read.me")
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
            ObjectStats stats = makeObjectStats(suffix + "-x")
                .physicalAddress(pac.createGetPhysicalAddress(this, key))
                .sizeBytes((long) contentsBytes.length);
            mockStatObject("repo", "main", suffix + "-x", stats);

            try (InputStream in = fs.open(new Path(path), readBufferSize)) {
                String actual = IOUtils.toString(in);
                Assert.assertEquals(contents, actual);
            }
        }
    }

    @Test
    public void testOpen_NotExists() throws IOException, ApiException {
        Path path = new Path("lakefs://repo/main/doesNotExi.st");
        mockStatObjectNotFound("repo", "main", "doesNotExi.st");
        Assert.assertThrows(FileNotFoundException.class,
                            () -> fs.open(path));
    }
}
