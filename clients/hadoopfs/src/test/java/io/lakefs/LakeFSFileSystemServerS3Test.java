package io.lakefs;

import io.lakefs.clients.api.model.*;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import io.lakefs.clients.api.ApiException;
import io.lakefs.utils.ObjectLocation;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;

import com.amazonaws.services.s3.model.*;
import org.junit.Assert;
import org.junit.Test;
import org.hamcrest.core.StringContains;

import org.mockserver.matchers.MatchType;

import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

import java.io.*;

public class LakeFSFileSystemServerS3Test extends S3FSTestBase {
    // TODO(ariels): Override and make abstract!
    protected String createPhysicalAddress(String key) {
        return s3Url(key);
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
}
