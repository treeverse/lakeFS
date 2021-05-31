package io.lakefs;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.lakefs.clients.api.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.RepositoriesApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;

public class LakeFSFileSystemTest {
    protected static final Logger LOG = LoggerFactory.getLogger(LakeFSFileSystemTest.class);
    private static final Long FILE_SIZE = 1L;
    private static final Long MTIME = 0L;
    private static final String UNUSED_CHECKSUM = "unused";

    protected final LakeFSFileSystem fs = new LakeFSFileSystem();

    protected LakeFSClient lfsClient;
    protected ObjectsApi objectsApi;
    protected RepositoriesApi repositoriesApi;
    protected StagingApi stagingApi;

    protected AmazonS3 s3Client;

    protected String s3Base;
    protected String s3Bucket;

    private static final DockerImageName MINIO = DockerImageName.parse("minio/minio:RELEASE.2021-05-16T05-32-34Z");
    protected static final String S3_ACCESS_KEY_ID = "AKIArootkey";
    protected static final String S3_SECRET_ACCESS_KEY = "secret/minio/key=";

    @Rule
    public final GenericContainer s3 = new GenericContainer(MINIO.toString()).
        withCommand("minio", "server", "/data").
        withEnv("MINIO_ROOT_USER", S3_ACCESS_KEY_ID).
        withEnv("MINIO_ROOT_PASSWORD", S3_SECRET_ACCESS_KEY).
        withEnv("MINIO_DOMAIN", "s3.local.lakefs.io").
        withExposedPorts(9000);

    protected static String makeS3BucketName() {
        String slug = NanoIdUtils.randomNanoId(NanoIdUtils.DEFAULT_NUMBER_GENERATOR,
                                               "abcdefghijklmnopqrstuvwxyz-0123456789".toCharArray(), 14);
        return String.format("bucket-%s-x", slug);
    }

    /** @return "s3://..." URL to use for s3Path (which does not start with a slash) on bucket */
    protected String s3Url(String s3Path) {
        return s3Base + s3Path;
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

        Configuration conf = new Configuration(false);

        conf.set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem");
        conf.set(Constants.FS_LAKEFS_ACCESS_KEY, "<lakefs key>");
        conf.set(Constants.FS_LAKEFS_SECRET_KEY, "<lakefs secret>");
        conf.set(Constants.FS_LAKEFS_ENDPOINT_KEY, "http://unused.invalid");

        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set(org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY, S3_ACCESS_KEY_ID);
        conf.set(org.apache.hadoop.fs.s3a.Constants.SECRET_KEY, S3_SECRET_ACCESS_KEY);
        conf.set(org.apache.hadoop.fs.s3a.Constants.ENDPOINT, s3Endpoint);
        conf.set(org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR, "/tmp/s3a");

        System.setProperty("hadoop.home.dir", "/");

        lfsClient = mock(LakeFSClient.class);
        objectsApi = mock(ObjectsApi.class, Answers.RETURNS_SMART_NULLS);
        when(lfsClient.getObjects()).thenReturn(objectsApi);
        repositoriesApi = mock(RepositoriesApi.class, Answers.RETURNS_SMART_NULLS);
        when(lfsClient.getRepositories()).thenReturn(repositoriesApi);
        stagingApi = mock(StagingApi.class, Answers.RETURNS_SMART_NULLS);
        when(lfsClient.getStaging()).thenReturn(stagingApi);

        when(repositoriesApi.getRepository("repo"))
            .thenReturn(new Repository().storageNamespace(s3Url("/repo-base")));

        fs.initializeWithClient(new URI("lakefs://repo/main/file.txt"), conf, lfsClient);
    }

    /**
     * @return all pathnames under s3Prefix that start with prefix.  (Obvious not scalable!)
     */
    protected List<String> getS3FilesByPrefix(String prefix) throws IOException {
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
    public void getUri() throws URISyntaxException, IOException {
        URI u = fs.getUri();
        Assert.assertNotNull(u);
    }

    @Test
    public void testExists_Exists() throws ApiException, IOException {
        Path p = new Path("lakefs://repo/main/exis.ts");
        when(objectsApi.statObject("repo", "main", "exis.ts"))
            .thenReturn(new ObjectStats());

        Assert.assertTrue(fs.exists(p));
    }
    @Test
    public void testExists_NotExists() throws ApiException, IOException {
        Path p = new Path("lakefs://repo/main/doesNotExi.st");
        when(objectsApi.statObject(any(), any(), any()))
            .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
        when(objectsApi.listObjects(any(), any(), any(), any(), any(), any()))
            .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));

        Assert.assertFalse(fs.exists(p));
    }

    @Test
    public void testCreate() throws ApiException, IOException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        Path p = new Path("lakefs://repo/main/create.me");

        StagingLocation stagingLocation = new StagingLocation().token("foo").physicalAddress(s3Url("/repo-base/create"));

        when(stagingApi.getPhysicalAddress("repo", "main", "create.me"))
            .thenReturn(stagingLocation);

        OutputStream out = fs.create(p);
        out.write(contents.getBytes());
        out.close();

        ArgumentCaptor<StagingMetadata> metadataCapture = ArgumentCaptor.forClass(StagingMetadata.class);
        verify(stagingApi).linkPhysicalAddress(eq("repo"), eq("main"), eq("create.me"),
                                               metadataCapture.capture());
        StagingMetadata actualMetadata = metadataCapture.getValue();
        Assert.assertEquals(stagingLocation, actualMetadata.getStaging());
        Assert.assertEquals(contents.getBytes().length, (long)actualMetadata.getSizeBytes());

        // Write succeeded, verify physical file on S3.
        S3Object ret = s3Client.getObject(new GetObjectRequest(s3Bucket, "/repo-base/create"));
        InputStream in = ret.getObjectContent();
        String actual = IOUtils.toString(in);

        Assert.assertEquals(contents, actual);

        List<String> actualFiles = getS3FilesByPrefix("/");
        Assert.assertEquals(ImmutableList.of("repo-base/create"), actualFiles);
    }

    @Test
    public void testOpen() throws ApiException, IOException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        byte[] contentsBytes = contents.getBytes();

        String key = "/repo-base/open";

        // Write physical file to S3.
        ObjectMetadata s3Metadata = new ObjectMetadata();
        s3Metadata.setContentLength(contentsBytes.length);
        s3Client.putObject(new PutObjectRequest(s3Bucket, key, new ByteArrayInputStream(contentsBytes), s3Metadata));

        Path p = new Path("lakefs://repo/main/read.me");
        when(objectsApi.statObject("repo", "main", "read.me")).
            thenReturn(new ObjectStats().
                       path(p.toString()).
                       pathType(PathTypeEnum.OBJECT).
                       physicalAddress(s3Url(key)).
                       checksum(UNUSED_CHECKSUM).
                       mtime(MTIME).
                       sizeBytes((long)contentsBytes.length));

        InputStream in = fs.open(p);

        String actual = IOUtils.toString(in);

        Assert.assertEquals(contents, actual);
    }

    /*
    @Test
    public void listFiles() throws IOException, URISyntaxException {
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("lakefs://example1/master"), true);
        List<LocatedFileStatus> l = new ArrayList<>();
        while (it.hasNext()) {
            l.add(it.next());
        }
        // expected 'l' to include all the files in branch - no directory will be listed, with or without recursive

        Configuration conf = new Configuration(false);
        conf.set(org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY, "<s3a key>");
        conf.set(org.apache.hadoop.fs.s3a.Constants.SECRET_KEY, "<s3a secret>");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        FileSystem fs2 = FileSystem.get(new URI("s3a://bucket/"), conf);
        RemoteIterator<LocatedFileStatus> it2 = fs2.listFiles(new Path("s3a://bucket"), true);
        List<LocatedFileStatus> l2 = new ArrayList<>();
        while (it2.hasNext()) {
            l2.add(it2.next());
        }
        // expected 'l2' to include all the files in bucket - no directory will be listed, with or without recursive
    }
     */

    @Test(expected = UnsupportedOperationException.class)
    public void testAppend() throws IOException {
        fs.append(null, 0, null);
    }

    private void mockNonExistingPath(ObjectLocation objectLoc) throws ApiException {
        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath()))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
        when(objectsApi.listObjects(objectLoc.getRepository(), objectLoc.getRef(),
                objectLoc.getPath() + Constants.SEPARATOR, "", 1, ""))
                .thenReturn(new ObjectStatsList());
    }

    private void mockExistingDirPath(ObjectLocation dirObjLoc, ObjectLocation fileInDir) throws ApiException {
        when(objectsApi.statObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath()))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

        ObjectStats fileStat = mockExistingFilePath(fileInDir);
        ObjectStatsList filesInDir = new ObjectStatsList();
        filesInDir.addResultsItem(fileStat).setPagination(new Pagination().hasMore(false));

        when(objectsApi.listObjects(dirObjLoc.getRepository(), dirObjLoc.getRef(),
                dirObjLoc.getPath() + Constants.SEPARATOR, "", 1, ""))
                .thenReturn(filesInDir);
        when(objectsApi.listObjects(dirObjLoc.getRepository(), dirObjLoc.getRef(),
                dirObjLoc.getPath() + Constants.SEPARATOR, "", 1000, ""))
                .thenReturn(filesInDir);
    }

    private ObjectStats mockExistingFilePath(ObjectLocation objectLoc) throws ApiException {
        String key = objectLocToS3ObjKey(objectLoc);
        ObjectStats srcStats = new ObjectStats()
                .path(objectLoc.getPath())
                .sizeBytes(FILE_SIZE)
                .mtime(MTIME)
                .pathType(PathTypeEnum.OBJECT)
                .physicalAddress(s3Url(key))
                .checksum(UNUSED_CHECKSUM);
        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath())).thenReturn(srcStats);
        return srcStats;
    }

    private String objectLocToS3ObjKey(ObjectLocation objectLoc) {
        return String.format("/%s/%s/%s",objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
    }


    private void verifyObjDeletion(ObjectLocation srcObjLoc) throws ApiException {
        verify(objectsApi).deleteObject(eq(srcObjLoc.getRepository()), eq(srcObjLoc.getRef()), eq(srcObjLoc.getPath()));
    }

    private boolean dstPathLinkedToSrcPhysicalAddress(ObjectLocation srcObjLoc, ObjectLocation dstObjLoc) throws ApiException {
        ArgumentCaptor<ObjectStageCreation> creationReqCapture = ArgumentCaptor.forClass(ObjectStageCreation.class);
        verify(objectsApi).stageObject(eq(dstObjLoc.getRepository()), eq(dstObjLoc.getRef()), eq(dstObjLoc.getPath()),
                creationReqCapture.capture());
        ObjectStageCreation actualCreationReq = creationReqCapture.getValue();
        // Rename is a metadata operation, therefore the dst name is expected to link to the src physical address.
        String expectedPhysicalAddress = s3Url(objectLocToS3ObjKey(srcObjLoc));
        return expectedPhysicalAddress.equals(actualCreationReq.getPhysicalAddress());
    }

    /**
     * rename(src.txt, non-existing-dst) -> non-existing-dst, non-existing-dst is a file
     */
    @Test
    public void testRename_existingFileToNonExistingDst() throws IOException, ApiException {
        Path src = new Path("lakefs://repo/main/existing.src");
        ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);
        mockExistingFilePath(srcObjLoc);

        Path dst = new Path("lakefs://repo/main/non-existing.dst");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);
        mockNonExistingPath(dstObjLoc);

        boolean renamed = fs.rename(src, dst);
        Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(srcObjLoc, dstObjLoc));
        verifyObjDeletion(srcObjLoc);
    }

    /**
     * file -> existing-file-name: rename(src.txt, existing-dst.txt) -> existing-dst.txt, existing-dst.txt is overridden
     */
    @Test
    public void testRename_existingFileToExistingFileName() throws ApiException, IOException {
        Path src = new Path("lakefs://repo/main/existing.src");
        ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);
        mockExistingFilePath(srcObjLoc);

        Path dst = new Path("lakefs://repo/main/existing.dst");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);
        mockExistingFilePath(dstObjLoc);

        boolean renamed = fs.rename(src, dst);
        Assert.assertTrue(renamed);
        Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(srcObjLoc, dstObjLoc));
        verifyObjDeletion(srcObjLoc);
    }

    /**
     * file -> existing-directory-name: rename(src.txt, existing-dstdir) -> existing-dstdir/src.txt
     */
    @Test
    public void testRename_existingFileToExistingDirName() throws ApiException, IOException {
        Path src = new Path("lakefs://repo/main/existing-dir1/existing.src");
        ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);
        mockExistingFilePath(srcObjLoc);

        Path fileInDstDir = new Path("lakefs://repo/main/existing-dir2/existing.src");
        ObjectLocation fileObjLoc = fs.pathToObjectLocation(fileInDstDir);
        Path dst = new Path("lakefs://repo/main/existing-dir2");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);
        mockExistingDirPath(dstObjLoc, fileObjLoc);

        boolean renamed = fs.rename(src, dst);
        Assert.assertTrue(renamed);
        Path expectedDstPath = new Path("lakefs://repo/main/existing-dir2/existing-dir1/existing.src");
        Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(srcObjLoc, fs.pathToObjectLocation(expectedDstPath)));
        verifyObjDeletion(srcObjLoc);
    }

    /**
     * rename(srcDir(containing srcDir/a.txt, srcDir/b.txt), non-existing-dstdir) -> non-existing-dstdir/a.txt, non-existing-dstdir/b.txt
     */
    @Test
    public void testRename_existingDirToNonExistingDirName() throws ApiException, IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir/existing.src");
        ObjectLocation fileObjLoc = fs.pathToObjectLocation(fileInSrcDir);
        Path srcDir = new Path("lakefs://repo/main/existing-dir");
        ObjectLocation srcDirObjLoc = fs.pathToObjectLocation(srcDir);
        mockExistingDirPath(srcDirObjLoc, fileObjLoc);

        Path dst = new Path("lakefs://repo/main/non-existing-dir");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);
        mockNonExistingPath(dstObjLoc);

        boolean renamed = fs.rename(srcDir, dst);
        Assert.assertTrue(renamed);
        Path expectedDstPath = new Path("lakefs://repo/main/non-existing-dir/existing.src");
        Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(fileObjLoc, fs.pathToObjectLocation(expectedDstPath)));
        verifyObjDeletion(fileObjLoc);
    }

    /**
     * rename(srcDir(containing srcDir/a.txt), existing-dstdir) -> existing-dstdir/srcDir/a.txt
     */
    @Test
    public void testRename_existingDirToExistingDirName() throws ApiException, IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir1/existing.src");
        ObjectLocation srcFileObjLoc = fs.pathToObjectLocation(fileInSrcDir);
        Path srcDir = new Path("lakefs://repo/main/existing-dir1");
        ObjectLocation srcDirObjLoc = fs.pathToObjectLocation(srcDir);
        mockExistingDirPath(srcDirObjLoc, srcFileObjLoc);

        Path fileInDstDir = new Path("lakefs://repo/main/existing-dir2/file.dst");
        ObjectLocation dstFileObjLoc = fs.pathToObjectLocation(fileInDstDir);
        Path dstDir = new Path("lakefs://repo/main/existing-dir2");
        ObjectLocation dstDirObjLoc = fs.pathToObjectLocation(dstDir);
        mockExistingDirPath(dstDirObjLoc, dstFileObjLoc);

        boolean renamed = fs.rename(srcDir, dstDir);
        Assert.assertTrue(renamed);
        Path expectedDstPath = new Path("lakefs://repo/main/existing-dir2/existing-dir1/existing.src");
        Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(srcFileObjLoc, fs.pathToObjectLocation(expectedDstPath)));
        verifyObjDeletion(srcFileObjLoc);
    }

    @Test
    public void testRename_srcAndDstOnDifferentBranch() throws IOException {
        Path src = new Path("lakefs://repo/branch/existing.src");
        ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);

        Path dst = new Path("lakefs://repo/another-branch/existing.dst");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);

        boolean renamed = fs.rename(src, dst);
        Assert.assertFalse(renamed);
    }

    /**
     * no-op. rename is expected to succeed.
     */
    @Test
    public void testRename_srcEqualsDst() throws IOException {
        Path src = new Path("lakefs://repo/main/existing.src");
        ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);

        Path dst = new Path("lakefs://repo/main/existing.src");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);

        boolean renamed = fs.rename(src, dst);
        Assert.assertTrue(renamed);
    }
}
