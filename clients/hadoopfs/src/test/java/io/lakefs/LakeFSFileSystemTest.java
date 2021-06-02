package io.lakefs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.RepositoriesApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.*;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class LakeFSFileSystemTest {
    private static final Long UNUSED_FILE_SIZE = 1L;
    private static final Long UNUSED_MTIME = 0L;
    private static final String UNUSED_CHECKSUM = "unused";

    private static final Long STATUS_FILE_SIZE = 2L;
    private static final Long STATUS_MTIME = 0L;
    private static final String STATUS_CHECKSUM = "status";

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
    public void testDelete_FileExists() throws ApiException, IOException {
        when(objectsApi.statObject("repo", "main", "delete/sample/file.txt"))
                .thenReturn(new ObjectStats().
                        path("lakefs://repo/main/delete/sample/file.txt").
                        pathType(PathTypeEnum.OBJECT).
                        physicalAddress(s3Url("/repo-base/delete")).
                        checksum(UNUSED_CHECKSUM).
                        mtime(UNUSED_MTIME).
                        sizeBytes(UNUSED_FILE_SIZE));
        // return true because file found
        Assert.assertTrue(
                fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), false));
    }

    @Test
    public void testDelete_FileNotExists() throws ApiException, IOException {
        doThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "not found"))
                .when(objectsApi).deleteObject("repo", "main", "no/place/file.txt");
        // return false because file not found
        Assert.assertFalse(
                fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), false));
    }

    @Test
    public void testDelete_DirectoryExists() throws ApiException, IOException {
        doThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "not found"))
                .when(objectsApi).deleteObject("repo", "main", "delete/sample");
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq("delete/sample/"), eq(""), any(), eq("")))
                .thenReturn(new ObjectStatsList().results(Collections.singletonList(new ObjectStats().
                        path("lakefs://repo/main/delete/sample/file.txt").
                        pathType(PathTypeEnum.OBJECT).
                        physicalAddress(s3Url("/repo-base/delete")).
                        checksum(UNUSED_CHECKSUM).
                        mtime(UNUSED_MTIME).
                        sizeBytes(UNUSED_FILE_SIZE))).pagination(new Pagination().hasMore(false)));
        // return false because we can't delete a directory without recursive
        Assert.assertFalse(
                fs.delete(new Path("lakefs://repo/main/delete/sample"), false));
    }

    @Test
    public void testDelete_NotExistsRecursive() throws ApiException, IOException {
        doThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "not found"))
                .when(objectsApi).deleteObject("repo", "main", "no/place/file.txt");
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq("no/place/file.txt/"), eq(""), any(), eq("")))
                .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));
        // recursive will always end successfully
        Assert.assertTrue(
                fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), true));
    }

    @Test
    public void testDelete_ExistsRecursive() throws ApiException, IOException {
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq("delete/sample/"), eq(""), any(), eq("")))
                .thenReturn(new ObjectStatsList().results(Collections.singletonList(new ObjectStats().
                        path("lakefs://repo/main/delete/sample/file.txt").
                        pathType(PathTypeEnum.OBJECT).
                        physicalAddress(s3Url("/repo-base/delete")).
                        checksum(UNUSED_CHECKSUM).
                        mtime(UNUSED_MTIME).
                        sizeBytes(UNUSED_FILE_SIZE))).pagination(new Pagination().hasMore(false)));
        // recursive will always end successfully
        Assert.assertTrue(
                fs.delete(new Path("lakefs://repo/main/delete/sample"), true));
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
                       mtime(UNUSED_MTIME).
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

    @Test
    public void testListStatusFile() throws ApiException, IOException {
        ObjectStats objectStats = new ObjectStats().
                path("status/file").
                pathType(PathTypeEnum.OBJECT).
                physicalAddress(s3Url("/repo-base/status")).
                checksum(STATUS_CHECKSUM).
                mtime(STATUS_MTIME).
                sizeBytes(STATUS_FILE_SIZE);
        when(objectsApi.statObject("repo", "main", "status/file"))
                .thenReturn(objectStats);
        Path p = new Path("lakefs://repo/main/status/file");
        FileStatus[] fileStatuses = fs.listStatus(p);
        LakeFSFileStatus expectedFileStatus = new LakeFSFileStatus.Builder(p)
                .length(STATUS_FILE_SIZE)
                .checksum(STATUS_CHECKSUM)
                .mTime(STATUS_MTIME)
                .physicalAddress(p.toString())
                .blocksize(Constants.DEFAULT_BLOCK_SIZE)
                .build();
        LakeFSFileStatus[] expectedFileStatuses = new LakeFSFileStatus[]{expectedFileStatus};
        Assert.assertArrayEquals(expectedFileStatuses, fileStatuses);
    }

    @Test
    public void testListStatusNotFound() throws ApiException, IOException {
        when(objectsApi.statObject("repo", "main", "status/file"))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq("status/file/"), eq(""),
                any(), eq("/")))
                .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));

        Path p = new Path("lakefs://repo/main/status/file");
        FileStatus[] fileStatuses = fs.listStatus(p);
        // don't expect to find anything - because we don't have the concept of directory
        // we do not throw FileNotFoundException
        Assert.assertArrayEquals(new FileStatus[]{}, fileStatuses);
    }

    @Test
    public void testListStatusDirectory() throws ApiException, IOException {
        int totalObjectsCount = 3;
        ObjectStatsList objects = new ObjectStatsList();
        for (int i = 0; i < totalObjectsCount; i++) {
            ObjectStats objectStats = new ObjectStats().
                    path("status/file" + i).
                    pathType(PathTypeEnum.OBJECT).
                    physicalAddress(s3Url("/repo-base/status" + i)).
                    checksum(STATUS_CHECKSUM).
                    mtime(STATUS_MTIME).
                    sizeBytes(STATUS_FILE_SIZE);
            objects.addResultsItem(objectStats);
        }
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq("status/"), eq(""),
                any(), eq("/")))
                .thenReturn(objects.pagination(new Pagination().hasMore(false)));
        when(objectsApi.statObject("repo", "main", "status"))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

        Path dir = new Path("lakefs://repo/main/status");
        FileStatus[] fileStatuses = fs.listStatus(dir);
        FileStatus[] expectedFileStatuses = new LakeFSLocatedFileStatus[totalObjectsCount];
        for (int i = 0; i < totalObjectsCount; i++) {
            Path p = new Path(dir + "/file" + i);
            LakeFSFileStatus fileStatus = new LakeFSFileStatus.Builder(p)
                    .length(STATUS_FILE_SIZE)
                    .checksum(STATUS_CHECKSUM)
                    .mTime(STATUS_MTIME)
                    .blocksize(Constants.DEFAULT_BLOCK_SIZE)
                    .physicalAddress(s3Url("/repo-base/status" + i))
                    .build();
            expectedFileStatuses[i] = new LakeFSLocatedFileStatus(fileStatus, null);
        }
        Assert.assertArrayEquals(expectedFileStatuses, fileStatuses);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAppend() throws IOException {
        fs.append(null, 0, null);
    }

    private void mockNonExistingPath(ObjectLocation objectLoc) throws ApiException {
        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath()))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

        when(objectsApi.listObjects(eq(objectLoc.getRepository()), eq(objectLoc.getRef()),
                eq(objectLoc.getPath() + Constants.SEPARATOR), eq(""), any(), eq("")))
                .thenReturn(new ObjectStatsList().pagination(new Pagination().hasMore(false)));
    }

    private void mockExistingDirPath(ObjectLocation dirObjLoc, List<ObjectLocation> filesInDir) throws ApiException {
        // io.lakefs.LakeFSFileSystem.getFileStatus tries to get object stats, when it can't find an object if will
        // fall back to try listing items under this path to discover the objects it contains. if objects are found,
        // then the path considered a directory.
        when(objectsApi.statObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath()))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

        // Mock the files under this directory
        ObjectStatsList stats = new ObjectStatsList();
        for (ObjectLocation loc : filesInDir) {
            ObjectStats fileStat = mockExistingFilePath(loc);
            stats.addResultsItem(fileStat).setPagination(new Pagination().hasMore(false));
        }

        // Mock listing the files under this directory
        when(objectsApi.listObjects(eq(dirObjLoc.getRepository()), eq(dirObjLoc.getRef()),
                eq(dirObjLoc.getPath() + Constants.SEPARATOR), eq(""), any(), eq("")))
                .thenReturn(stats);
    }

    private ObjectStats mockExistingFilePath(ObjectLocation objectLoc) throws ApiException {
        String key = objectLocToS3ObjKey(objectLoc);
        ObjectStats srcStats = new ObjectStats()
                .path(objectLoc.getPath())
                .sizeBytes(UNUSED_FILE_SIZE)
                .mtime(UNUSED_MTIME)
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
        verify(objectsApi).deleteObject(srcObjLoc.getRepository(), srcObjLoc.getRef(), srcObjLoc.getPath());
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
        Assert.assertTrue(renamed);
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
        mockExistingDirPath(dstObjLoc, ImmutableList.of(fileObjLoc));

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
        mockExistingDirPath(srcDirObjLoc, ImmutableList.of(fileObjLoc));

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
        Path firstSrcFile = new Path("lakefs://repo/main/existing-dir1/a.src");
        ObjectLocation firstObjLoc = fs.pathToObjectLocation(firstSrcFile);
        Path secSrcFile = new Path("lakefs://repo/main/existing-dir1/b.src");
        ObjectLocation secObjLoc = fs.pathToObjectLocation(secSrcFile);

        Path srcDir = new Path("lakefs://repo/main/existing-dir1");
        ObjectLocation srcDirObjLoc = fs.pathToObjectLocation(srcDir);
        mockExistingDirPath(srcDirObjLoc, ImmutableList.of(firstObjLoc, secObjLoc));

        Path fileInDstDir = new Path("lakefs://repo/main/existing-dir2/file.dst");
        ObjectLocation dstFileObjLoc = fs.pathToObjectLocation(fileInDstDir);
        Path dstDir = new Path("lakefs://repo/main/existing-dir2");
        ObjectLocation dstDirObjLoc = fs.pathToObjectLocation(dstDir);
        mockExistingDirPath(dstDirObjLoc, ImmutableList.of(dstFileObjLoc));

        boolean renamed = fs.rename(srcDir, dstDir);
        Assert.assertTrue(renamed);
        Path firstExpectedDst = new Path("lakefs://repo/main/existing-dir2/existing-dir1/a.src");
        Path secExpectedDst = new Path("lakefs://repo/main/existing-dir2/existing-dir1/b.src");
        Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(firstObjLoc, fs.pathToObjectLocation(firstExpectedDst)));
        Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(secObjLoc, fs.pathToObjectLocation(secExpectedDst)));
        verifyObjDeletion(firstObjLoc);
        verifyObjDeletion(secObjLoc);
    }

    @Test
    public void testRename_srcAndDstOnDifferentBranch() throws IOException, ApiException {
        Path src = new Path("lakefs://repo/branch/existing.src");
        Path dst = new Path("lakefs://repo/another-branch/existing.dst");
        boolean renamed = fs.rename(src, dst);
        Assert.assertFalse(renamed);
        Mockito.verify(objectsApi, never()).statObject(any(), any(), any());
        Mockito.verify(objectsApi, never()).stageObject(any(), any(), any(), any());
        Mockito.verify(objectsApi, never()).deleteObject(any(), any(), any());
    }

    /**
     * no-op. rename is expected to succeed.
     */
    @Test
    public void testRename_srcEqualsDst() throws IOException, ApiException {
        Path src = new Path("lakefs://repo/main/existing.src");
        Path dst = new Path("lakefs://repo/main/existing.src");
        boolean renamed = fs.rename(src, dst);
        Assert.assertTrue(renamed);
        Mockito.verify(objectsApi, never()).statObject(any(), any(), any());
        Mockito.verify(objectsApi, never()).stageObject(any(), any(), any(), any());
        Mockito.verify(objectsApi, never()).deleteObject(any(), any(), any());
    }

    @Test
    public void testRename_nonExistingSrcFile() throws ApiException, IOException {
        Path src = new Path("lakefs://repo/main/non-existing.src");
        ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);
        mockNonExistingPath(srcObjLoc);

        Path dst = new Path("lakefs://repo/main/existing.dst");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);
        mockExistingFilePath(dstObjLoc);

        boolean renamed = fs.rename(src, dst);
        Assert.assertFalse(renamed);
    }
}
