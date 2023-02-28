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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
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
import java.util.Arrays;
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

    protected Configuration conf;
    protected final LakeFSFileSystem fs = new LakeFSFileSystem();

    protected LakeFSClient lfsClient;
    protected ObjectsApi objectsApi;
    protected BranchesApi branchesApi;
    protected RepositoriesApi repositoriesApi;
    protected StagingApi stagingApi;
    protected ConfigApi configApi;

    protected AmazonS3 s3Client;

    protected String s3Base;
    protected String s3Bucket;

    private static final DockerImageName MINIO = DockerImageName.parse("minio/minio:RELEASE.2021-06-07T21-40-51Z");
    protected static final String S3_ACCESS_KEY_ID = "AKIArootkey";
    protected static final String S3_SECRET_ACCESS_KEY = "secret/minio/key=";

    protected static final ApiException noSuchFile = new ApiException(HttpStatus.SC_NOT_FOUND, "no such file");

    @Rule
    public final GenericContainer s3 = new GenericContainer(MINIO.toString()).
        withCommand("minio", "server", "/data").
        withEnv("MINIO_ROOT_USER", S3_ACCESS_KEY_ID).
        withEnv("MINIO_ROOT_PASSWORD", S3_SECRET_ACCESS_KEY).
        withEnv("MINIO_DOMAIN", "s3.local.lakefs.io").
        withEnv("MINIO_UPDATE", "off").
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

        conf = new Configuration(false);

        conf.set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem");

        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set(org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY, S3_ACCESS_KEY_ID);
        conf.set(org.apache.hadoop.fs.s3a.Constants.SECRET_KEY, S3_SECRET_ACCESS_KEY);
        conf.set(org.apache.hadoop.fs.s3a.Constants.ENDPOINT, s3Endpoint);
        conf.set(org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR, "/tmp/s3a");

        System.setProperty("hadoop.home.dir", "/");

        // Return the *same* mock for each client.  Otherwise it is too hard
        // to program _which_ client should do *what*.  There is no risk of
        // blocking, this client is synchronous.

        lfsClient = mock(LakeFSClient.class, Answers.RETURNS_SMART_NULLS);
        objectsApi = mock(ObjectsApi.class, Answers.RETURNS_SMART_NULLS);
        when(lfsClient.getObjectsApi()).thenReturn(objectsApi);
        branchesApi = mock(BranchesApi.class, Answers.RETURNS_SMART_NULLS);
        when(lfsClient.getBranchesApi()).thenReturn(branchesApi);
        repositoriesApi = mock(RepositoriesApi.class, Answers.RETURNS_SMART_NULLS);
        when(lfsClient.getRepositoriesApi()).thenReturn(repositoriesApi);
        stagingApi = mock(StagingApi.class, Answers.RETURNS_SMART_NULLS);
        when(lfsClient.getStagingApi()).thenReturn(stagingApi);
        configApi = mock(ConfigApi.class, Answers.RETURNS_SMART_NULLS);
        when(lfsClient.getConfigApi()).thenReturn(configApi);
        when(configApi.getStorageConfig())
            .thenReturn(new StorageConfig().blockstoreType("s3").blockstoreNamespaceValidityRegex("^s3://"));
        when(repositoriesApi.getRepository("repo"))
            .thenReturn(new Repository().storageNamespace(s3Url("/repo-base")));

        when(repositoriesApi.getRepository("repo"))
            .thenReturn(new Repository().storageNamespace(s3Url("/repo-base")));

        fs.initializeWithClientFactory(new URI("lakefs://repo/main/file.txt"), conf,
                                       new LakeFSFileSystem.ClientFactory() {
                public LakeFSClient newClient() { return lfsClient; }
            });
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
    public void testGetFileStatus_ExistingFile() throws ApiException, IOException {
        Path p = new Path("lakefs://repo/main/exists");
        ObjectStats os = new ObjectStats();
        os.setPath("exists");
        os.checksum(UNUSED_CHECKSUM);
        os.setPathType(PathTypeEnum.OBJECT);
        os.setMtime(UNUSED_MTIME);
        os.setSizeBytes(UNUSED_FILE_SIZE);
        os.setPhysicalAddress(s3Url("/repo-base/exists"));
        when(objectsApi.statObject("repo", "main", "exists", false, false))
                .thenReturn(os);
        LakeFSFileStatus fileStatus = fs.getFileStatus(p);
        Assert.assertTrue(fileStatus.isFile());
        Assert.assertEquals(p, fileStatus.getPath());
    }

    @Test
    public void testGetFileStatus_NoFile() throws ApiException, IOException {
        Path noFilePath = new Path("lakefs://repo/main/no.file");

        when(objectsApi.statObject("repo", "main", "no.file", false, false))
                .thenThrow(noSuchFile);
        when(objectsApi.statObject("repo", "main", "no.file/", false, false))
                .thenThrow(noSuchFile);
        when(objectsApi.listObjects("repo", "main", false, false, "", 1, "", "no.file/"))
                .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));
        Assert.assertThrows(FileNotFoundException.class, () -> fs.getFileStatus(noFilePath));
    }

    @Test
    public void testGetFileStatus_DirectoryMarker() throws ApiException, IOException {
        Path dirPath = new Path("lakefs://repo/main/dir1/dir2");
        when(objectsApi.statObject("repo", "main", "dir1/dir2", false, false))
                .thenThrow(noSuchFile);
        ObjectStats dirObjectStats = new ObjectStats();
        dirObjectStats.setPath("dir1/dir2/");
        dirObjectStats.checksum(UNUSED_CHECKSUM);
        dirObjectStats.setPathType(PathTypeEnum.OBJECT);
        dirObjectStats.setMtime(UNUSED_MTIME);
        dirObjectStats.setSizeBytes(0L);
        dirObjectStats.setPhysicalAddress(s3Url("/repo-base/dir12"));
        when(objectsApi.statObject("repo", "main", "dir1/dir2/", false, false))
                .thenReturn(dirObjectStats);
        LakeFSFileStatus dirStatus = fs.getFileStatus(dirPath);
        Assert.assertTrue(dirStatus.isDirectory());
        Assert.assertEquals(dirPath, dirStatus.getPath());
    }

    @Test
    public void testExists_ExistsAsObject() throws ApiException, IOException {
        Path p = new Path("lakefs://repo/main/exis.ts");
        ObjectStats stats = new ObjectStats().path("exis.ts").pathType(PathTypeEnum.OBJECT);
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""), any(), eq(""), eq("exis.ts")))
            .thenReturn(new ObjectStatsList().results(ImmutableList.of(stats)));
        Assert.assertTrue(fs.exists(p));
    }

    @Test
    public void testExists_ExistsAsDirectoryMarker() throws ApiException, IOException {
        Path p = new Path("lakefs://repo/main/exis.ts");
        ObjectStats stats = new ObjectStats().path("exis.ts/").pathType(PathTypeEnum.OBJECT);
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false),  eq(""), any(), eq(""), eq("exis.ts")))
            .thenReturn(new ObjectStatsList().results(ImmutableList.of(stats)));
        Assert.assertTrue(fs.exists(p));
    }

    @Test
    public void testExists_ExistsAsDirectoryContents() throws ApiException, IOException {
        Path p = new Path("lakefs://repo/main/exis.ts");
        ObjectStats stats = new ObjectStats().path("exis.ts/inside").pathType(PathTypeEnum.OBJECT);
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false),  eq(""), any(), eq(""), eq("exis.ts")))
            .thenReturn(new ObjectStatsList().results(ImmutableList.of(stats)));
        Assert.assertTrue(fs.exists(p));
    }

    @Test
    public void testExists_ExistsAsDirectoryInSecondList() throws ApiException, IOException {
        // TODO(ariels)
    }

    @Test
    public void testExists_NotExistsNoPrefix() throws ApiException, IOException {
        Path p = new Path("lakefs://repo/main/doesNotExi.st");
        when(objectsApi.listObjects(any(), any(), any(), any(), any(), any(), any(), any()))
            .thenReturn(new ObjectStatsList());
        boolean exists = fs.exists(p);
        Assert.assertFalse(exists);
    }

    @Test
    public void testExists_NotExistsPrefixWithNoSlash() throws ApiException, IOException {
        // TODO(ariels)
    }

    @Test
    public void testExists_NotExistsPrefixWithNoSlashTwoLists() throws ApiException, IOException {
        // TODO(ariels)
    }

    @Test
    public void testDelete_FileExists() throws ApiException, IOException {
        when(objectsApi.statObject("repo", "main", "no/place/file.txt", false, false))
                .thenReturn(new ObjectStats().
                        path("delete/sample/file.txt").
                        pathType(PathTypeEnum.OBJECT).
                        physicalAddress(s3Url("/repo-base/delete")).
                        checksum(UNUSED_CHECKSUM).
                        mtime(UNUSED_MTIME).
                        sizeBytes(UNUSED_FILE_SIZE));
        String[] arrDirs = {"no/place", "no"};
        for (String dir: arrDirs) {
            when(objectsApi.statObject("repo", "main", dir, false, false))
                    .thenThrow(noSuchFile);
            when(objectsApi.statObject("repo", "main", dir + Constants.SEPARATOR, false, false))
                    .thenThrow(noSuchFile);
            when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""), any(), eq(""), eq(dir + Constants.SEPARATOR)))
                    .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));
        }
        StagingLocation stagingLocation = new StagingLocation().token("foo").physicalAddress(s3Url("/repo-base/dir-marker"));
        when(stagingApi.getPhysicalAddress("repo", "main", "no/place/", false))
                .thenReturn(stagingLocation);

        Path path = new Path("lakefs://repo/main/no/place/file.txt");

        mockDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));

        // return true because file found
        boolean success = fs.delete(path, false);
        Assert.assertTrue(success);
    }

    @Test
    public void testDelete_FileNotExists() throws ApiException, IOException {
        doThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "not found"))
                .when(objectsApi).deleteObject("repo", "main", "no/place/file.txt");
        when(objectsApi.statObject("repo", "main", "no/place/file.txt", false, false))
                .thenThrow(noSuchFile);
        when(objectsApi.statObject("repo", "main", "no/place/file.txt/", false, false))
                .thenThrow(noSuchFile);
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""), any(), eq(""), eq("no/place/file.txt/")))
                .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));

        // return false because file not found
        boolean success = fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), false);
        Assert.assertFalse(success);
    }

    @Test
    public void testDelete_EmptyDirectoryExists() throws ApiException, IOException {
        ObjectLocation dirObjLoc = new ObjectLocation("lakefs", "repo", "main", "delete/me");
        String key = objectLocToS3ObjKey(dirObjLoc);

        when(objectsApi.statObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath(), false, false))
                .thenThrow(noSuchFile);

        ObjectStats srcStats = new ObjectStats()
                .path(dirObjLoc.getPath() + Constants.SEPARATOR)
                .sizeBytes(0L)
                .mtime(UNUSED_MTIME)
                .pathType(PathTypeEnum.OBJECT)
                .physicalAddress(s3Url(key+Constants.SEPARATOR))
                .checksum(UNUSED_CHECKSUM);
        when(objectsApi.statObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath() + Constants.SEPARATOR, false, false))
                .thenReturn(srcStats)
                .thenThrow(noSuchFile);

        Path path = new Path("lakefs://repo/main/delete/me");

        mockDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));

        boolean success = fs.delete(path, false);
        Assert.assertTrue(success);
    }

    @Test(expected = IOException.class)
    public void testDelete_DirectoryWithFile() throws ApiException, IOException {
        when(objectsApi.statObject("repo", "main", "delete/sample", false, false))
                .thenThrow(noSuchFile);
        when(objectsApi.statObject("repo", "main", "delete/sample/", false, false))
                .thenThrow(noSuchFile);
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""), any(), eq(""), eq("delete/sample/")))
                .thenReturn(new ObjectStatsList().results(Collections.singletonList(new ObjectStats().
                        path("delete/sample/file.txt").
                        pathType(PathTypeEnum.OBJECT).
                        physicalAddress(s3Url("/repo-base/delete")).
                        checksum(UNUSED_CHECKSUM).
                        mtime(UNUSED_MTIME).
                        sizeBytes(UNUSED_FILE_SIZE))).pagination(new Pagination().hasMore(false)));
        doThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "not found"))
                .when(objectsApi).deleteObject("repo", "main", "delete/sample/");
        // return false because we can't delete a directory without recursive
        fs.delete(new Path("lakefs://repo/main/delete/sample"), false);
    }

    @Test
    public void testDelete_NotExistsRecursive() throws ApiException, IOException {
        when(objectsApi.statObject(eq("repo"), eq("main"), any(), eq(false), eq(false)))
                .thenThrow(noSuchFile);
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false),eq(false), eq(""), any(), eq(""), eq("no/place/file.txt/")))
                .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));
        boolean delete = fs.delete(new Path("lakefs://repo/main/no/place/file.txt"), true);
        Assert.assertFalse(delete);
    }

    private PathList newPathList(String... paths) {
        return new PathList().paths(Arrays.asList(paths));
    }

    @Test
    public void testDelete_DirectoryWithFileRecursive() throws ApiException, IOException {
        when(objectsApi.statObject("repo", "main", "delete/sample", false, false))
                .thenThrow(noSuchFile);
        when(objectsApi.statObject("repo", "main", "delete/sample/", false, false))
                .thenThrow(noSuchFile);
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""), any(), eq(""), eq("delete/sample/")))
                .thenReturn(new ObjectStatsList().results(Collections
                        .singletonList(new ObjectStats().
                                path("delete/sample/file.txt").
                                pathType(PathTypeEnum.OBJECT).
                                physicalAddress(s3Url("/repo-base/delete")).
                                checksum(UNUSED_CHECKSUM).
                                mtime(UNUSED_MTIME).
                                sizeBytes(UNUSED_FILE_SIZE)))
                        .pagination(new Pagination().hasMore(false)));
        when(objectsApi.deleteObjects("repo", "main", newPathList("delete/sample/file.txt")))
            .thenReturn(new ObjectErrorList());
        // recursive will always end successfully
        Path path = new Path("lakefs://repo/main/delete/sample");

        mockDirectoryMarker(ObjectLocation.pathToObjectLocation(null, path.getParent()));

        boolean delete = fs.delete(path, true);
        Assert.assertTrue(delete);
    }

    protected void caseDeleteDirectoryRecursive(int bulkSize, int numObjects) throws ApiException, IOException {
        conf.setInt(LakeFSFileSystem.LAKEFS_DELETE_BULK_SIZE, bulkSize);
        when(objectsApi.statObject("repo", "main", "delete/sample", false, false))
            .thenThrow(noSuchFile);
        when(objectsApi.statObject("repo", "main", "delete/sample/", false, false))
                .thenThrow(noSuchFile);

        List<ObjectStats> objects = new ArrayList();
        for (int i = 0; i < numObjects; i++) {
            objects.add(new ObjectStats().
                        path(String.format("delete/sample/file%04d.txt", i)).
                        pathType(PathTypeEnum.OBJECT).
                        physicalAddress(s3Url(String.format("/repo-base/delete%04d", i))).
                        checksum(UNUSED_CHECKSUM).
                        mtime(UNUSED_MTIME).
                        sizeBytes(UNUSED_FILE_SIZE));
        }
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""), any(), eq(""), eq("delete/sample/")))
            .thenReturn(new ObjectStatsList()
                        .results(objects)
                        .pagination(new Pagination().hasMore(false)));

        // Set up multiple deleteObjects expectations of bulkSize deletes
        // each (except for the last, which might be smaller).
        for (int start = 0; start < numObjects; start += bulkSize) {
            PathList pl = new PathList();
            for (int i = start; i < numObjects && i < start + bulkSize; i++) {
                pl.addPathsItem(String.format("delete/sample/file%04d.txt", i));
            }
            when(objectsApi.deleteObjects(eq("repo"), eq("main"), eq(pl)))
                .thenReturn(new ObjectErrorList());
        }
        // Mock parent directory marker creation at end of fs.delete to show
        // the directory marker exists.
        ObjectLocation dir = new ObjectLocation("lakefs", "repo", "main", "delete");
        mockDirectoryMarker(dir);
        // recursive will always end successfully
        boolean delete = fs.delete(new Path("lakefs://repo/main/delete/sample"), true);
        Assert.assertTrue(delete);
    }

    @Test
    public void testDeleteDirectoryRecursiveBatch1() throws ApiException, IOException {
        caseDeleteDirectoryRecursive(1, 123);
    }

    @Test
    public void testDeleteDirectoryRecursiveBatch2() throws ApiException, IOException {
        caseDeleteDirectoryRecursive(2, 123);
    }

    @Test
    public void testDeleteDirectoryRecursiveBatch3() throws ApiException, IOException {
        caseDeleteDirectoryRecursive(3, 123);
    }
    @Test
    public void testDeleteDirectoryRecursiveBatch5() throws ApiException, IOException {
        caseDeleteDirectoryRecursive(5, 123);
    }
    @Test
    public void testDeleteDirectoryRecursiveBatch120() throws ApiException, IOException {
        caseDeleteDirectoryRecursive(120, 123);
    }
    @Test
    public void testDeleteDirectoryRecursiveBatch123() throws ApiException, IOException {
        caseDeleteDirectoryRecursive(123, 123);
    }

    @Test
    public void testCreate() throws ApiException, IOException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        Path p = new Path("lakefs://repo/main/sub1/sub2/create.me");

        mockNonExistingPath(new ObjectLocation("lakefs", "repo", "main", "sub1/sub2/create.me"));

        StagingLocation stagingLocation = new StagingLocation().token("foo").physicalAddress(s3Url("/repo-base/create"));
        when(stagingApi.getPhysicalAddress("repo", "main", "sub1/sub2/create.me", false))
                .thenReturn(stagingLocation);

        // mock sub1/sub2 was an empty directory
        ObjectLocation sub2Loc = new ObjectLocation("lakefs", "repo", "main", "sub1/sub2");
        mockEmptyDirectoryMarker(sub2Loc);

        OutputStream out = fs.create(p);
        out.write(contents.getBytes());
        out.close();

        ArgumentCaptor<StagingMetadata> metadataCapture = ArgumentCaptor.forClass(StagingMetadata.class);
        verify(stagingApi).linkPhysicalAddress(eq("repo"), eq("main"), eq("sub1/sub2/create.me"),
                                               metadataCapture.capture());
        StagingMetadata actualMetadata = metadataCapture.getValue();
        Assert.assertEquals(stagingLocation, actualMetadata.getStaging());
        Assert.assertEquals(contents.getBytes().length, actualMetadata.getSizeBytes().longValue());

        // Write succeeded, verify physical file on S3.
        S3Object ret = s3Client.getObject(new GetObjectRequest(s3Bucket, "/repo-base/create"));
        InputStream in = ret.getObjectContent();
        String actual = IOUtils.toString(in);

        Assert.assertEquals(contents, actual);

        List<String> actualFiles = getS3FilesByPrefix("/");
        Assert.assertEquals(ImmutableList.of("repo-base/create"), actualFiles);

        // expected to delete the empty dir marker
        verifyObjDeletion(new ObjectLocation("lakefs", "repo", "main", "sub1/sub2/"));
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void testCreateExistingDirectory() throws ApiException, IOException {
        ObjectLocation dir = new ObjectLocation("lakefs", "repo", "main", "sub1/sub2/create.me");
        mockExistingDirPath(dir, Collections.emptyList());
        fs.create(new Path("lakefs://repo/main/sub1/sub2/create.me"), false);
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void testCreateExistingFile() throws ApiException, IOException {
        ObjectLocation dir = new ObjectLocation("lakefs", "repo", "main", "sub1/sub2");
        mockExistingDirPath(dir, ImmutableList.of(new ObjectLocation("lakefs", "repo", "main", "sub1/sub2/create.me")));
        fs.create(new Path("lakefs://repo/main/sub1/sub2/create.me"), false);
    }

    @Test
    public void testMkdirs() throws ApiException, IOException {
        // setup empty folder checks
        Path testPath = new Path("dir1/dir2/dir3");
        do {
            when(objectsApi.statObject("repo", "main", testPath.toString(), false, false))
                    .thenThrow(noSuchFile);
            when(objectsApi.statObject("repo", "main", testPath.toString()+"/", false, false))
                    .thenThrow(noSuchFile);
            when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""), any(), eq(""), eq(testPath.toString()+"/")))
                    .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));
            testPath = testPath.getParent();
        } while(testPath != null && !testPath.isRoot());

        // physical address to directory marker object
        StagingLocation stagingLocation = new StagingLocation().token("foo").physicalAddress(s3Url("/repo-base/emptyDir"));
        when(stagingApi.getPhysicalAddress("repo", "main", "dir1/dir2/dir3/", false))
                .thenReturn(stagingLocation);

        // call mkdirs
        Path p = new Path("lakefs://repo/main/dir1/dir2/dir3");
        boolean mkdirs = fs.mkdirs(p);
        Assert.assertTrue("make dirs", mkdirs);

        // verify metadata
        ArgumentCaptor<StagingMetadata> metadataCapture = ArgumentCaptor.forClass(StagingMetadata.class);
        verify(stagingApi).linkPhysicalAddress(eq("repo"), eq("main"), eq("dir1/dir2/dir3/"),
                metadataCapture.capture());
        StagingMetadata actualMetadata = metadataCapture.getValue();
        Assert.assertEquals(stagingLocation, actualMetadata.getStaging());
        Assert.assertEquals(0, (long)actualMetadata.getSizeBytes());

        // verify file exists on s3
        S3Object ret = s3Client.getObject(new GetObjectRequest(s3Bucket, "/repo-base/emptyDir"));
        String actual = IOUtils.toString(ret.getObjectContent());
        Assert.assertEquals("", actual);
    }

    @Test
    public void testOpen() throws IOException, ApiException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        byte[] contentsBytes = contents.getBytes();

        String key = "/repo-base/open";

        // Write physical file to S3.
        ObjectMetadata s3Metadata = new ObjectMetadata();
        s3Metadata.setContentLength(contentsBytes.length);
        s3Client.putObject(new PutObjectRequest(s3Bucket, key, new ByteArrayInputStream(contentsBytes), s3Metadata));

        Path p = new Path("lakefs://repo/main/read.me");
        when(objectsApi.statObject("repo", "main", "read.me", false, false)).
            thenReturn(new ObjectStats().
                       path(p.toString()).
                       pathType(PathTypeEnum.OBJECT).
                       physicalAddress(s3Url(key)).
                       checksum(UNUSED_CHECKSUM).
                       mtime(UNUSED_MTIME).
                       sizeBytes((long)contentsBytes.length));

        try (InputStream in = fs.open(p)) {
            String actual = IOUtils.toString(in);
            Assert.assertEquals(contents, actual);
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void testOpen_NotExists() throws IOException, ApiException {
        Path p = new Path("lakefs://repo/main/doesNotExi.st");
        when(objectsApi.statObject(any(), any(), any(), any(), any()))
            .thenThrow(noSuchFile);
        fs.open(p);
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
        when(objectsApi.statObject("repo", "main", "status/file", false, false))
                .thenReturn(objectStats);
        Path p = new Path("lakefs://repo/main/status/file");
        FileStatus[] fileStatuses = fs.listStatus(p);
        LakeFSFileStatus expectedFileStatus = new LakeFSFileStatus.Builder(p)
                .length(STATUS_FILE_SIZE)
                .checksum(STATUS_CHECKSUM)
                .mTime(STATUS_MTIME)
                .physicalAddress(p.toString())
                .blockSize(Constants.DEFAULT_BLOCK_SIZE)
                .build();
        LakeFSFileStatus[] expectedFileStatuses = new LakeFSFileStatus[]{expectedFileStatus};
        Assert.assertArrayEquals(expectedFileStatuses, fileStatuses);
    }

    @Test(expected = FileNotFoundException.class)
    public void testListStatusNotFound() throws ApiException, IOException {
        when(objectsApi.statObject("repo", "main", "status/file", false, false))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
        when(objectsApi.statObject("repo", "main", "status/file/", false, false))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""),
                any(), eq("/"), eq("status/file/")))
                .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));
        Path p = new Path("lakefs://repo/main/status/file");
        fs.listStatus(p);
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
        when(objectsApi.listObjects(eq("repo"), eq("main"), eq(false), eq(false), eq(""),
                any(), eq("/"), eq("status/")))
                .thenReturn(objects.pagination(new Pagination().hasMore(false)));
        when(objectsApi.statObject("repo", "main", "status", false, false))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

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

    private void mockDirectoryMarker(ObjectLocation objectLoc) throws ApiException {
        // Mock parent directory to show the directory marker exists.
        ObjectStats markerStats = new ObjectStats().path(objectLoc.getPath()).pathType(PathTypeEnum.OBJECT);
        when(objectsApi.listObjects(eq(objectLoc.getRepository()), eq(objectLoc.getRef()), eq(false), eq(false), eq(""), any(), eq(""), eq(objectLoc.getPath()))).
            thenReturn(new ObjectStatsList().results(ImmutableList.of(markerStats)));
    }

    private void mockNonExistingPath(ObjectLocation objectLoc) throws ApiException {
        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), false, false))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath() + Constants.SEPARATOR, false, false))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

        when(objectsApi.listObjects(eq(objectLoc.getRepository()), eq(objectLoc.getRef()), eq(false), eq(false),
                eq(""), any(), eq(""), eq(objectLoc.getPath() + Constants.SEPARATOR)))
                .thenReturn(new ObjectStatsList().pagination(new Pagination().hasMore(false)));
    }

    private void mockExistingDirPath(ObjectLocation dirObjLoc, List<ObjectLocation> filesInDir) throws ApiException {
        // io.lakefs.LakeFSFileSystem.getFileStatus tries to get object stats, when it can't find an object if will
        // fall back to try listing items under this path to discover the objects it contains. if objects are found,
        // then the path considered a directory.

        ObjectStatsList stats = new ObjectStatsList();

        // Mock directory marker
        if (filesInDir.isEmpty()) {
            ObjectStats markerStat = mockEmptyDirectoryMarker(dirObjLoc);
            stats.addResultsItem(markerStat);
        } else {
            when(objectsApi.statObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath(), false, false))
                    .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
            when(objectsApi.statObject(dirObjLoc.getRepository(), dirObjLoc.getRef(), dirObjLoc.getPath() + Constants.SEPARATOR, false, false))
                    .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
        }

        // Mock the files under this directory
        for (ObjectLocation loc : filesInDir) {
            ObjectStats fileStat = mockExistingFilePath(loc);
            stats.addResultsItem(fileStat);
        }


        // Mock listing the files under this directory
        stats.setPagination(new Pagination().hasMore(false));
        when(objectsApi.listObjects(eq(dirObjLoc.getRepository()), eq(dirObjLoc.getRef()), eq(false), eq(false),
                eq(""), any(), eq(""), eq(dirObjLoc.getPath() + Constants.SEPARATOR)))
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
        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), false, false)).thenReturn(srcStats);
        return srcStats;
    }

    private void mockMissingCopyAPI() throws ApiException {
        when(objectsApi.copyObject(any(), any(), any(), any())).thenThrow(new ApiException(HttpStatus.SC_INTERNAL_SERVER_ERROR, null, "{\"message\":\"invalid API endpoint\"}"));
        when(objectsApi.stageObject(any(), any(), any(), any())).thenReturn(new ObjectStats());
    }

    private ObjectStats mockEmptyDirectoryMarker(ObjectLocation objectLoc) throws ApiException {
        String key = objectLocToS3ObjKey(objectLoc);

        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), false, false))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

        ObjectStats srcStats = new ObjectStats()
                .path(objectLoc.getPath() + Constants.SEPARATOR)
                .sizeBytes(0L)
                .mtime(UNUSED_MTIME)
                .pathType(PathTypeEnum.OBJECT)
                .physicalAddress(s3Url(key+Constants.SEPARATOR))
                .checksum(UNUSED_CHECKSUM);
        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath() + Constants.SEPARATOR, false, false))
                .thenReturn(srcStats);

        ObjectLocation parentLoc = objectLoc.getParent();
        while (parentLoc != null && parentLoc.isValidPath()) {
            when(objectsApi.statObject(parentLoc.getRepository(), parentLoc.getRef(), parentLoc.getPath(), false, false))
                    .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
            when(objectsApi.statObject(parentLoc.getRepository(), parentLoc.getRef(), parentLoc.getPath()+ Constants.SEPARATOR, false, false))
                    .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
            when(objectsApi.listObjects(parentLoc.getRepository(), parentLoc.getRef(), false, false, "", 1, "", parentLoc.getPath() + Constants.SEPARATOR))
                    .thenReturn(new ObjectStatsList().results(Collections.emptyList()).pagination(new Pagination().hasMore(false)));
            parentLoc = parentLoc.getParent();
        }
        return srcStats;
    }

    private String objectLocToS3ObjKey(ObjectLocation objectLoc) {
        return String.format("/%s/%s/%s",objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
    }

    private void verifyObjDeletion(ObjectLocation srcObjLoc) throws ApiException {
        verify(objectsApi).deleteObject(srcObjLoc.getRepository(), srcObjLoc.getRef(), srcObjLoc.getPath());
    }

    private boolean dstPathLinkedToSrcPhysicalAddress(ObjectLocation srcObjLoc, ObjectLocation dstObjLoc) throws ApiException {
        ArgumentCaptor<ObjectCopyCreation> creationReqCapture = ArgumentCaptor.forClass(ObjectCopyCreation.class);
        verify(objectsApi).copyObject(eq(dstObjLoc.getRepository()), eq(dstObjLoc.getRef()), eq(dstObjLoc.getPath()),
                creationReqCapture.capture());
        ObjectCopyCreation actualCreationReq = creationReqCapture.getValue();
        // Rename is a metadata operation, therefore the dst name is expected to link to the src physical address.
        return srcObjLoc.getRef().equals(actualCreationReq.getSrcRef()) &&
                srcObjLoc.getPath().equals(actualCreationReq.getSrcPath());
    }

    /**
     * rename(src.txt, non-existing-dst) -> non-existing/new - unsupported, should fail with false
     */
    @Test
    public void testRename_existingFileToNonExistingDst() throws IOException, ApiException {
        Path src = new Path("lakefs://repo/main/existing.src");
        ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);
        mockExistingFilePath(srcObjLoc);

        Path dst = new Path("lakefs://repo/main/non-existing/new");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);
        mockNonExistingPath(dstObjLoc);
        mockNonExistingPath(fs.pathToObjectLocation(dst.getParent()));

        mockDirectoryMarker(fs.pathToObjectLocation(src.getParent()));

        boolean renamed = fs.rename(src, dst);
        Assert.assertFalse(renamed);
    }

    @Test
    public void testRename_existingFileToExistingFileName() throws ApiException, IOException {
        Path src = new Path("lakefs://repo/main/existing.src");
        ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);
        mockExistingFilePath(srcObjLoc);

        Path dst = new Path("lakefs://repo/main/existing.dst");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);
        mockExistingFilePath(dstObjLoc);

        mockDirectoryMarker(fs.pathToObjectLocation(src.getParent()));

        boolean success = fs.rename(src, dst);
        Assert.assertTrue(success);
    }

    @Test
    public void testRename_existingDirToExistingFileName() throws ApiException, IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir/existing.src");
        ObjectLocation fileObjLoc = fs.pathToObjectLocation(fileInSrcDir);
        Path srcDir = new Path("lakefs://repo/main/existing-dir");
        ObjectLocation srcDirObjLoc = fs.pathToObjectLocation(srcDir);
        mockExistingDirPath(srcDirObjLoc, ImmutableList.of(fileObjLoc));

        Path dst = new Path("lakefs://repo/main/existingdst.file");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);
        mockExistingFilePath(dstObjLoc);

        boolean success = fs.rename(srcDir, dst);
        Assert.assertFalse(success);
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

        mockDirectoryMarker(fs.pathToObjectLocation(src.getParent()));

        boolean renamed = fs.rename(src, dst);
        Assert.assertTrue(renamed);
        Path expectedDstPath = new Path("lakefs://repo/main/existing-dir2/existing.src");
        Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(srcObjLoc, fs.pathToObjectLocation(expectedDstPath)));
        verifyObjDeletion(srcObjLoc);
    }

    /**
     * rename(srcDir(containing srcDir/a.txt, srcDir/b.txt), non-existing-dir/new) -> unsupported, rename should fail by returning false
     */
    @Test
    public void testRename_existingDirToNonExistingDirWithoutParent() throws ApiException, IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir/existing.src");
        ObjectLocation fileObjLoc = fs.pathToObjectLocation(fileInSrcDir);
        Path srcDir = new Path("lakefs://repo/main/existing-dir");
        ObjectLocation srcDirObjLoc = fs.pathToObjectLocation(srcDir);
        mockExistingDirPath(srcDirObjLoc, ImmutableList.of(fileObjLoc));
        mockNonExistingPath(new ObjectLocation("lakefs", "repo", "main", "non-existing-dir"));
        mockNonExistingPath(new ObjectLocation("lakefs", "repo", "main", "non-existing-dir/new"));

        Path dst = new Path("lakefs://repo/main/non-existing-dir/new");
        boolean renamed = fs.rename(srcDir, dst);
        Assert.assertFalse(renamed);
    }

    /**
     * rename(srcDir(containing srcDir/a.txt, srcDir/b.txt), non-existing-dir/new) -> unsupported, rename should fail by returning false
     */
    @Test
    public void testRename_existingDirToNonExistingDirWithParent() throws ApiException, IOException {
        Path fileInSrcDir = new Path("lakefs://repo/main/existing-dir/existing.src");
        ObjectLocation fileObjLoc = fs.pathToObjectLocation(fileInSrcDir);
        Path srcDir = new Path("lakefs://repo/main/existing-dir");
        ObjectLocation srcDirObjLoc = fs.pathToObjectLocation(srcDir);
        mockExistingDirPath(srcDirObjLoc, ImmutableList.of(fileObjLoc));
        mockExistingDirPath(new ObjectLocation("lakefs", "repo", "main", "existing-dir2/new"), Collections.emptyList());

        Path dst = new Path("lakefs://repo/main/existing-dir2/new");
        mockDirectoryMarker(fs.pathToObjectLocation(srcDir));

        boolean renamed = fs.rename(srcDir, dst);
        Assert.assertTrue(renamed);
    }

    /**
     * rename(srcDir(containing srcDir/a.txt), existing-nonempty-dstdir) -> unsupported, rename should fail by returning false.
     */
    @Test
    public void testRename_existingDirToExistingNonEmptyDirName() throws ApiException, IOException {
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
        Assert.assertFalse(renamed);
    }

    /**
     * Check that a file is renamed when working against a lakeFS version
     * where CopyObject API doesn't exist
     */
    @Test
    public void testRename_fallbackStageAPI() throws ApiException, IOException {
        Path src = new Path("lakefs://repo/main/existing-dir1/existing.src");
        ObjectLocation srcObjLoc = fs.pathToObjectLocation(src);
        mockExistingFilePath(srcObjLoc);

        Path fileInDstDir = new Path("lakefs://repo/main/existing-dir2/existing.src");
        ObjectLocation fileObjLoc = fs.pathToObjectLocation(fileInDstDir);
        Path dst = new Path("lakefs://repo/main/existing-dir2");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(dst);

        mockExistingDirPath(dstObjLoc, ImmutableList.of(fileObjLoc));
        mockDirectoryMarker(fs.pathToObjectLocation(src.getParent()));
        mockMissingCopyAPI();

        boolean renamed = fs.rename(src, dst);
        Assert.assertTrue(renamed);
        Path expectedDstPath = new Path("lakefs://repo/main/existing-dir2/existing.src");
        Assert.assertTrue(dstPathLinkedToSrcPhysicalAddress(srcObjLoc, fs.pathToObjectLocation(expectedDstPath)));
        verifyObjDeletion(srcObjLoc);
    }

    @Test
    public void testRename_srcAndDstOnDifferentBranch() throws IOException, ApiException {
        Path src = new Path("lakefs://repo/branch/existing.src");
        Path dst = new Path("lakefs://repo/another-branch/existing.dst");
        boolean renamed = fs.rename(src, dst);
        Assert.assertFalse(renamed);
        Mockito.verify(objectsApi, never()).statObject(any(), any(), any(), any(), any());
        Mockito.verify(objectsApi, never()).copyObject(any(), any(), any(), any());
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
        Mockito.verify(objectsApi, never()).statObject(any(), any(), any(), any(), any());
        Mockito.verify(objectsApi, never()).copyObject(any(), any(), any(), any());
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

        boolean success = fs.rename(src, dst);
        Assert.assertFalse(success);
    }

    /**
     * globStatus is used only by the Hadoop CLI where the pattern is always the exact file.
     */
    @Test
    public void testGlobStatus_SingleFile() throws ApiException, IOException {
        Path path = new Path("lakefs://repo/main/existing.dst");
        ObjectLocation dstObjLoc = fs.pathToObjectLocation(path);
        mockExistingFilePath(dstObjLoc);

        FileStatus[] statuses = fs.globStatus(path);
        Assert.assertArrayEquals(new FileStatus[]{
                new LakeFSFileStatus.Builder(path).build()
        }, statuses);
    }
}
