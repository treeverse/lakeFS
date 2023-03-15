package io.lakefs;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpStatus;
import org.junit.Rule;
import org.mockito.Answers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.google.common.collect.ImmutableList;
import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.BranchesApi;
import io.lakefs.clients.api.ConfigApi;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.RepositoriesApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStats.PathTypeEnum;
import io.lakefs.clients.api.model.ObjectStatsList;
import io.lakefs.clients.api.model.Pagination;
import io.lakefs.clients.api.model.Repository;
import io.lakefs.clients.api.model.StorageConfig;
import io.lakefs.utils.ObjectLocation;

abstract class LakeFSFileSystemBaseTest {
    protected static final String S3_ACCESS_KEY_ID = "AKIArootkey";
    protected static final String S3_SECRET_ACCESS_KEY = "secret/minio/key=";
    static final String UNUSED_CHECKSUM = "unused";
    static final Long UNUSED_FILE_SIZE = 1L;
    static final Long UNUSED_MTIME = 0L;

    static final Long STATUS_FILE_SIZE = 2L;
    static final Long STATUS_MTIME = 0L;
    static final String STATUS_CHECKSUM = "status";
    static final ApiException noSuchFile = new ApiException(HttpStatus.SC_NOT_FOUND, "no such file");


    protected final LakeFSFileSystem fs = new LakeFSFileSystem();

    protected String s3Base;
    protected String s3Bucket;
    protected AmazonS3 s3Client;
    protected Configuration conf = new Configuration(false);
    protected LakeFSClient lfsClient;

    protected ObjectsApi objectsApi;
    protected BranchesApi branchesApi;
    protected RepositoriesApi repositoriesApi;
    protected StagingApi stagingApi;
    protected ConfigApi configApi;


    private static final DockerImageName MINIO = DockerImageName.parse("minio/minio:RELEASE.2021-06-07T21-40-51Z");
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

    String objectLocToS3ObjKey(ObjectLocation objectLoc) {
        return String.format("/%s/%s/%s",objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath());
    }

    void verifyObjDeletion(ObjectLocation srcObjLoc) throws ApiException {
        verify(objectsApi).deleteObject(srcObjLoc.getRepository(), srcObjLoc.getRef(), srcObjLoc.getPath());
    }

    void mockDirectoryMarker(ObjectLocation objectLoc) throws ApiException {
        // Mock parent directory to show the directory marker exists.
        ObjectStats markerStats = new ObjectStats().path(objectLoc.getPath()).pathType(PathTypeEnum.OBJECT);
        when(objectsApi.listObjects(eq(objectLoc.getRepository()), eq(objectLoc.getRef()), eq(false), eq(false), eq(""), any(), eq(""), eq(objectLoc.getPath()))).
            thenReturn(new ObjectStatsList().results(ImmutableList.of(markerStats)));
    }

    void mockNonExistingPath(ObjectLocation objectLoc) throws ApiException {
        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(), false, false))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

        when(objectsApi.statObject(objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath() + Constants.SEPARATOR, false, false))
                .thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));

        when(objectsApi.listObjects(eq(objectLoc.getRepository()), eq(objectLoc.getRef()), eq(false), eq(false),
                eq(""), any(), eq(""), eq(objectLoc.getPath() + Constants.SEPARATOR)))
                .thenReturn(new ObjectStatsList().pagination(new Pagination().hasMore(false)));
    }

    void mockExistingDirPath(ObjectLocation dirObjLoc, List<ObjectLocation> filesInDir) throws ApiException {
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

    ObjectStats mockExistingFilePath(ObjectLocation objectLoc) throws ApiException {
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

    void mockMissingCopyAPI() throws ApiException {
        when(objectsApi.copyObject(any(), any(), any(), any())).thenThrow(new ApiException(HttpStatus.SC_INTERNAL_SERVER_ERROR, null, "{\"message\":\"invalid API endpoint\"}"));
        when(objectsApi.stageObject(any(), any(), any(), any())).thenReturn(new ObjectStats());
    }

    ObjectStats mockEmptyDirectoryMarker(ObjectLocation objectLoc) throws ApiException {
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


    public void init() throws ApiException, IOException, URISyntaxException {
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
        logS3Container();
    }
    
    public void logS3Container() {
        Logger s3Logger = LoggerFactory.getLogger("s3 container");
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(s3Logger)
            .withMdc("container", "s3")
            .withSeparateOutputStreams();
        s3.followOutput(logConsumer);
    }
}
