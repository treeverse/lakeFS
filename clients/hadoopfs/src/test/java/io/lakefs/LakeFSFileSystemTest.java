package io.lakefs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockedConstruction.MockInitializer;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.MockedConstruction.Context;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.RepositoriesApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.model.ObjectStats;
import io.lakefs.clients.api.model.ObjectStatsList;
import io.lakefs.clients.api.model.Repository;
import io.lakefs.clients.api.model.StagingLocation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.aventrix.jnanoid.jnanoid.NanoIdUtils;

public class LakeFSFileSystemTest {
    protected static final Logger LOG = LoggerFactory.getLogger(LakeFSFileSystemTest.class);
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

    protected static final Regions REGION = Regions.US_EAST_1;

    @Rule
    public final GenericContainer s3 = new GenericContainer(MINIO.toString()).
        withCommand("minio", "server", "/data").
        withEnv("MINIO_ROOT_USER", S3_ACCESS_KEY_ID).
        withEnv("MINIO_ROOT_PASSWORD", S3_SECRET_ACCESS_KEY).
        //        withEnv("MINIO_REGION_NAME", REGION.getName()).
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
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(s3Logger).
            withMdc("container", "s3").
            withSeparateOutputStreams();
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

        // S3ClientOptions opts = new S3ClientOptions();
        // opts.setPathStyleAccess(true);
        // s3Client.setS3ClientOptions(opts);

        s3Bucket = makeS3BucketName();
        s3Base = String.format("s3://%s", s3Bucket);
        LOG.debug(String.format("S3 endpoint \"%s\" bucket \"%s\" base URL \"%s\" region \"%s\"", s3Endpoint, s3Bucket, s3Base, REGION.getName()));
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

        when(repositoriesApi.getRepository("repo")).
            thenReturn(new Repository().storageNamespace(s3Url("/repo-base")));

        fs.initialize(new URI("lakefs://repo/main/file.txt"), conf, lfsClient);
    }

    @Test
    public void getUri() throws URISyntaxException, IOException {
        URI u = fs.getUri();
        Assert.assertNotNull(u);
    }

    @Test
    public void testExists_Exists() throws ApiException, IOException {
        Path p = new Path("lakefs://repo/main/exis.ts");
        when(objectsApi.statObject("repo", "main", "exis.ts")).
            thenReturn(new ObjectStats());

        Assert.assertTrue(fs.exists(p));
    }
    @Test
    public void testExists_NotExists() throws ApiException, IOException {
        Path p = new Path("lakefs://repo/main/doesNotExi.st");
        when(objectsApi.statObject(any(), any(), any())).
            thenThrow(new ApiException(HttpStatus.SC_NOT_FOUND, "no such file"));
        when(objectsApi.listObjects(any(), any(), any(), any(), any(), any())).
            thenReturn(new ObjectStatsList().results(Collections.emptyList()));

        Assert.assertFalse(fs.exists(p));
    }

    @Test
    public void testCreate() throws ApiException, IOException {
        String contents = "The quick brown fox jumps over the lazy dog.";
        Path p = new Path("lakefs://repo/main/create.me");

        when(stagingApi.getPhysicalAddress("repo", "main", "create.me")).
            thenReturn(new StagingLocation().token("foo").physicalAddress(s3Url("/repo-base/create")));
        // TODO(ariels): Verify call to lakeFS "link" -- or verify that lakeFSFS "open" works.

        OutputStream out = fs.create(p);
        out.write(contents.getBytes());
        out.close();

        // Write succeeded, verify physical file on S3.
        S3Object ret = s3Client.getObject(new GetObjectRequest(s3Bucket, "/repo-base/create"));
        InputStream in = ret.getObjectContent();
        ByteArrayOutputStream actualBytes = new ByteArrayOutputStream();
        {
            int len;
            byte[] buf = new byte[1024];
            while ((len = in.read(buf)) > 0) {
                actualBytes.write(buf, 0, len);
            }
        }
        actualBytes.close();
        String actual = actualBytes.toString();

        Assert.assertEquals(contents, actual);

        // TODO(ariels): Verify no *other* files on the bucket.
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

    /*
    @Test
    public void testRename() throws URISyntaxException, IOException {
        Configuration conf = new Configuration(true);
        conf.set(Constants.FS_LAKEFS_ACCESS_KEY, "<access_key>");
        conf.set(Constants.FS_LAKEFS_SECRET_KEY, "<secret_key>");
        conf.set(Constants.FS_LAKEFS_ENDPOINT_KEY, "http://localhost:8000/api/v1");
        conf.set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        // With lakefsFS the user does not need to point to the s3 gateway
        conf.set("fs.s3a.access.key", "<aws_access_key>");
        conf.set("fs.s3a.secret.key", "<aws_secret_key>");

        LakeFSFileSystem lfs = (LakeFSFileSystem)FileSystem.get(new URI("lakefs://aws-repo/main/nothere.txt"), conf);

        // Uncommitted -
        // rename existing src file to non-existing dst
        Path src = new Path("lakefs://aws-repo/main/peopleLakefs.parquet/_temporary/0/_temporary/attempt_202105191158068718340739981962409_0001_m_000000_1/part-00000-10b8c14f-51c0-4604-b7b5-45bf009bd3b0-c000.snappy.parquet");
        Path dst = new Path("lakefs://aws-repo/main/peopleLakefs.parquet/new-name.parquet");
        lfs.rename(src, dst);

        // rename non-existing src file - src not found, return false.
        Path src = new Path("lakefs://aws-repo/main/peopleLakefs.parquet/_temporary/0/_temporary/attempt_202105161150342255421072959703851_0001_m_000000_1/part-00000-c72e1fa6-9d86-4032-a2b1-f8dd1334e52e-c000.snappy.parquet");
        Path dst = new Path("lakefs://aws-repo/main/peopleLakefs.parquet/dst2.parquet");
        lfs.rename(src, dst);

        // rename existing src file to existing dst - no failure, src is rename, dst file is overridden with the renamed file.
        Path src = new Path("lakefs://aws-repo/main/peopleLakefs.parquet/_SUCCESS");
        Path dst = new Path("lakefs://aws-repo/main/peopleLakefs.parquet/new-name.parquet");
        lfs.rename(src, dst);

        // rename dir (a common prefix?), currently not working. for path type = common prefix I can't stat the object.
        Path src = new Path("lakefs://aws-repo/main/peopleLakefs.parquet/_temporary");
        Path dst = new Path("lakefs://aws-repo/main/peopleLakefs.parquet");
        lfs.rename(src, dst);
    }
     */
}
