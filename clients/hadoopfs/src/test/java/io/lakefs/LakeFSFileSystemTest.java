package io.lakefs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class LakeFSFileSystemTest {
    protected final LakeFSFileSystem fs = new LakeFSFileSystem();

    private static final DockerImageName MINIO = DockerImageName.parse("minio/minio:RELEASE.2021-05-16T05-32-34Z");
    protected static final String S3_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE";
    protected static final String S3_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    @Rule
    public final GenericContainer s3 = new GenericContainer(MINIO).
        withCommand("minio", "server", "/data").
        withEnv("MINIO_ROOT_USER", S3_ACCESS_KEY_ID).
        withEnv("MINIO_ROOT_PASSWORD", S3_SECRET_ACCESS_KEY).
        withExposedPorts(9000);

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set(io.lakefs.Constants.FS_LAKEFS_ACCESS_KEY, "<lakefs key>");
        conf.set(io.lakefs.Constants.FS_LAKEFS_SECRET_KEY, "<lakefs secret>");
        conf.set(io.lakefs.Constants.FS_LAKEFS_ENDPOINT_KEY, "http://localhost:8000/api/v1");
        URI name = new URI("lakefs://repo/master/file.txt");
        fs.initialize(name, conf);
    }

    @Test
    public void getUri() throws URISyntaxException, IOException {
        URI u = fs.getUri();
        Assert.assertNotNull(u);
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
