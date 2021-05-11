package io.lakefs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class LakeFSFileSystemTest {

    private LakeFSFileSystem fs;

    @Before
    public void setUp() throws Exception {
        fs = new LakeFSFileSystem();
        Configuration conf = new Configuration(false);
        conf.set(io.lakefs.Constants.FS_LAKEFS_ACCESS_KEY, "<lakefs key>");
        conf.set(io.lakefs.Constants.FS_LAKEFS_SECRET_KEY, "<lakefs secret>");
        conf.set(io.lakefs.Constants.FS_LAKEFS_ENDPOINT_KEY, "http://localhost:8000/api/v1");
        URI name = new URI("lakefs://repo/master/file.txt");
        fs.initialize(name, conf);
    }

    @After
    public void tearDown() throws Exception {
        fs = null;
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
}
