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
        conf.set(LakeFSFileSystem.FS_LAKEFS_ACCESS_KEY, "key");
        conf.set(LakeFSFileSystem.FS_LAKEFS_SECRET_KEY, "secret");
        conf.set(LakeFSFileSystem.FS_LAKEFS_ENDPOINT, "http://localhost:8000/api/v1");
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

    @Test(expected = UnsupportedOperationException.class)
    public void testAppend() throws IOException {
        fs.append(null, 0, null);
    }
}
