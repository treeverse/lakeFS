package io.lakefs;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class LakeFSFileSystemTest {

    LakeFSFileSystem fs;

    @Before
    public void setUp() throws Exception {
        fs = new LakeFSFileSystem();
    }

    @After
    public void tearDown() throws Exception {
        fs = null;
    }

    @Test
    public void getUri() {
        URI u = fs.getUri();
        Assert.assertEquals(u.toString(), "lakefs://main");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAppend() throws IOException {
        fs.append(null, 0, null);
    }
}