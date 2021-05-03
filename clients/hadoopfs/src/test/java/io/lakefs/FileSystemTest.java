package io.lakefs;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

public class FileSystemTest {

    @Test
    public void getUri() {
        FileSystem fs = new FileSystem();
        URI u = fs.getUri();
        Assert.assertEquals(u.toString(), "lakefs://main");
    }

}