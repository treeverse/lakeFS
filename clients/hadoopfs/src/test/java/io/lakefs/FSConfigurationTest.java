package io.lakefs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class FSConfigurationTest {

    @Test
    public void testGet() {
        Configuration conf = new Configuration(false);
        conf.set("fs.lakefs.key1", "lakefs1");
        conf.set("fs.scheme.key1", "scheme1");
        conf.set("fs.lakefs.key2", "lakefs2");
        conf.set("fs.scheme.key3", "scheme3");
        Assert.assertEquals("lakefs1", FSConfiguration.get(conf, "lakefs", "key1"));
        Assert.assertEquals("lakefs1", FSConfiguration.get(conf, "lakefs", "key1", "default"));
        Assert.assertEquals("scheme1", FSConfiguration.get(conf, "scheme", "key1"));
        Assert.assertEquals("scheme1", FSConfiguration.get(conf, "scheme", "key1", "default"));
        Assert.assertEquals("lakefs2", FSConfiguration.get(conf, "scheme", "key2"));
        Assert.assertEquals("lakefs2", FSConfiguration.get(conf, "scheme", "key2", "default"));
        Assert.assertEquals("lakefs2", FSConfiguration.get(conf, "lakefs", "key2"));
        Assert.assertEquals("lakefs2", FSConfiguration.get(conf, "lakefs", "key2", "default"));
        Assert.assertEquals("scheme3", FSConfiguration.get(conf, "scheme", "key3"));
        Assert.assertEquals("scheme3", FSConfiguration.get(conf, "scheme", "key3", "default"));
        Assert.assertNull(FSConfiguration.get(conf, "lakefs", "key3"));
        Assert.assertEquals("default", FSConfiguration.get(conf, "lakefs", "key3", "default"));
        Assert.assertNull(FSConfiguration.get(conf, "lakefs", "missing"));
        Assert.assertEquals("default", FSConfiguration.get(conf, "lakefs", "missing", "default"));
    }

    @Test
    public void testGetInt() {
        Configuration conf = new Configuration(false);
        conf.setInt("fs.lakefs.key1", 1);
        conf.setInt("fs.scheme.key1", 11);
        conf.setInt("fs.lakefs.key2", 2);
        conf.setInt("fs.scheme.key3", 33);
        conf.set("fs.lakefs.bad.key", "bad");
        Assert.assertEquals(1, FSConfiguration.getInt(conf, "lakefs", "key1", 99));
        Assert.assertEquals(11, FSConfiguration.getInt(conf, "scheme", "key1", 99));
        Assert.assertEquals(2, FSConfiguration.getInt(conf, "scheme", "key2", 99));
        Assert.assertEquals(2, FSConfiguration.getInt(conf, "lakefs", "key2", 99));
        Assert.assertEquals(33, FSConfiguration.getInt(conf, "scheme", "key3", 99));
        Assert.assertEquals(99, FSConfiguration.getInt(conf, "lakefs", "key3", 99));
        Assert.assertEquals(99, FSConfiguration.getInt(conf, "lakefs", "missing", 99));
        Assert.assertThrows(NumberFormatException.class, () -> FSConfiguration.getInt(conf, "lakefs", "bad.key", 99));
    }
}