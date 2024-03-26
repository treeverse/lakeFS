package io.lakefs.auth;

import io.lakefs.Constants;
import io.lakefs.FSConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;


import java.io.IOException;

public class LakeFSTokenProviderFactoryTest {
    @Test
    public void testSanity() throws IOException {
        Configuration conf = new Configuration(false);
        conf.set("fs.lakefs." + Constants.TOKEN_PROVIDER_KEY_SUFFIX, "TemporaryAWSCredentialsLakeFSTokenProvider");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_ACCESS_KEY_SUFFIX, "<accessKeyId>");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SECRET_KEY_SUFFIX, "<secretAccess>");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SESSION_TOKEN_KEY_SUFFIX, "<sessionToken>");
        LakeFSTokenProviderFactory.newLakeFSTokenProvider(Constants.DEFAULT_SCHEME, conf);

//        conf.set("fs.lakefs.key1", "lakefs1");
//        Assert.assertEquals("default", FSConfiguration.get(conf, "lakefs", "missing", "default"));
    }
}
