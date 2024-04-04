package io.lakefs.auth;

import io.lakefs.Constants;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class LakeFSTokenProviderFactoryTest {
    @Test
    public void testLakeFSTokenProvidersLoad() throws IOException {
        Configuration conf = new Configuration(false);
        conf.set("fs.lakefs." + Constants.LAKEFS_AUTH_PROVIDER_KEY_SUFFIX, TemporaryAWSCredentialsLakeFSTokenProvider.class.getName());
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_ACCESS_KEY_SUFFIX, "...");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SECRET_KEY_SUFFIX, "...");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SESSION_TOKEN_KEY_SUFFIX, "...");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_STS_ENDPOINT, "...");
        LakeFSTokenProvider provider = LakeFSTokenProviderFactory.newLakeFSTokenProvider(Constants.DEFAULT_SCHEME, conf);
        Assert.assertEquals("loaded wrong class", TemporaryAWSCredentialsLakeFSTokenProvider.class.getName(), provider.getClass().getName());
    }
}
