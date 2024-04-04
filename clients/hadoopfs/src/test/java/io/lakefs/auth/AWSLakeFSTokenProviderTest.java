package io.lakefs.auth;

import com.amazonaws.util.json.JSONObject;
import io.lakefs.Constants;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class AWSLakeFSTokenProviderTest {
    @Test
    public void testProviderIdentityTokenSerde() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("fs.lakefs." + Constants.LAKEFS_AUTH_PROVIDER_KEY_SUFFIX, TemporaryAWSCredentialsLakeFSTokenProvider.NAME);
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_ACCESS_KEY_SUFFIX, "accessKeyId");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SECRET_KEY_SUFFIX, "secretAccessKey");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SESSION_TOKEN_KEY_SUFFIX, "sessionToken");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_STS_ENDPOINT, "https://sts.amazonaws.com");

        AWSLakeFSTokenProvider provider = (AWSLakeFSTokenProvider)LakeFSTokenProviderFactory.newLakeFSTokenProvider(Constants.DEFAULT_SCHEME, conf);
        String identityToken = provider.newPresignedGetCallerIdentityToken();
        String decodedToken = new String(Base64.decodeBase64(identityToken.getBytes()));
        JSONObject identityParams = new JSONObject(decodedToken);
        // TODO(isan) finallize this test to check all the fields as expected in the identity token
        // that is the request object that lakeFS will recieve and pass to the auth service
        identityParams.keys().forEachRemaining(key -> {
            Assert.assertTrue(identityParams.has((String) key));
        });
    }
}
