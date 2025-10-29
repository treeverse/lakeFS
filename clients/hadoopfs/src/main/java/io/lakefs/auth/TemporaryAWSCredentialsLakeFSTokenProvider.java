package io.lakefs.auth;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import io.lakefs.Constants;
import io.lakefs.FSConfiguration;

import java.io.IOException;

public class TemporaryAWSCredentialsLakeFSTokenProvider extends AWSLakeFSTokenProvider {

    public static final String NAME = "io.lakefs.auth.TemporaryAWSCredentialsLakeFSTokenProvider";

    public TemporaryAWSCredentialsLakeFSTokenProvider(String scheme, Configuration conf) throws IOException {
        String accessKey = FSConfiguration.get(conf, scheme, Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_ACCESS_KEY_SUFFIX);
        if (accessKey == null) {
            throw new IOException("Missing AWS access key");
        }
        String secretKey = FSConfiguration.get(conf, scheme, Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SECRET_KEY_SUFFIX);
        if (secretKey == null) {
            throw new IOException("Missing AWS secret key");
        }
        String sessionToken = FSConfiguration.get(conf, scheme, Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SESSION_TOKEN_KEY_SUFFIX);
        if (sessionToken == null) {
            throw new IOException("Missing AWS session token");
        }
        AWSCredentialsProvider awsProvider = new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new BasicSessionCredentials(
                        accessKey,
                        secretKey,
                        sessionToken
                );
            }

            @Override
            public void refresh() {
            }
        };

        this.initialize(awsProvider, scheme, conf);
    }
}
