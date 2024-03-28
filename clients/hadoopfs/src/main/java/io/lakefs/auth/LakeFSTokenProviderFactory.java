package io.lakefs.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import io.lakefs.Constants;
import io.lakefs.FSConfiguration;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeFSTokenProviderFactory {
    LakeFSTokenProviderFactory() {
    }
    public static LakeFSTokenProvider newLakeFSTokenProvider(String scheme, Configuration conf) throws IOException {
        String tokenProvider = FSConfiguration.get(conf, scheme, Constants.LAKEFS_AUTH_PROVIDER_KEY_SUFFIX);

        switch (tokenProvider) {
            case "TemporaryAWSCredentialsLakeFSTokenProvider":
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
                    public void refresh() {}
                };
                // parse additional headers to sign into a dict
                // set the default headers if not (host)
                Map<String, String> additionalHeaders = new HashMap<>();
                String endpoint = FSConfiguration.get(conf, scheme, Constants.ENDPOINT_KEY_SUFFIX, Constants.DEFAULT_CLIENT_ENDPOINT);
                additionalHeaders.put(Constants.DEFAULT_AUTH_SERVER_ID_HEADER, new URL(endpoint).getHost());
                additionalHeaders = FSConfiguration.getMap(conf, scheme, Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_ADDITIONAL_HEADERS, additionalHeaders);
                long expirationInSeconds = Long.parseLong(FSConfiguration.get(conf, scheme, Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_TOKEN_DURATION));
                String stsEndpoint = FSConfiguration.get(conf, scheme, Constants.TOKEN_AWS_STS_ENDPOINT);
                return new AWSLakeFSTokenProvider(awsProvider, new GetCallerIdentityV4Presigner(),stsEndpoint, additionalHeaders, expirationInSeconds);
            default:
                throw new IOException("Unknown LakeFS Token provider for: " + tokenProvider);
        }
    }
}
