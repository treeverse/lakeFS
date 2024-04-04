package io.lakefs.auth;

import org.apache.hadoop.conf.Configuration;
import io.lakefs.Constants;
import io.lakefs.FSConfiguration;

import java.io.IOException;

public class LakeFSTokenProviderFactory {

    public static LakeFSTokenProvider newLakeFSTokenProvider(String scheme, Configuration conf) throws IOException {
        String providerClassPath = FSConfiguration.get(conf, scheme, Constants.LAKEFS_AUTH_PROVIDER_KEY_SUFFIX);
        if (providerClassPath == null) {
            throw new IOException("Missing lakeFS Auth provider configuration");
        }
        // Load the Token Provider class and create an instance of it.
        // This setup is similar to Custom AWS Credential Provider in Hadoop-AWS: allow using custom Token Provider provided by runtime.
        try {
            LakeFSTokenProvider provider = (LakeFSTokenProvider) Class.forName(providerClassPath).getConstructor(String.class, Configuration.class).newInstance(scheme, conf);
            return provider;
        } catch (Exception e) {
            throw new IOException(String.format("Failed loading LakeFSTokenProvider %s %s \n%s", providerClassPath, e.getCause(), e));
        }
    }
}
