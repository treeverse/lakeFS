package io.lakefs;

import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.RepositoriesApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.auth.HttpBasicAuth;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Provides access to lakeFS API using client library.
 * This class uses the configuration to initialize API client and instance per API interface we expose.
 */
public class LakeFSClient {
    public static final String DEFAULT_SCHEME = "lakefs";
    public static final String DEFAULT_ENDPOINT = "http://localhost:8000/api/v1";
    private static final String BASIC_AUTH = "basic_auth";

    private final ObjectsApi objects;
    private final StagingApi staging;
    private final RepositoriesApi repositories;

    public LakeFSClient(String scheme, Configuration conf) throws IOException {
        String accessKey = getFSConfigurationValue(conf, scheme, "access.key");
        if (accessKey == null) {
            throw new IOException("Missing lakeFS access key");
        }

        String secretKey = getFSConfigurationValue(conf, scheme, "secret.key");
        if (secretKey == null) {
            throw new IOException("Missing lakeFS secret key");
        }

        ApiClient apiClient = io.lakefs.clients.api.Configuration.getDefaultApiClient();
        String endpoint = getFSConfigurationValue(conf, scheme, "endpoint");
        if (endpoint == null) {
            endpoint = DEFAULT_ENDPOINT;
        } else if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        apiClient.setBasePath(endpoint);

        HttpBasicAuth basicAuth = (HttpBasicAuth) apiClient.getAuthentication(BASIC_AUTH);
        basicAuth.setUsername(accessKey);
        basicAuth.setPassword(secretKey);

        this.objects = new ObjectsApi(apiClient);
        this.staging = new StagingApi(apiClient);
        this.repositories = new RepositoriesApi(apiClient);
    }

    public ObjectsApi getObjects() {
        return objects;
    }

    public StagingApi getStaging() {
        return staging;
    }

    public RepositoriesApi getRepositories() { return repositories; }

    private static String formatFSConfigurationKey(String scheme, String key) {
        return "fs." + scheme + "." + key;
    }

    /**
     * lookup value from configuration based on scheme and key suffix.
     * first try to get "fs.\<scheme\>.\<key suffix\>", if value not found use the default scheme
     * to build a key for lookup.
     * @param conf configuration object to get the value from
     * @param scheme used to format the key for lookup
     * @param keySuffix key suffix
     * @return key value or null in case no value found
     */
    private static String getFSConfigurationValue(Configuration conf, String scheme, String keySuffix) {
        String key = formatFSConfigurationKey(scheme, keySuffix);
        String value = conf.get(key);
        if (value == null && !scheme.equals(DEFAULT_SCHEME)) {
            value = conf.get(fallbackKey);
        }
        return value;
    }
}
