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
    public static final String DEFAULT_ENDPOINT = "http://localhost:8000/api/v1";
    private static final String BASIC_AUTH = "basic_auth";
    public static final String ACCESS_KEY_KEY_SUFFIX = "access.key";
    public static final String SECRET_KEY_KEY_SUFFIX = "secret.key";
    public static final String ENDPOINT_KEY_SUFFIX = "endpoint";

    private final ObjectsApi objects;
    private final StagingApi staging;
    private final RepositoriesApi repositories;

    public LakeFSClient(String scheme, Configuration conf) throws IOException {
        String accessKey = FSConfiguration.get(conf, scheme, ACCESS_KEY_KEY_SUFFIX);
        if (accessKey == null) {
            throw new IOException("Missing lakeFS access key");
        }

        String secretKey = FSConfiguration.get(conf, scheme, SECRET_KEY_KEY_SUFFIX);
        if (secretKey == null) {
            throw new IOException("Missing lakeFS secret key");
        }

        ApiClient apiClient = io.lakefs.clients.api.Configuration.getDefaultApiClient();
        String endpoint = FSConfiguration.get(conf, scheme, ENDPOINT_KEY_SUFFIX, DEFAULT_ENDPOINT);
        if (endpoint.endsWith("/")) {
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
}
