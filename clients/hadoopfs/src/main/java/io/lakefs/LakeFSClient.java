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
    private static final String BASIC_AUTH = "basic_auth";
    private final ObjectsApi objects;
    private final StagingApi staging;
    private final RepositoriesApi repositories;

    public LakeFSClient(Configuration conf) throws IOException {
        String accessKey = conf.get(Constants.FS_LAKEFS_ACCESS_KEY);
        if (accessKey == null) {
            throw new IOException("Missing lakeFS access key");
        }
        String secretKey = conf.get(Constants.FS_LAKEFS_SECRET_KEY);
        if (secretKey == null) {
            throw new IOException("Missing lakeFS secret key");
        }

        ApiClient apiClient = io.lakefs.clients.api.Configuration.getDefaultApiClient();
        String endpoint = conf.get(Constants.FS_LAKEFS_ENDPOINT_KEY, "http://localhost:8000/api/v1");
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
