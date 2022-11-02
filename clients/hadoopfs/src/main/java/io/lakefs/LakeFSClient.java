package io.lakefs;

import io.lakefs.clients.api.*;
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
    private final BranchesApi branches;

    public LakeFSClient(String scheme, Configuration conf) throws IOException {
        String accessKey = FSConfiguration.get(conf, scheme, Constants.ACCESS_KEY_KEY_SUFFIX);
        if (accessKey == null) {
            throw new IOException("Missing lakeFS access key");
        }

        String secretKey = FSConfiguration.get(conf, scheme, Constants.SECRET_KEY_KEY_SUFFIX);
        if (secretKey == null) {
            throw new IOException("Missing lakeFS secret key");
        }

        ApiClient apiClient = io.lakefs.clients.api.Configuration.getDefaultApiClient();
        String endpoint = FSConfiguration.get(conf, scheme, Constants.ENDPOINT_KEY_SUFFIX, Constants.DEFAULT_CLIENT_ENDPOINT);
        if (endpoint.endsWith(Constants.SEPARATOR)) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        apiClient.setBasePath(endpoint);
        apiClient.addDefaultHeader("X-Lakefs-Client", "lakefs-hadoopfs/" + getClass().getPackage().getImplementationVersion());
        HttpBasicAuth basicAuth = (HttpBasicAuth) apiClient.getAuthentication(BASIC_AUTH);
        basicAuth.setUsername(accessKey);
        basicAuth.setPassword(secretKey);

        this.objects = new ObjectsApi(apiClient);
        this.staging = new StagingApi(apiClient);
        this.repositories = new RepositoriesApi(apiClient);
        this.branches = new BranchesApi(apiClient);
    }

    public ObjectsApi getObjects() {
        return objects;
    }

    public StagingApi getStaging() {
        return staging;
    }

    public RepositoriesApi getRepositories() { return repositories; }

    public BranchesApi getBranches() { return branches; }
}
