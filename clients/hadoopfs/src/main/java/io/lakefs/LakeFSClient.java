package io.lakefs;

import io.lakefs.clients.sdk.*;
import io.lakefs.clients.sdk.auth.HttpBasicAuth;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Provides access to lakeFS API using client library.
 * This class uses the configuration to initialize API client and instance per API interface we expose.
 */
public class LakeFSClient {
    private static final String BASIC_AUTH = "basic_auth";

    private final ObjectsApi objectsApi;
    private final StagingApi stagingApi;
    private final RepositoriesApi repositoriesApi;
    private final BranchesApi branchesApi;
    private final ConfigApi configApi;

    public LakeFSClient(String scheme, Configuration conf) throws IOException {
        String accessKey = FSConfiguration.get(conf, scheme, Constants.ACCESS_KEY_KEY_SUFFIX);
        if (accessKey == null) {
            throw new IOException("Missing lakeFS access key");
        }

        String secretKey = FSConfiguration.get(conf, scheme, Constants.SECRET_KEY_KEY_SUFFIX);
        if (secretKey == null) {
            throw new IOException("Missing lakeFS secret key");
        }

        ApiClient apiClient = io.lakefs.clients.sdk.Configuration.getDefaultApiClient();
        String endpoint = FSConfiguration.get(conf, scheme, Constants.ENDPOINT_KEY_SUFFIX, Constants.DEFAULT_CLIENT_ENDPOINT);
        if (endpoint.endsWith(Constants.SEPARATOR)) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        apiClient.setBasePath(endpoint);
        apiClient.addDefaultHeader("X-Lakefs-Client", "lakefs-hadoopfs/" + getClass().getPackage().getImplementationVersion());
        HttpBasicAuth basicAuth = (HttpBasicAuth) apiClient.getAuthentication(BASIC_AUTH);
        basicAuth.setUsername(accessKey);
        basicAuth.setPassword(secretKey);

        String sessionId = FSConfiguration.get(conf, scheme, Constants.SESSION_ID);
        if (sessionId != null) {
            apiClient.addDefaultCookie("sessionId", sessionId);
        }

        this.objectsApi = new ObjectsApi(apiClient);
        this.stagingApi = new StagingApi(apiClient);
        this.repositoriesApi = new RepositoriesApi(apiClient);
        this.branchesApi = new BranchesApi(apiClient);
        this.configApi = new ConfigApi(apiClient);
    }

    public ObjectsApi getObjectsApi() {
        return objectsApi;
    }

    public StagingApi getStagingApi() {
        return stagingApi;
    }

    public RepositoriesApi getRepositoriesApi() { return repositoriesApi; }

    public BranchesApi getBranchesApi() { return branchesApi; }

    public ConfigApi getConfigApi() {
        return configApi;
    }

}
