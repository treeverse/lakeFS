package io.lakefs;

import io.lakefs.clients.api.ApiClient;
import io.lakefs.clients.api.ObjectsApi;
import io.lakefs.clients.api.StagingApi;
import io.lakefs.clients.api.auth.HttpBasicAuth;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class LakeFSClient {
    private static final String BASIC_AUTH = "basic_auth";
    private final ObjectsApi objects;
    private final StagingApi staging;
    private ApiClient apiClient;

    public LakeFSClient(Configuration conf) throws IOException {
        // setup lakeFS api client
        String endpoint = conf.get(Constants.FS_LAKEFS_ENDPOINT_KEY, "http://localhost:8000/api/v1");
        String accessKey = conf.get(Constants.FS_LAKEFS_ACCESS_KEY);
        if (accessKey == null) {
            throw new IOException("Missing lakeFS access key");
        }
        String secretKey = conf.get(Constants.FS_LAKEFS_SECRET_KEY);
        if (secretKey == null) {
            throw new IOException("Missing lakeFS secret key");
        }

        this.apiClient = io.lakefs.clients.api.Configuration.getDefaultApiClient();
        this.apiClient.setBasePath(endpoint);
        HttpBasicAuth basicAuth = (HttpBasicAuth) this.apiClient.getAuthentication(BASIC_AUTH);
        basicAuth.setUsername(accessKey);
        basicAuth.setPassword(secretKey);

        this.objects = new ObjectsApi(apiClient);
        this.staging = new StagingApi(apiClient);
    }

    public ObjectsApi getObjects() {
        return objects;
    }

    public StagingApi getStaging() {
        return staging;
    }
}
