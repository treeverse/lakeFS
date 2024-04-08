package io.lakefs.auth;

import com.amazonaws.auth.AWSCredentials;

import java.net.URI;
import java.util.Map;

class GeneratePresignGetCallerIdentityRequest {
    private final Map<String, String> additionalHeaders;
    private final int expirationInSeconds;
    private final URI stsEndpoint;

    private AWSCredentials credentials;

    public GeneratePresignGetCallerIdentityRequest(URI stsEndpoint, AWSCredentials credentials, Map<String, String> additionalHeaders, int expirationInSeconds) {
        this.credentials = credentials;
        this.stsEndpoint = stsEndpoint;
        this.additionalHeaders = additionalHeaders;
        this.expirationInSeconds = expirationInSeconds;
    }

    public Map<String, String> getAdditionalHeaders() {
        return additionalHeaders;
    }

    public int getExpirationInSeconds() {
        return expirationInSeconds;
    }

    public URI getStsEndpoint() {
        return stsEndpoint;
    }

    public AWSCredentials getCredentials() {
        return credentials;
    }

}