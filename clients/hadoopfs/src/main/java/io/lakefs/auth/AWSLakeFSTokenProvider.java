package io.lakefs.auth;

import com.amazonaws.Request;
import com.amazonaws.auth.*;
import io.lakefs.Constants;
import io.lakefs.FSConfiguration;
import io.lakefs.clients.sdk.ApiClient;
import io.lakefs.clients.sdk.model.AuthenticationToken;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;


public class AWSLakeFSTokenProvider implements LakeFSTokenProvider {
    STSGetCallerIdentityPresigner stsPresigner;
    AWSCredentialsProvider awsProvider;
    AuthenticationToken lakeFSAuthToken = null;
    String stsEndpoint;
    Map<String, String> stsAdditionalHeaders;
    int stsExpirationInSeconds;
    ApiClient lakeFSApi;

    AWSLakeFSTokenProvider() {
    }

    public AWSLakeFSTokenProvider(AWSCredentialsProvider awsProvider, ApiClient lakeFSClient, STSGetCallerIdentityPresigner stsPresigner, String stsEndpoint, Map<String, String> stsAdditionalHeaders, int stsExpirationInSeconds) {
        this.awsProvider = awsProvider;
        this.stsPresigner = stsPresigner;
        this.lakeFSApi = lakeFSClient;
        this.stsEndpoint = stsEndpoint;
        this.stsAdditionalHeaders = stsAdditionalHeaders;
        this.stsExpirationInSeconds = stsExpirationInSeconds;
    }

    protected void initialize(AWSCredentialsProvider awsProvider, String scheme, Configuration conf) throws IOException {
        // aws credentials provider
        this.awsProvider = awsProvider;

        // sts endpoint to call STS
        this.stsEndpoint = FSConfiguration.get(conf, scheme, Constants.TOKEN_AWS_STS_ENDPOINT);

        if (this.stsEndpoint == null) {
            throw new IOException("Missing sts endpoint");
        }

        // Expiration for each identity token generated (they are very short-lived and only used for exchange, the value is part of the signature)
        this.stsExpirationInSeconds = FSConfiguration.getInt(conf, scheme, Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_TOKEN_DURATION_SECONDS, 60);

        // initialize the presigner
        this.stsPresigner = new GetCallerIdentityV4Presigner();

        // initialize a lakeFS api client

        this.lakeFSApi = io.lakefs.clients.sdk.Configuration.getDefaultApiClient();
        this.lakeFSApi.addDefaultHeader("X-Lakefs-Client", "lakefs-hadoopfs/" + getClass().getPackage().getImplementationVersion());
        String endpoint = FSConfiguration.get(conf, scheme, Constants.ENDPOINT_KEY_SUFFIX, Constants.DEFAULT_CLIENT_ENDPOINT);
        if (endpoint.endsWith(Constants.SEPARATOR)) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        String sessionId = FSConfiguration.get(conf, scheme, Constants.SESSION_ID);
        if (sessionId != null) {
            this.lakeFSApi.addDefaultCookie("sessionId", sessionId);
        }
        this.lakeFSApi.setBasePath(endpoint);

        // set additional headers (non-canonical) to sign with each request to STS
        // non-canonical headers are signed by the presigner and sent to STS for verification in the requests by lakeFS to exchange the token
        Map<String, String> additionalHeaders = FSConfiguration.getMap(conf, scheme, Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_ADDITIONAL_HEADERS);
        if (additionalHeaders == null) {
            additionalHeaders = new HashMap<String, String>() {{
                put(Constants.DEFAULT_AUTH_PROVIDER_SERVER_ID_HEADER, new URL(lakeFSApi.getBasePath()).getHost());
            }};
            // default header to sign is the lakeFS server host name
            additionalHeaders.put(Constants.DEFAULT_AUTH_PROVIDER_SERVER_ID_HEADER, new URL(endpoint).getHost());
        }
        this.stsAdditionalHeaders = additionalHeaders;
    }

    @Override
    public String getToken() {
        if (needsNewToken()) {
            refresh();
        }
        return this.lakeFSAuthToken.getToken();
    }

    private boolean needsNewToken() {
        return this.lakeFSAuthToken == null || this.lakeFSAuthToken.getTokenExpiration() < System.currentTimeMillis();
    }

    public Request<GeneratePresignGetCallerIdentityRequest> newPresignedRequest() throws Exception {
        GeneratePresignGetCallerIdentityRequest stsReq = new GeneratePresignGetCallerIdentityRequest(
                new URI(this.stsEndpoint),
                this.awsProvider.getCredentials(),
                this.stsAdditionalHeaders,
                this.stsExpirationInSeconds
        );
        return this.stsPresigner.presignRequest(stsReq);
    }

    public String newPresignedGetCallerIdentityToken() throws Exception {
        Request<GeneratePresignGetCallerIdentityRequest> signedRequest = this.newPresignedRequest();
        Map<String, String> params = signedRequest.getParameters();
        String securityToken = null;
        if (this.awsProvider.getCredentials() instanceof AWSSessionCredentials) {
            AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) this.awsProvider.getCredentials();

        }


        // generate token parameters object
        LakeFSExternalPrincipalIdentityRequest identityTokenParams = new LakeFSExternalPrincipalIdentityRequest(
                signedRequest.getHttpMethod().name(),
                signedRequest.getEndpoint().getHost(),
                this.stsPresigner.getSignedRegion(signedRequest),
                params.get(STSGetCallerIdentityPresigner.AMZ_ACTION_PARAM_NAME),
                params.get(STSGetCallerIdentityPresigner.AMZ_DATE_PARAM_NAME),
                params.get(STSGetCallerIdentityPresigner.AMZ_EXPIRES_PARAM_NAME),
                this.awsProvider.getCredentials().getAWSAccessKeyId(),
                params.get(STSGetCallerIdentityPresigner.AMZ_SIGNATURE_PARAM_NAME),
                Arrays.asList(params.get(STSGetCallerIdentityPresigner.AMZ_SIGNED_HEADERS_PARAM_NAME).split(";")),
                params.get(STSGetCallerIdentityPresigner.AMZ_VERSION_PARAM_NAME),
                params.get(STSGetCallerIdentityPresigner.AMZ_ALGORITHM_PARAM_NAME),
                params.get(STSGetCallerIdentityPresigner.AMZ_SECURITY_TOKEN_PARAM_NAME)
        );

        // base64 encode
        return Base64.encodeBase64String(identityTokenParams.toJSON().getBytes());
    }

    private void newToken() throws Exception {
        String identityToken = this.newPresignedGetCallerIdentityToken();
        /*
        TODO(isan)
         depends on missing functionality PR https://github.com/treeverse/lakeFS/pull/7578 being merged.
         before merging this code - implement the call to lakeFS.
         it will introduce the functionality in the generated client of actually doing the login.
         call lakeFS to exchange the token for a lakeFS token
         The flow will be:
         1. use this.lakeFSApi Client with ExternalPrincipal API class (no auth required)
         2. this.lakeFSAuthToken = call api.ExternalPrincipalLogin(identityToken, <lakeFS Token optional TTL>)
        */
        // dummy initiation
        this.lakeFSAuthToken = new AuthenticationToken();
        this.lakeFSAuthToken.setTokenExpiration(System.currentTimeMillis() + 60);
    }

    // refresh can be called to create a new token regardless if the current token is expired or not or does not exist.
    @Override
    public void refresh() {
        synchronized (this) {
            try {
                newToken();
            } catch (Exception e) {
                throw new RuntimeException("Failed to refresh token", e);
            }
        }
    }
}
