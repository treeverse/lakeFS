package io.lakefs.auth;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.*;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.util.BinaryUtils;
import org.apache.commons.codec.binary.Base64;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import com.amazonaws.util.json.JSONObject;

class GeneratePresignGetCallerIdentityRequest extends AmazonWebServiceRequest {
    private final Map<String, String> additionalHeaders;
    private final Long expirationInSeconds;
    private final URI stsEndpoint;

    private AWSCredentials credentials;

    public GeneratePresignGetCallerIdentityRequest(URI stsEndpoint, AWSCredentials credentials, Map<String, String> additionalHeaders, Long expirationInSeconds) {
        this.credentials = credentials;
        this.stsEndpoint = stsEndpoint;
        this.additionalHeaders = additionalHeaders;
        this.expirationInSeconds = expirationInSeconds;
    }

    public Map<String, String> getAdditionalHeaders() {
        return additionalHeaders;
    }

    public Long getExpirationInSeconds() {
        return expirationInSeconds;
    }

    public URI getStsEndpoint() {
        return stsEndpoint;
    }

    public AWSCredentials getCredentials() {
        return credentials;
    }
}

interface STSGetCallerIdentitPresigner {
    Request<GeneratePresignGetCallerIdentityRequest> presignRequest(GeneratePresignGetCallerIdentityRequest r) throws IOException;
}

class GetCallerIdentityV4Presigner extends AWS4Signer implements  STSGetCallerIdentitPresigner{
    /**
     * Seconds in a week, which is the max expiration time Sig-v4 accepts
     */
    private final static long MAX_EXPIRATION_TIME_IN_SECONDS = 60 * 60 * 24 * 7;

    GetCallerIdentityV4Presigner() {
        super(false);
    }

    public Request<GeneratePresignGetCallerIdentityRequest> presignRequest(GeneratePresignGetCallerIdentityRequest input) throws IOException {
        Request request = new DefaultRequest(input, "sts");
        request.setEndpoint(input.getStsEndpoint());
        request.addParameter("Action", "GetCallerIdentity");
        request.addParameter("Version", "2011-06-15");
        // we want to support generating only POST requests, adding an empty stream is a hack
        // to make it use query params when Signer calls this.usePayloadForQueryParameters returns false for an empty POST request.
        request.setContent(new InputStream() {
            @Override
            public int read() throws IOException {
                return -1;
            }
        });

        request.setHttpMethod(HttpMethodName.POST);
        addHostHeader(request);

        AWSCredentials sanitizedCredentials = sanitizeCredentials(input.getCredentials());

        if (sanitizedCredentials instanceof AWSSessionCredentials) {
            // For SigV4 presigning URL, we need to add "x-amz-security-token"
            // as a query string parameter, before constructing the canonical request.
            request.addParameter("X-Amz-Security-Token", ((AWSSessionCredentials) sanitizedCredentials).getSessionToken());
        }

        // add additional headers to sign (i.e X-lakeFS-Server-ID)
        for (Map.Entry<String, String> entry : input.getAdditionalHeaders().entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }

        // Add required parameters for v4 signing
        long now = System.currentTimeMillis();
        final String timeStamp = getTimeStamp(now);
        request.addParameter("X-Amz-Algorithm", ALGORITHM);
        request.addParameter("X-Amz-Date", timeStamp);
        request.addParameter("X-Amz-SignedHeaders", getSignedHeadersString(request));

        if (input.getExpirationInSeconds() > MAX_EXPIRATION_TIME_IN_SECONDS) {
            throw new IOException("Requests that are pre-signed by SigV4 algorithm are valid for at most 7 days. " +
                    "The expiration [" + input.getExpirationInSeconds() + "] has exceeded this limit.");
        }

        request.addParameter("X-Amz-Expires", input.getExpirationInSeconds().toString());

        // add X-Amz-Credential header (e.g <AccessKeyId>/<date>/<region>>/sts/aws4_request)
        long dateMilli = getDateFromRequest(request);
        final String dateStamp = getDateStamp(dateMilli);
        String scope = getScope(request, dateStamp);
        String signingCredentials = sanitizedCredentials.getAWSAccessKeyId() + "/" + scope;

        request.addParameter("X-Amz-Credential", signingCredentials);

        // contentSha256 is HashedPayload Hex(SHA256Hash("")) see https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html#create-string-to-sign
        byte[] contentDigest = this.hash(request.getContent());
        String contentSha256 = BinaryUtils.toHex(contentDigest);

        HeaderSigningResult headerSigningResult = computeSignature(
                request,
                dateStamp,
                timeStamp,
                ALGORITHM,
                contentSha256,
                sanitizedCredentials);

        request.addParameter("X-Amz-Signature", BinaryUtils.toHex(headerSigningResult.getSignature()));

        return request;
    }
}

public class AWSLakeFSTokenProvider implements LakeFSTokenProvider {
    STSGetCallerIdentitPresigner stsPresigner;
    AWSCredentialsProvider awsProvider;
    String lakeFSAuthToken = null;
    String stsEndpoint;
    Map<String,String> stsAdditionalHeaders;
    Long stsExpirationInSeconds;
    AWSLakeFSTokenProvider(AWSCredentialsProvider awsProvider, STSGetCallerIdentitPresigner stsPresigner, String stsEndpoint, Map<String,String> stsAdditionalHeaders, Long stsExpirationInSeconds) {
        this.stsPresigner = stsPresigner;
        this.awsProvider = awsProvider;
        this.stsEndpoint = stsEndpoint;
        this.stsAdditionalHeaders = stsAdditionalHeaders;
        this.stsExpirationInSeconds = stsExpirationInSeconds;
    }

    @Override
    public String getToken() {
        if (needsNewToken()) {
            refresh();
        }
        return this.lakeFSAuthToken;
    }

    private boolean needsNewToken() {
        return this.lakeFSAuthToken == null;
    }

    public String newPresignedGetCallerIdentityToken() throws Exception {
        GeneratePresignGetCallerIdentityRequest stsReq = new GeneratePresignGetCallerIdentityRequest(
                new URI(this.stsEndpoint),
                this.awsProvider.getCredentials(),
                this.stsAdditionalHeaders,
                this.stsExpirationInSeconds
        );
        Request<GeneratePresignGetCallerIdentityRequest> signedRequest = this.stsPresigner.presignRequest(stsReq);

        // generate anonymous class that will be seriallized to json string
        JSONObject identityTokenParams = new JSONObject();
        identityTokenParams.put("method", signedRequest.getHttpMethod().name());
        identityTokenParams.put("endpoint", signedRequest.getEndpoint().toString());
        identityTokenParams.put("signedHeaders", signedRequest.getHeaders().keySet().toArray());
        identityTokenParams.put("expiration", this.stsExpirationInSeconds);
        identityTokenParams.put("signedParams", signedRequest.getParameters());

        // base64 encode
        return "lakefs.iam.v1."+Base64.encodeBase64String(identityTokenParams.toString().getBytes());
    }

    private void newToken() throws Exception {
        String token = this.newPresignedGetCallerIdentityToken();
        // call lakeFS to exchange the token for a lakeFS token
        this.lakeFSAuthToken = "lakefs jwt 123";
    }

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
