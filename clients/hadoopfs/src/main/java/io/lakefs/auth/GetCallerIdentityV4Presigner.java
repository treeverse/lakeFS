package io.lakefs.auth;

import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.*;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.util.BinaryUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * GetCallerIdentityV4Presigner is that knows how to generate a presigned URL for the GetCallerIdentity API.
 * The presigned URL is signed using SigV4.
 * TODO: when we move to AWS SDK v2, we can use the AWS SDK's implementation of this (depends on hadoop-aws upgrading their own AWS SDK dependency).
 */
public class GetCallerIdentityV4Presigner extends AWS4Signer implements STSGetCallerIdentityPresigner {

    GetCallerIdentityV4Presigner() {
        super(false);
    }

    public Request<GeneratePresignGetCallerIdentityRequest> presignRequest(GeneratePresignGetCallerIdentityRequest input) throws IOException {
        Request request = new DefaultRequest("sts");
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
        request.addParameter("X-Amz-Expires", String.valueOf(input.getExpirationInSeconds()));

        // add X-Amz-Credential header (e.g <AccessKeyId>/<date>/<region>>/sts/aws4_request)
        long dateMilli = getDateFromRequest(request);
        final String dateStamp = getDateStamp(dateMilli);
        String scope = getScope(request, dateStamp);
        String signingCredentials = sanitizedCredentials.getAWSAccessKeyId() + "/" + scope;

        request.addParameter("X-Amz-Credential", signingCredentials);

        // contentSha256 is HashedPayload Hex(SHA256Hash("")) see https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html#create-string-to-sign
        byte[] contentDigest = this.hash(request.getContent());
        String contentSha256 = BinaryUtils.toHex(contentDigest);

        HeaderSigningResult headerSigningResult = computeSignature(request, dateStamp, timeStamp, ALGORITHM, contentSha256, sanitizedCredentials);

        request.addParameter("X-Amz-Signature", BinaryUtils.toHex(headerSigningResult.getSignature()));

        return request;
    }
}