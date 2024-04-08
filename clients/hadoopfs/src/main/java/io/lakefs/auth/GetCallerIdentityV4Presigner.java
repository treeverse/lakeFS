package io.lakefs.auth;

import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.*;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.util.AwsHostNameUtils;
import com.amazonaws.util.BinaryUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * GetCallerIdentityV4Presigner is that knows how to generate a presigned URL for the GetCallerIdentity API. The presigned URL is signed using SigV4.
 * This class is extending AWS4Signer of AWS SDK version 1.7.4 and copies some functions from https://github.com/aws/aws-sdk-java/blob/1.7.4/src/main/java/com/amazonaws/auth/AWS4Signer.java
 * The reason we copy some functions is that we need to support aws-hadoop-2 which depends on aws sdk 1.7.4 while aws-hadoop-3 depends on aws sdk 1.11.375.
 * Everything that is copied starts with "overridden" prefix, a reasonable alternative would be to use @Override but, AWS made those functions final.
 */
public class GetCallerIdentityV4Presigner extends AWS4Signer implements STSGetCallerIdentityPresigner {
    protected static final String OVERRIDDEN_ALGORITHM = "AWS4-HMAC-SHA256";
    protected static final String OVERRIDDEN_TERMINATOR = "aws4_request";
    private static final String DEFAULT_ENCODING = "UTF-8";


    // copy from https://github.com/aws/aws-sdk-java/blob/679abaebd371b09e887afaa5386dc182be4c6498/aws-java-sdk-core/src/main/java/com/amazonaws/util/SdkHttpUtils.java#L39
    /**
     * Regex which matches any of the sequences that we need to fix up after
     * URLEncoder.encode().
     */
    private static final Pattern OVERRIDDEN_ENCODED_CHARACTERS_PATTERN;

    static {
        StringBuilder pattern = new StringBuilder();

        pattern
                .append(Pattern.quote("+"))
                .append("|")
                .append(Pattern.quote("*"))
                .append("|")
                .append(Pattern.quote("%7E"))
                .append("|")
                .append(Pattern.quote("%2F"));

        OVERRIDDEN_ENCODED_CHARACTERS_PATTERN = Pattern.compile(pattern.toString());
    }

    GetCallerIdentityV4Presigner() {
        super(false);
    }
    // copy from https://github.com/aws/aws-sdk-java/blob/679abaebd371b09e887afaa5386dc182be4c6498/aws-java-sdk-core/src/main/java/com/amazonaws/util/SdkHttpUtils.java#L66

    /**
     * Encode a string for use in the path of a URL; uses URLEncoder.encode,
     * (which encodes a string for use in the query portion of a URL), then
     * applies some postfilters to fix things up per the RFC. Can optionally
     * handle strings which are meant to encode a path (ie include '/'es
     * which should NOT be escaped).
     *
     * @param value the value to encode
     * @param path  true if the value is intended to represent a path
     * @return the encoded value
     */

    public static String urlEncode(final String value, final boolean path) {
        if (value == null) {
            return "";
        }

        try {
            String encoded = URLEncoder.encode(value, DEFAULT_ENCODING);

            Matcher matcher = OVERRIDDEN_ENCODED_CHARACTERS_PATTERN.matcher(encoded);
            StringBuffer buffer = new StringBuffer(encoded.length());

            while (matcher.find()) {
                String replacement = matcher.group(0);

                if ("+".equals(replacement)) {
                    replacement = "%20";
                } else if ("*".equals(replacement)) {
                    replacement = "%2A";
                } else if ("%7E".equals(replacement)) {
                    replacement = "~";
                } else if (path && "%2F".equals(replacement)) {
                    replacement = "/";
                }

                matcher.appendReplacement(buffer, replacement);
            }

            matcher.appendTail(buffer);
            return buffer.toString();

        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/679abaebd371b09e887afaa5386dc182be4c6498/aws-java-sdk-core/src/main/java/com/amazonaws/util/SdkHttpUtils.java#L194

    /**
     * Append the given path to the given baseUri.
     *
     * <p>This method will encode the given path but not the given
     * baseUri.</p>
     *
     * @param baseUri           The URI to append to (required, may be relative)
     * @param path              The path to append (may be null or empty)
     * @param escapeDoubleSlash Whether double-slash in the path should be escaped to "/%2F"
     * @return The baseUri with the (encoded) path appended
     */
    public static String appendUri(final String baseUri, String path, final boolean escapeDoubleSlash) {
        String resultUri = baseUri;
        if (path != null && path.length() > 0) {
            if (path.startsWith("/")) {
                // trim the trailing slash in baseUri, since the path already starts with a slash
                if (resultUri.endsWith("/")) {
                    resultUri = resultUri.substring(0, resultUri.length() - 1);
                }
            } else if (!resultUri.endsWith("/")) {
                resultUri += "/";
            }
            String encodedPath = GetCallerIdentityV4Presigner.urlEncode(path, true);
            if (escapeDoubleSlash) {
                encodedPath = encodedPath.replace("//", "/%2F");
            }
            resultUri += encodedPath;
        } else if (!resultUri.endsWith("/")) {
            resultUri += "/";
        }

        return resultUri;
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L294
    protected final String overriddenGetTimeStamp(long dateMilli) {
        // Convert milliseconds to Instant
        Instant instant = Instant.ofEpochMilli(dateMilli);

        // Format Instant to String in UTC time zone
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(ZoneId.of("UTC"));
        String formattedDateTime = formatter.format(instant);
        return formattedDateTime;
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L298
    protected final String overriddenGetDateStamp(long dateMilli) {

        // Convert milliseconds to Instant
        Instant instant = Instant.ofEpochMilli(dateMilli);

        // Format Instant to String in UTC time zone
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
                .withZone(ZoneId.of("UTC"));
        String formattedDate = formatter.format(instant);

        return formattedDate;
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L302
    protected final long overriddenGetDateFromRequest(Request<?> request) {
        int timeOffset = getTimeOffset(request);
        Date date = getSignatureDate(timeOffset);
        if (overriddenDate != null) date = overriddenDate;
        return date.getTime();
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L183
    protected String overriddenExtractRegionName(URI endpoint) {
        if (regionName != null) return regionName;

        return AwsHostNameUtils.parseRegionName(endpoint.getHost(),
                serviceName);
    }

    public String getSignedRegion(Request<GeneratePresignGetCallerIdentityRequest> r) {
        return overriddenExtractRegionName(r.getEndpoint());
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L190
    protected String overriddenExtractServiceName(URI endpoint) {
        if (serviceName != null) return serviceName;

        // This should never actually be called, as we should always be setting
        // a service name on the signer; retain it for now in case anyone is
        // using the AWS4Signer directly and not setting a service name
        // explicitly.

        return AwsHostNameUtils.parseServiceName(endpoint);
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L320
    protected String overriddenGetScope(Request<?> request, String dateStamp) {
        String regionName = overriddenExtractRegionName(request.getEndpoint());
        String serviceName = overriddenExtractServiceName(request.getEndpoint());
        String scope = dateStamp + "/" + regionName + "/" + serviceName + "/" + OVERRIDDEN_TERMINATOR;
        return scope;
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L241
    protected String overriddenGetCanonicalRequest(Request<?> request, String contentSha256) {
        /* This would url-encode the resource path for the first time */
        //String path = HttpUtils.appendUri(request.getEndpoint().getPath(), request.getResourcePath());
        String path = GetCallerIdentityV4Presigner.appendUri(request.getEndpoint().getPath(), request.getResourcePath(), false);
        String canonicalRequest =
                request.getHttpMethod().toString() + "\n" +
                        /* This would optionally double url-encode the resource path */
                        getCanonicalizedResourcePath(path, doubleUrlEncode) + "\n" +
                        getCanonicalizedQueryString(request) + "\n" +
                        getCanonicalizedHeaderString(request) + "\n" +
                        getSignedHeadersString(request) + "\n" +
                        contentSha256;
        log.debug("AWS4 Canonical Request: '\"" + canonicalRequest + "\"");
        return canonicalRequest;
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L257C22-L257C37
    protected String overriddenGetStringToSign(String algorithm, String dateTime, String scope, String canonicalRequest) {
        String stringToSign =
                algorithm + "\n" +
                        dateTime + "\n" +
                        scope + "\n" +
                        BinaryUtils.toHex(hash(canonicalRequest));
        log.debug("AWS4 String to Sign: '\"" + stringToSign + "\"");
        return stringToSign;
    }

    // copy from https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L268
    protected final byte[] overriddenComputeSignature(
            Request<?> request,
            String dateStamp,
            String timeStamp,
            String algorithm,
            String contentSha256,
            AWSCredentials sanitizedCredentials) {
        String regionName = overriddenExtractRegionName(request.getEndpoint());
        String serviceName = overriddenExtractServiceName(request.getEndpoint());
        String scope = dateStamp + "/" + regionName + "/" + serviceName + "/" + OVERRIDDEN_TERMINATOR;

        String stringToSign = overriddenGetStringToSign(algorithm, timeStamp, scope, overriddenGetCanonicalRequest(request, contentSha256));

        // AWS4 uses a series of derived keys, formed by hashing different
        // pieces of data
        byte[] kSecret = ("AWS4" + sanitizedCredentials.getAWSSecretKey()).getBytes();
        byte[] kDate = sign(dateStamp, kSecret, SigningAlgorithm.HmacSHA256);
        byte[] kRegion = sign(regionName, kDate, SigningAlgorithm.HmacSHA256);
        byte[] kService = sign(serviceName, kRegion, SigningAlgorithm.HmacSHA256);
        byte[] kSigning = sign(OVERRIDDEN_TERMINATOR, kService, SigningAlgorithm.HmacSHA256);

        byte[] signature = sign(stringToSign.getBytes(), kSigning, SigningAlgorithm.HmacSHA256);
        return signature;
    }

    public Request<GeneratePresignGetCallerIdentityRequest> presignRequest(GeneratePresignGetCallerIdentityRequest input) throws IOException {
        Request request = new DefaultRequest("sts");
        request.setEndpoint(input.getStsEndpoint());
        request.addParameter(STSGetCallerIdentityPresigner.AMZ_ACTION_PARAM_NAME, "GetCallerIdentity");
        request.addParameter(STSGetCallerIdentityPresigner.AMZ_VERSION_PARAM_NAME, "2011-06-15");
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
            request.addParameter(STSGetCallerIdentityPresigner.AMZ_SECURITY_TOKEN_PARAM_NAME, ((AWSSessionCredentials) sanitizedCredentials).getSessionToken());
        }

        // add additional headers to sign (i.e X-lakeFS-Server-ID)
        for (Map.Entry<String, String> entry : input.getAdditionalHeaders().entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }

        // Add required parameters for v4 signing
        long now = System.currentTimeMillis();

        final String timeStamp = overriddenGetTimeStamp(now);
        request.addParameter(STSGetCallerIdentityPresigner.AMZ_ALGORITHM_PARAM_NAME, OVERRIDDEN_ALGORITHM);
        request.addParameter(STSGetCallerIdentityPresigner.AMZ_DATE_PARAM_NAME, timeStamp);
        request.addParameter(STSGetCallerIdentityPresigner.AMZ_SIGNED_HEADERS_PARAM_NAME, getSignedHeadersString(request));
        request.addParameter(STSGetCallerIdentityPresigner.AMZ_EXPIRES_PARAM_NAME, String.valueOf(input.getExpirationInSeconds()));

        // add X-Amz-Credential header (e.g <AccessKeyId>/<date>/<region>>/sts/aws4_request)

        long dateMilli = overriddenGetDateFromRequest(request);
        final String dateStamp = overriddenGetDateStamp(dateMilli);
        String scope = overriddenGetScope(request, dateStamp);
        String signingCredentials = sanitizedCredentials.getAWSAccessKeyId() + "/" + scope;

        request.addParameter(STSGetCallerIdentityPresigner.AMZ_CREDENTIAL_PARAM_NAME, signingCredentials);

        // contentSha256 is HashedPayload Hex(SHA256Hash("")) see https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html#create-string-to-sign
        byte[] contentDigest = this.hash(request.getContent());
        String contentSha256 = BinaryUtils.toHex(contentDigest);

        byte[] signature = overriddenComputeSignature(request, dateStamp, timeStamp, OVERRIDDEN_ALGORITHM, contentSha256, sanitizedCredentials);

        request.addParameter(STSGetCallerIdentityPresigner.AMZ_SIGNATURE_PARAM_NAME, BinaryUtils.toHex(signature));

        return request;
    }
}