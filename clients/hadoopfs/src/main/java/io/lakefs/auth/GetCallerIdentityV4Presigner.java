package io.lakefs.auth;
//import org.joda.time.format.DateTimeFormat;
import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.*;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.util.AwsHostNameUtils;
import com.amazonaws.util.BinaryUtils;
//import com.amazonaws.util.HttpUtils;
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
 * GetCallerIdentityV4Presigner is that knows how to generate a presigned URL for the GetCallerIdentity API.
 * The presigned URL is signed using SigV4.
 * TODO: when we move to AWS SDK v2, we can use the AWS SDK's implementation of this (depends on hadoop-aws upgrading their own AWS SDK dependency).
 */
public class GetCallerIdentityV4Presigner extends AWS4Signer implements STSGetCallerIdentityPresigner {
    protected static final String COMPATIBLE_ALGORITHM = "AWS4-HMAC-SHA256";
    protected static final String COMPATIBLE_TERMINATOR = "aws4_request";
    private static final String DEFAULT_ENCODING = "UTF-8";
//    private static final DateTimeFormatter compatibleTimeFormatter = DateTimeFormat
//            .forPattern("yyyyMMdd'T'HHmmss'Z'").withZoneUTC();
//    private static final DateTimeFormatter compatibleDateFormatter = DateTimeFormat
//            .forPattern("yyyyMMdd").withZoneUTC();
    GetCallerIdentityV4Presigner() {
        super(false);
    }

    /**
     * Regex which matches any of the sequences that we need to fix up after
     * URLEncoder.encode().
     */
    private static final Pattern ENCODED_CHARACTERS_PATTERN;
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

        ENCODED_CHARACTERS_PATTERN = Pattern.compile(pattern.toString());
    }
    /**
     * Encode a string for use in the path of a URL; uses URLEncoder.encode,
     * (which encodes a string for use in the query portion of a URL), then
     * applies some postfilters to fix things up per the RFC. Can optionally
     * handle strings which are meant to encode a path (ie include '/'es
     * which should NOT be escaped).
     *
     * @param value the value to encode
     * @param path true if the value is intended to represent a path
     * @return the encoded value
     */
    public static String urlEncode(final String value, final boolean path) {
        if (value == null) {
            return "";
        }

        try {
            String encoded = URLEncoder.encode(value, DEFAULT_ENCODING);

            Matcher matcher = ENCODED_CHARACTERS_PATTERN.matcher(encoded);
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
    /**
     * Append the given path to the given baseUri.
     *
     * <p>This method will encode the given path but not the given
     * baseUri.</p>
     *
     * @param baseUri The URI to append to (required, may be relative)
     * @param path The path to append (may be null or empty)
     * @param escapeDoubleSlash Whether double-slash in the path should be escaped to "/%2F"
     * @return The baseUri with the (encoded) path appended
     */
    public static String appendUri(final String baseUri, String path, final boolean escapeDoubleSlash ) {
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
    protected final String compatibleGetTimeStamp(long dateMilli) {
        //return compatibleTimeFormatter.print(dateMilli);

        // Convert milliseconds to Instant
        Instant instant = Instant.ofEpochMilli(dateMilli);

        // Format Instant to String in UTC time zone
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(ZoneId.of("UTC"));
        String formattedDateTime = formatter.format(instant);
        return formattedDateTime;
    }
    protected final String compatibleGetDateStamp(long dateMilli) {

        // Convert milliseconds to Instant
        Instant instant = Instant.ofEpochMilli(dateMilli);

        // Format Instant to String in UTC time zone
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
                .withZone(ZoneId.of("UTC"));
        String formattedDate = formatter.format(instant);

        return formattedDate;
    }
    protected final long compatibleGetDateFromRequest(Request<?> request) {
        int timeOffset = getTimeOffset(request);
        Date date = getSignatureDate(timeOffset);
        if (overriddenDate != null) date = overriddenDate;
        return date.getTime();
    }
    protected String compatibleExtractRegionName(URI endpoint) {
        if (regionName != null) return regionName;

        return AwsHostNameUtils.parseRegionName(endpoint.getHost(),
                serviceName);
    }
    protected String compatibleExtractServiceName(URI endpoint) {
        if (serviceName != null) return serviceName;

        // This should never actually be called, as we should always be setting
        // a service name on the signer; retain it for now in case anyone is
        // using the AWS4Signer directly and not setting a service name
        // explicitly.

        return AwsHostNameUtils.parseServiceName(endpoint);
    }
    protected String compatiobleGetScope(Request<?> request, String dateStamp) {
        String regionName = compatibleExtractRegionName(request.getEndpoint());
        String serviceName = compatibleExtractServiceName(request.getEndpoint());
        String scope =  dateStamp + "/" + regionName + "/" + serviceName + "/" + COMPATIBLE_TERMINATOR;
        return scope;
    }
    protected String compatibleGetCanonicalRequest(Request<?> request, String contentSha256) {
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
    protected String compatibleGetStringToSign(String algorithm, String dateTime, String scope, String canonicalRequest) {
        String stringToSign =
                algorithm + "\n" +
                        dateTime + "\n" +
                        scope + "\n" +
                        BinaryUtils.toHex(hash(canonicalRequest));
        log.debug("AWS4 String to Sign: '\"" + stringToSign + "\"");
        return stringToSign;
    }
    protected final byte[] compatibleComputeSignature(
            Request<?> request,
            String dateStamp,
            String timeStamp,
            String algorithm,
            String contentSha256,
            AWSCredentials sanitizedCredentials)
    {
        String regionName = compatibleExtractRegionName(request.getEndpoint());
        String serviceName = compatibleExtractServiceName(request.getEndpoint());
        String scope =  dateStamp + "/" + regionName + "/" + serviceName + "/" + COMPATIBLE_TERMINATOR;

        String stringToSign = compatibleGetStringToSign(algorithm, timeStamp, scope, compatibleGetCanonicalRequest(request,contentSha256 ));

        // AWS4 uses a series of derived keys, formed by hashing different
        // pieces of data
        byte[] kSecret = ("AWS4" + sanitizedCredentials.getAWSSecretKey()).getBytes();
        byte[] kDate = sign(dateStamp, kSecret, SigningAlgorithm.HmacSHA256);
        byte[] kRegion = sign(regionName, kDate, SigningAlgorithm.HmacSHA256);
        byte[] kService = sign(serviceName, kRegion, SigningAlgorithm.HmacSHA256);
        byte[] kSigning = sign(COMPATIBLE_TERMINATOR, kService, SigningAlgorithm.HmacSHA256);

        byte[] signature = sign(stringToSign.getBytes(), kSigning, SigningAlgorithm.HmacSHA256);
        return signature;
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

        final String timeStamp = compatibleGetTimeStamp(now);
        request.addParameter("X-Amz-Algorithm", COMPATIBLE_ALGORITHM);
        request.addParameter("X-Amz-Date", timeStamp);
        request.addParameter("X-Amz-SignedHeaders", getSignedHeadersString(request));
        request.addParameter("X-Amz-Expires", String.valueOf(input.getExpirationInSeconds()));

        // add X-Amz-Credential header (e.g <AccessKeyId>/<date>/<region>>/sts/aws4_request)

        long dateMilli = compatibleGetDateFromRequest(request);
        final String dateStamp = compatibleGetDateStamp(dateMilli);
        String scope = compatiobleGetScope(request, dateStamp);
        String signingCredentials = sanitizedCredentials.getAWSAccessKeyId() + "/" + scope;

        request.addParameter("X-Amz-Credential", signingCredentials);

        // contentSha256 is HashedPayload Hex(SHA256Hash("")) see https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html#create-string-to-sign
        byte[] contentDigest = this.hash(request.getContent());
        String contentSha256 = BinaryUtils.toHex(contentDigest);

        byte[] signature = compatibleComputeSignature(request, dateStamp, timeStamp, COMPATIBLE_ALGORITHM, contentSha256, sanitizedCredentials);

        request.addParameter("X-Amz-Signature", BinaryUtils.toHex(signature));

        return request;
    }
}