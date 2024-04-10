package io.lakefs.auth;

import static com.amazonaws.util.StringUtils.UTF8;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.*;
import com.amazonaws.util.AwsHostNameUtils;
import com.amazonaws.util.BinaryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  * GetCallerIdentityV4Presigner generates a presigned URL for the GetCallerIdentity API, signed using SigV4.
 *  * This class extends AWS4Signer of AWS SDK version 1.7.4 and copies some functions from https://github.com/aws/aws-sdk-java/blob/1.7.4/src/main/java/com/amazonaws/auth/AWS4Signer.java
 *    * The copied functions exist in AWS SDK 1.7.4 but not AWS SDK 1.11.375, so they are
 *      not available on Hadoop AWS 3.
 */
public class GetCallerIdentityV4Presigner implements STSGetCallerIdentityPresigner {
    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final String TERMINATOR = "aws4_request";
    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final String SERVICE_NAME = "sts";
    private static final Logger LOG = LoggerFactory.getLogger(GetCallerIdentityV4Presigner.class);
    // source at https://github.com/aws/aws-sdk-java/blob/679abaebd371b09e887afaa5386dc182be4c6498/aws-java-sdk-core/src/main/java/com/amazonaws/util/SdkHttpUtils.java#L39
    /**
     * Regex which matches any of the sequences that we need to fix up after
     * URLEncoder.encode().
     */
    private static final Pattern ENCODED_CHARACTERS_PATTERN;

    static {
        StringBuilder pattern = new StringBuilder();

        pattern.append(Pattern.quote("+")).append("|").append(Pattern.quote("*")).append("|").append(Pattern.quote("%7E")).append("|").append(Pattern.quote("%2F"));

        ENCODED_CHARACTERS_PATTERN = Pattern.compile(pattern.toString());
    }

    public GetCallerIdentityV4Presigner() {
    }

    public byte[] sign(String stringData, byte[] key, SigningAlgorithm algorithm) throws AmazonClientException {
        try {
            byte[] data = stringData.getBytes(UTF8);
            return sign(data, key, algorithm);
        } catch (Exception e) {
            throw new AmazonClientException("Unable to calculate a request signature: " + e.getMessage(), e);
        }
    }

    protected byte[] sign(byte[] data, byte[] key, SigningAlgorithm algorithm) throws AmazonClientException {
        try {
            Mac mac = Mac.getInstance(algorithm.toString());
            mac.init(new SecretKeySpec(key, algorithm.toString()));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new AmazonClientException("Unable to calculate a request signature: " + e.getMessage(), e);
        }
    }

    /**
     * Hashes the string contents (assumed to be UTF-8) using the SHA-256
     * algorithm.
     *
     * @param text The string to hash.
     * @return The hashed bytes from the specified string.
     * @throws AmazonClientException If the hash cannot be computed.
     */
    public byte[] hash(String text) throws AmazonClientException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(text.getBytes(UTF8));
            return md.digest();
        } catch (Exception e) {
            throw new AmazonClientException("Unable to compute hash while signing request: " + e.getMessage(), e);
        }
    }

    /**
     * Examines the specified query string parameters and returns a
     * canonicalized form.
     * <p>
     * The canonicalized query string is formed by first sorting all the query
     * string parameters, then URI encoding both the key and value and then
     * joining them, in order, separating key value pairs with an '&'.
     *
     * @param parameters The query string parameters to be canonicalized.
     * @return A canonicalized form for the specified query string parameters.
     */
    protected String getCanonicalizedQueryString(Map<String, String> parameters) {

        SortedMap<String, String> sorted = new TreeMap<String, String>();

        Iterator<Map.Entry<String, String>> pairs = parameters.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            String key = pair.getKey();
            String value = pair.getValue();
            String encodedKey = urlEncode(key, false);
            String encodedValue = urlEncode(value, false);
            sorted.put(encodedKey, encodedValue);

        }

        StringBuilder builder = new StringBuilder();
        pairs = sorted.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            builder.append(pair.getKey());
            builder.append("=");
            builder.append(pair.getValue());
            if (pairs.hasNext()) {
                builder.append("&");
            }
        }

        return builder.toString();
    }

    /**
     * Loads the individual access key ID and secret key from the specified
     * credentials, ensuring that access to the credentials is synchronized on
     * the credentials object itself, and trimming any extra whitespace from the
     * credentials.
     * <p>
     * Returns either a {@link BasicSessionCredentials} or a
     * {@link BasicAWSCredentials} object, depending on the input type.
     *
     * @param credentials
     * @return A new credentials object with the sanitized credentials.
     */
    protected AWSCredentials sanitizeCredentials(AWSCredentials credentials) {
        String accessKeyId = null;
        String secretKey = null;
        String token = null;
        synchronized (credentials) {
            accessKeyId = credentials.getAWSAccessKeyId();
            secretKey = credentials.getAWSSecretKey();
            if (credentials instanceof AWSSessionCredentials) {
                token = ((AWSSessionCredentials) credentials).getSessionToken();
            }
        }
        if (secretKey != null) secretKey = secretKey.trim();
        if (accessKeyId != null) accessKeyId = accessKeyId.trim();
        if (token != null) token = token.trim();

        if (credentials instanceof AWSSessionCredentials) {
            return new BasicSessionCredentials(accessKeyId, secretKey, token);
        }

        return new BasicAWSCredentials(accessKeyId, secretKey);
    }

    protected Date getSignatureDate(int timeOffset) {
        Date dateValue = new Date();
        if (timeOffset != 0) {
            long epochMillis = dateValue.getTime();
            epochMillis -= timeOffset * 1000;
            dateValue = new Date(epochMillis);
        }
        return dateValue;
    }

    public static String getDateStamp(long dateMilli) {

        // Convert milliseconds to Instant
        Instant instant = Instant.ofEpochMilli(dateMilli);

        // Format Instant to String in UTC time zone
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC"));
        String formattedDate = formatter.format(instant);

        return formattedDate;
    }

    public static String getTimeStamp(long dateMilli) {
        // Convert milliseconds to Instant
        Instant instant = Instant.ofEpochMilli(dateMilli);

        // Format Instant to String in UTC time zone
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneId.of("UTC"));
        String formattedDateTime = formatter.format(instant);
        return formattedDateTime;
    }

    // source at https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java
    protected String getCanonicalizedHeaderString(Map<String, String> requestHeaders) {
        List<String> sortedHeaders = new ArrayList<>();
        sortedHeaders.addAll(requestHeaders.keySet());
        Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

        StringBuilder buffer = new StringBuilder();
        for (String header : sortedHeaders) {
            String key = header.toLowerCase().replaceAll("\\s+", " ");
            String value = requestHeaders.get(header);

            buffer.append(key).append(":");
            if (value != null) {
                buffer.append(value.replaceAll("\\s+", " "));
            }

            buffer.append("\n");
        }

        return buffer.toString();
    }

    // source at https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java
    protected String getSignedHeadersString(Map<String, String> headers) {
        List<String> sortedHeaders = new ArrayList<>();
        sortedHeaders.addAll(headers.keySet());
        Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

        StringBuilder buffer = new StringBuilder();
        for (String header : sortedHeaders) {
            if (buffer.length() > 0) buffer.append(";");
            buffer.append(header.toLowerCase());
        }

        return buffer.toString();
    }
    // source at https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L241
    protected String getCanonicalRequest(String httpMethod, Map<String, String> parameters, Map<String, String> headers, String contentSha256) {
        final String path = "/";
        String canonicalRequest = httpMethod + "\n" + path + "\n" + getCanonicalizedQueryString(parameters) + "\n" + getCanonicalizedHeaderString(headers) + "\n" + getSignedHeadersString(headers) + "\n" + contentSha256;

        LOG.debug("AWS4 Canonical Request: '{}'", canonicalRequest);
        return canonicalRequest;
    }

    // source at https://github.com/aws/aws-sdk-java/blob/d1790c78af50488f38d758fd1f654d035b505150/src/main/java/com/amazonaws/auth/AWS4Signer.java#L257C22-L257C37
    protected String getStringToSign(String algorithm, String dateTime, String scope, String canonicalRequest) {
        String stringToSign = algorithm + "\n" + dateTime + "\n" + scope + "\n" + BinaryUtils.toHex(hash(canonicalRequest));
        LOG.debug("AWS4 String to Sign: '{}'", stringToSign);
        return stringToSign;
    }
    // source at https://github.com/aws/aws-sdk-java/blob/679abaebd371b09e887afaa5386dc182be4c6498/aws-java-sdk-core/src/main/java/com/amazonaws/util/SdkHttpUtils.java#L66

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
     * Returns true if the specified URI is using a non-standard port (i.e. any
     * port other than 80 for HTTP URIs or any port other than 443 for HTTPS
     * URIs).
     *
     * @param uri
     * @return True if the specified URI is using a non-standard port, otherwise
     * false.
     */
    public static boolean isUsingNonDefaultPort(URI uri) {
        String scheme = uri.getScheme().toLowerCase();
        int port = uri.getPort();

        if (port <= 0) return false;
        if (scheme.equals("http") && port == 80) return false;
        if (scheme.equals("https") && port == 443) return false;

        return true;
    }

    // cloned original addHostHeader in AWS4Signer
    protected String getHostHeader(URI endpoint) {
        StringBuilder hostHeaderBuilder = new StringBuilder(endpoint.getHost());
        if (isUsingNonDefaultPort(endpoint)) {
            hostHeaderBuilder.append(":").append(endpoint.getPort());
        }
        return hostHeaderBuilder.toString();
    }

    public GeneratePresignGetCallerIdentityResponse presignRequest(GeneratePresignGetCallerIdentityRequest input) {

        AWSCredentials sanitizedCredentials = sanitizeCredentials(input.getCredentials());
        String region = AwsHostNameUtils.parseRegionName(input.getStsEndpoint());
        // TODO(isan) use global sdk commented out here
        int timeOffset = 0;
        Date date = getSignatureDate(timeOffset);
        long dateMilli = date.getTime();
        String dateStamp = getDateStamp(dateMilli);
        String timeStamp = getTimeStamp(System.currentTimeMillis());
        String scope = dateStamp + "/" + region + "/" + SERVICE_NAME + "/" + TERMINATOR;
        String signingCredentials = sanitizedCredentials.getAWSAccessKeyId() + "/" + scope;

        ///////////// compute signature  /////////////

        Map<String, String> headersToSign = new HashMap<String, String>() {{
            put("Host", getHostHeader(input.getStsEndpoint()));
        }};
        // add additional headers to sign (i.e X-lakeFS-Server-ID)
        for (Map.Entry<String, String> entry : input.getAdditionalHeaders().entrySet()) {
            headersToSign.put(entry.getKey(), entry.getValue());
        }

        Map<String, String> queryParamsToSign = new HashMap<String, String>() {{
            put(STSGetCallerIdentityPresigner.AMZ_ACTION_PARAM_NAME, "GetCallerIdentity");
            put(STSGetCallerIdentityPresigner.AMZ_VERSION_PARAM_NAME, "2011-06-15");
            put(STSGetCallerIdentityPresigner.AMZ_SECURITY_TOKEN_PARAM_NAME, ((AWSSessionCredentials) sanitizedCredentials).getSessionToken());
            put(STSGetCallerIdentityPresigner.AMZ_ALGORITHM_PARAM_NAME, ALGORITHM);
            put(STSGetCallerIdentityPresigner.AMZ_DATE_PARAM_NAME, timeStamp);
            put(STSGetCallerIdentityPresigner.AMZ_SIGNED_HEADERS_PARAM_NAME, getSignedHeadersString(headersToSign));
            put(STSGetCallerIdentityPresigner.AMZ_EXPIRES_PARAM_NAME, String.valueOf(input.getExpirationInSeconds()));
            put(STSGetCallerIdentityPresigner.AMZ_CREDENTIAL_PARAM_NAME, signingCredentials);
        }};

        // contentSha256 is HashedPayload Hex(SHA256Hash(""))
        // see https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html#create-string-to-sign
        byte[] contentDigest = hash("");
        String contentSha256 = BinaryUtils.toHex(contentDigest);
        String canonicalRequest = getCanonicalRequest("POST", queryParamsToSign, headersToSign, contentSha256);

        String stringToSign = getStringToSign(ALGORITHM, timeStamp, scope, canonicalRequest);

        byte[] signature = computeSignature(sanitizedCredentials, dateStamp, region, SERVICE_NAME, TERMINATOR, stringToSign);

        GeneratePresignGetCallerIdentityResponse result = new GeneratePresignGetCallerIdentityResponse(input,region, queryParamsToSign, headersToSign, BinaryUtils.toHex(signature));
        return result;
    }

    public byte[] computeSignature(AWSCredentials sanitizedCredentials, String dateStamp, String region, String SERVICE_NAME, String TERMINATOR, String stringToSign) {
        // AWS4 uses a series of derived keys, formed by hashing different
        // pieces of data
        byte[] kSecret = ("AWS4" + sanitizedCredentials.getAWSSecretKey()).getBytes();
        byte[] kDate = sign(dateStamp, kSecret, SigningAlgorithm.HmacSHA256);
        byte[] kRegion = sign(region, kDate, SigningAlgorithm.HmacSHA256);
        byte[] kService = sign(SERVICE_NAME, kRegion, SigningAlgorithm.HmacSHA256);
        byte[] kSigning = sign(TERMINATOR, kService, SigningAlgorithm.HmacSHA256);

        byte[] signature = sign(stringToSign.getBytes(), kSigning, SigningAlgorithm.HmacSHA256);
        return signature;
    }
}
