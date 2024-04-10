package io.lakefs.auth;

import com.amazonaws.Request;
import com.amazonaws.auth.AWSSessionCredentials;
import io.lakefs.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GetCallerIdentityV4PresignerTest {
    public static AWSSessionCredentials newMockAWSCreds() {
        return new AWSSessionCredentials() {
            @Override
            public String getSessionToken() {
                return "sessionToken";
            }

            @Override
            public String getAWSAccessKeyId() {
                return "accessKeyId";
            }

            @Override
            public String getAWSSecretKey() {
                return "secretKey";
            }
        };
    }

    public static Map<String, String> getQueryParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (query != null) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] keyValue = pair.split("=");
                if (keyValue.length == 2) {
                    String key = keyValue[0];
                    String value = keyValue[1];
                    params.put(key, value);
                }
            }
        }
        return params;
    }

    @Test
    public void testPresignAsURL() throws Exception {
        AWSSessionCredentials awsCreds = GetCallerIdentityV4PresignerTest.newMockAWSCreds();
        GetCallerIdentityV4Presigner stsPresigner = new GetCallerIdentityV4Presigner();
        GeneratePresignGetCallerIdentityRequest stsReq = new GeneratePresignGetCallerIdentityRequest(
                new URI("https://sts.amazonaws.com"),
                awsCreds,
                new HashMap<String, String>() {{
                    put(Constants.DEFAULT_AUTH_PROVIDER_SERVER_ID_HEADER, "lakefs-host");
                }},
                60
        );

        GeneratePresignGetCallerIdentityResponse signedRequest = stsPresigner.presignRequest(stsReq);
        URL url = new URL(signedRequest.convertToURL());

        Assert.assertEquals("https", url.getProtocol());
        Assert.assertEquals("sts.amazonaws.com", url.getHost());
        Map<String, String> generatedQueryParams = GetCallerIdentityV4PresignerTest.getQueryParams(url.getQuery());

        Map<String, String> paramsExpected = new HashMap() {{
            put("X-Amz-Date", "\\d{8}T\\d{6}Z");
            put("Action", "GetCallerIdentity");
            put("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
            put("X-Amz-Signature", "[a-f0-9]{64}");
            put("Version", "2011-06-15");
            put("X-Amz-SignedHeaders", "host%3Bx-lakefs-server-id");
            put("X-Amz-Security-Token", GetCallerIdentityV4Presigner.urlEncode(awsCreds.getSessionToken(), false));
            put("X-Amz-Credential", awsCreds.getAWSAccessKeyId() + "%2F\\d{8}%2Fus-east-1%2Fsts%2Faws4_request");
            put("X-Amz-Expires", "60");
        }};

        // check that all expected params are present in the generated URL
        for (Map.Entry<String, String> entry : paramsExpected.entrySet()) {
            String expectedKey = entry.getKey();
            String expectedValuePattern = entry.getValue();
            Assert.assertEquals(String.format("missing param %s in URL %s", expectedKey, url), true, generatedQueryParams.containsKey(expectedKey));
            Pattern compiledPattern = Pattern.compile(expectedValuePattern);
            Matcher matcher = compiledPattern.matcher(generatedQueryParams.get(expectedKey));
            Assert.assertEquals(String.format("Query param %s does not match \npattern: %s", generatedQueryParams.get(expectedKey), expectedValuePattern), true, matcher.matches());
        }
    }
}