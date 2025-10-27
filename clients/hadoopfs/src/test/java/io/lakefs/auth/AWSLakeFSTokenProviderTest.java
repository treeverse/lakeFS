package io.lakefs.auth;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.lakefs.Constants;
import io.lakefs.FSConfiguration;
import io.lakefs.clients.sdk.model.AuthenticationToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.matchers.Times;
import org.mockserver.model.Cookie;
import org.mockserver.model.HttpRequest;

import static org.mockserver.model.HttpResponse.response;


public class AWSLakeFSTokenProviderTest {
    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);
    protected MockServerClient mockServerClient;
    protected final Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

    @Test
    public void testProviderIdentityTokenSerde() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("fs.lakefs." + Constants.LAKEFS_AUTH_PROVIDER_KEY_SUFFIX, TemporaryAWSCredentialsLakeFSTokenProvider.NAME);
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_ACCESS_KEY_SUFFIX, "accessKeyId");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SECRET_KEY_SUFFIX, "secretAccessKey");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SESSION_TOKEN_KEY_SUFFIX, "sessionToken");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_STS_ENDPOINT, "https://sts.amazonaws.com");

        AWSLakeFSTokenProvider provider = (AWSLakeFSTokenProvider) LakeFSTokenProviderFactory.newLakeFSTokenProvider(Constants.DEFAULT_SCHEME, conf);
        String identityToken = provider.newPresignedGetCallerIdentityToken();
        String decodedToken = new String(Base64.decodeBase64(identityToken.getBytes()));
        LakeFSExternalPrincipalIdentityRequest request = LakeFSExternalPrincipalIdentityRequest.fromJSON(decodedToken);
        Assert.assertEquals("POST", request.getMethod());
        Assert.assertEquals("sts.amazonaws.com", request.getHost());
        Assert.assertEquals("us-east-1", request.getRegion());
        Assert.assertEquals("GetCallerIdentity", request.getAction());
        Assert.assertTrue(request.getDate().matches("\\d{8}T\\d{6}Z"));
        Assert.assertEquals("60", request.getExpirationDuration());
        Assert.assertEquals(FSConfiguration.get(conf, "lakefs", Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_ACCESS_KEY_SUFFIX), request.getAccessKeyId());
        Assert.assertTrue(request.getSignature().matches("[0-9a-fA-F]{64}"));
        Assert.assertEquals("host", request.getSignedHeaders().get(0));
        Assert.assertEquals("x-lakefs-server-id", request.getSignedHeaders().get(1));
        Assert.assertEquals("2011-06-15", request.getVersion());
        Assert.assertEquals("AWS4-HMAC-SHA256", request.getAlgorithm());
        Assert.assertEquals(FSConfiguration.get(conf, "lakefs", Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SESSION_TOKEN_KEY_SUFFIX), request.getSecurityToken());
    }

    protected void mockExternalPrincipalLogin(Long tokenExpiration, String token, String sessionID) {
        // lakeFSFS initialization requires a blockstore.
        HttpRequest request = HttpRequest.request().withCookie(new Cookie("sessionId", sessionID));

        mockServerClient
                .when(
                        request.withMethod("POST").withPath("/auth/external/principal/login"),
                        Times.once())
                .respond(
                        response().withStatusCode(200).withBody(new AuthenticationToken().token(token).tokenExpiration(tokenExpiration).toJson())
                );
    }

    @Test
    public void testProviderToken() throws Exception {
        String sessionID = "testProviderToken";
        String expectedToken = "lakefs-jwt-token";
        Configuration conf = new Configuration(false);
        conf.set("fs.lakefs." + Constants.LAKEFS_AUTH_PROVIDER_KEY_SUFFIX, TemporaryAWSCredentialsLakeFSTokenProvider.NAME);
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_ACCESS_KEY_SUFFIX, "accessKeyId");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SECRET_KEY_SUFFIX, "secretAccessKey");
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_CREDENTIALS_PROVIDER_SESSION_TOKEN_KEY_SUFFIX, "sessionToken");
        conf.setInt("fs.lakefs." + Constants.LAKEFS_AUTH_TOKEN_TTL_KEY_SUFFIX, 120);
        conf.set("fs.lakefs." + Constants.TOKEN_AWS_STS_ENDPOINT, "https://sts.amazonaws.com");
        conf.set("fs.lakefs.endpoint", String.format("http://localhost:%d/", mockServerClient.getPort()));
        conf.set("fs.lakefs.session_id", sessionID);

        LakeFSTokenProvider provider = LakeFSTokenProviderFactory.newLakeFSTokenProvider(Constants.DEFAULT_SCHEME, conf);
        mockExternalPrincipalLogin(1000L, expectedToken, sessionID);

        String lakeFSJWT = provider.getToken();
        Assert.assertEquals(expectedToken, lakeFSJWT);
    }
}
