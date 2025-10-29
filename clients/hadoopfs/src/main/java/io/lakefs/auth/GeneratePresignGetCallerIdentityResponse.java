package io.lakefs.auth;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class GeneratePresignGetCallerIdentityResponse {
    private Map<String, String> signedQueryParams;
    private GeneratePresignGetCallerIdentityRequest req;
    private Map<String, String> signedHeaders;
    private String signature;
    private String region;
    public GeneratePresignGetCallerIdentityResponse(GeneratePresignGetCallerIdentityRequest req, String region, Map<String, String> signedQueryParams, Map<String, String> signedHeaders, String signature) {
        this.signedQueryParams = signedQueryParams;
        this.req = req;
        this.signedHeaders = signedHeaders;
        this.signature = signature;
        this.region = region;
    }
    public Map<String, String> getSignedQueryParams() {
        return signedQueryParams;
    }
    public GeneratePresignGetCallerIdentityRequest getRequest() {
        return req;
    }
    public Map<String, String> getSignedHeaders() {
        return signedHeaders;
    }
    public String getSignature() {
        return signature;
    }
    public String getHTTPMethod(){
        return "POST";
    }
    public String getRegion() {
        return region;
    }
    public String getHost() {
        return req.getStsEndpoint().getHost();
    }
    public String getAction(){
        return signedQueryParams.get(STSGetCallerIdentityPresigner.AMZ_ACTION_PARAM_NAME);
    }
    public String getDate(){
        return signedQueryParams.get(STSGetCallerIdentityPresigner.AMZ_DATE_PARAM_NAME);
    }
    public String getExpires(){
        return signedQueryParams.get(STSGetCallerIdentityPresigner.AMZ_EXPIRES_PARAM_NAME);
    }
    public String getAccessKeyId(){
        return req.getCredentials().getAWSAccessKeyId();
    }
    public String getVersion(){
        return signedQueryParams.get(STSGetCallerIdentityPresigner.AMZ_VERSION_PARAM_NAME);
    }
    public String getAlgorithm(){
        return signedQueryParams.get(STSGetCallerIdentityPresigner.AMZ_ALGORITHM_PARAM_NAME);
    }
    public String getSecurityToken(){
        return signedQueryParams.get(STSGetCallerIdentityPresigner.AMZ_SECURITY_TOKEN_PARAM_NAME);
    }
    public String getSignedHeadersParam(){
        return signedQueryParams.get(STSGetCallerIdentityPresigner.AMZ_SIGNED_HEADERS_PARAM_NAME);
    }
    public String convertToURL(){
        Map<String, String> allQueryParams = new HashMap<>();
        allQueryParams.putAll(signedQueryParams);
        allQueryParams.put(STSGetCallerIdentityPresigner.AMZ_SIGNATURE_PARAM_NAME, getSignature());
        return URLBuilder(allQueryParams, req.getStsEndpoint());
    }
    public static String URLBuilder(Map<String, String> params, URI endpoint) {
        StringBuilder query = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value != null) {
                if (query.length() > 0) {
                    query.append("&");
                }
                try {

                    String encodedValue = URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
                    query.append(key).append("=").append(encodedValue);
                } catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        try {
            String url = String.format("%s?%s", endpoint, query.toString());
            return new URI(url).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
