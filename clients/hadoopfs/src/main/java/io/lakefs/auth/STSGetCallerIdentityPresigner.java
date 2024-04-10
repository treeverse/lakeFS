package io.lakefs.auth;

public interface STSGetCallerIdentityPresigner {
    String AMZ_DATE_PARAM_NAME = "X-Amz-Date";
    String AMZ_SECURITY_TOKEN_PARAM_NAME = "X-Amz-Security-Token";
    String AMZ_ALGORITHM_PARAM_NAME = "X-Amz-Algorithm";
    String AMZ_CREDENTIAL_PARAM_NAME = "X-Amz-Credential";
    String AMZ_SIGNED_HEADERS_PARAM_NAME = "X-Amz-SignedHeaders";
    String AMZ_SIGNATURE_PARAM_NAME = "X-Amz-Signature";
    String AMZ_EXPIRES_PARAM_NAME = "X-Amz-Expires";
    String AMZ_ACTION_PARAM_NAME = "Action";
    String AMZ_VERSION_PARAM_NAME = "Version";
    GeneratePresignGetCallerIdentityResponse presignRequest(GeneratePresignGetCallerIdentityRequest input);
}
