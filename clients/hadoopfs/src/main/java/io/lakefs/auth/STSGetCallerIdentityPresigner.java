package io.lakefs.auth;

import com.amazonaws.Request;
import com.amazonaws.services.s3.internal.ServiceUtils;


import java.io.IOException;
import java.net.URL;

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

    Request<GeneratePresignGetCallerIdentityRequest> presignRequest(GeneratePresignGetCallerIdentityRequest r) throws IOException;
    String getSignedRegion(Request<GeneratePresignGetCallerIdentityRequest> r);
    static URL convertRequestToUrl(Request<GeneratePresignGetCallerIdentityRequest> r) {
        return ServiceUtils.convertRequestToUrl(r, false);
    }
}
