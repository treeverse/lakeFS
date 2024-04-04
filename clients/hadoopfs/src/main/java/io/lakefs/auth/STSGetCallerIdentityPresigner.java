package io.lakefs.auth;

import com.amazonaws.Request;
import com.amazonaws.services.s3.internal.ServiceUtils;


import java.io.IOException;
import java.net.URL;

public interface STSGetCallerIdentityPresigner {
    Request<GeneratePresignGetCallerIdentityRequest> presignRequest(GeneratePresignGetCallerIdentityRequest r) throws IOException;

    static URL convertRequestToUrl(Request<GeneratePresignGetCallerIdentityRequest> r) {
        return ServiceUtils.convertRequestToUrl(r, false);
    }
}
