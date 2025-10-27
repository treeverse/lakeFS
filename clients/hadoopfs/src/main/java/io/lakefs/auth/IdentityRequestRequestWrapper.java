package io.lakefs.auth;

import com.google.gson.annotations.SerializedName;

public class IdentityRequestRequestWrapper {
    @SerializedName("identity_token")
    private String identityToken;
    public IdentityRequestRequestWrapper(String identityToken) {
        this.identityToken = identityToken;
    }
}
