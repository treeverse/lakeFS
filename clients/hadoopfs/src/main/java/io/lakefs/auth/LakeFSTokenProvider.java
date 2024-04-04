package io.lakefs.auth;


public interface LakeFSTokenProvider {
    String getToken();

    void refresh();
}
