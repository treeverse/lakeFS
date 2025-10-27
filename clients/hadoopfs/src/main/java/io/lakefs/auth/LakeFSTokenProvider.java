package io.lakefs.auth;


public interface LakeFSTokenProvider {
    // get lakeFS JWT token to authenticate with lakeFS server
    String getToken();
    // refresh can be called by anyone to generate a new token for lakeF, not all implementations will support thus, it might result in a noop.
    void refresh();
}
