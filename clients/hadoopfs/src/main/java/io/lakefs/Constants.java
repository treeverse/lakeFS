package io.lakefs;

import org.apache.commons.io.FileUtils;

public class Constants {
    public static final String DEFAULT_SCHEME = "lakefs";
    public static final String DEFAULT_CLIENT_ENDPOINT = "http://localhost:8000/api/v1";
    public static final String ACCESS_KEY_KEY_SUFFIX = "access.key";
    public static final String SECRET_KEY_KEY_SUFFIX = "secret.key";
    public static final String ENDPOINT_KEY_SUFFIX = "endpoint";
    public static final String LIST_AMOUNT_KEY_SUFFIX = "list.amount";
    public static final String PRESIGNED_MODE_KEY_SUFFIX = "presigned.mode";


    public static final int DEFAULT_LIST_AMOUNT = 1000;
    public static final String SEPARATOR = "/";

    public static final long DEFAULT_BLOCK_SIZE = 32 * FileUtils.ONE_MB;
}
