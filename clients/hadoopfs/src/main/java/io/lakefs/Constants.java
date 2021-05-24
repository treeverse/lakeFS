package io.lakefs;

import org.apache.commons.io.FileUtils;

class Constants {
    public static final String URI_SCHEME = "lakefs";
    public static final String FS_LAKEFS_ENDPOINT_KEY = "fs.lakefs.endpoint";
    public static final String FS_LAKEFS_ACCESS_KEY = "fs.lakefs.access.key";
    public static final String FS_LAKEFS_SECRET_KEY = "fs.lakefs.secret.key";
    public static final String FS_LAKEFS_LIST_AMOUNT_KEY = "fs.lakefs.list.amount";

    public static final int DEFAULT_LIST_AMOUNT = 1000;
    public static final String SEPARATOR = "/";

    public static final long DEFAULT_BLOCK_SIZE = 32 * FileUtils.ONE_MB;
}
