package io.lakefs;

import org.apache.commons.io.FileUtils;

class Constants {
    public static final String FS_LAKEFS_LIST_AMOUNT_KEY_SUFFIX = "list.amount";

    public static final int DEFAULT_LIST_AMOUNT = 1000;
    public static final String SEPARATOR = "/";

    public static final long DEFAULT_BLOCK_SIZE = 32 * FileUtils.ONE_MB;
}
