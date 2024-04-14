package io.lakefs;

import org.apache.commons.io.FileUtils;

public class Constants {
    public static final String DEFAULT_SCHEME = "lakefs";
    public static final String DEFAULT_CLIENT_ENDPOINT = "http://localhost:8000/api/v1";
    public static final String DEFAULT_AUTH_PROVIDER_SERVER_ID_HEADER = "X-Lakefs-Server-ID";
    public static final String ACCESS_KEY_KEY_SUFFIX = "access.key";
    public static final String SECRET_KEY_KEY_SUFFIX = "secret.key";
    public static final String ENDPOINT_KEY_SUFFIX = "endpoint";
    public static final String LIST_AMOUNT_KEY_SUFFIX = "list.amount";
    public static final String ACCESS_MODE_KEY_SUFFIX = "access.mode";
    // io.lakefs.auth.TemporaryAWSCredentialsLakeFSTokenProvider, io.lakefs.auth.InstanceProfileAWSCredentialsLakeFSTokenProvider
    public static final String LAKEFS_AUTH_PROVIDER_KEY_SUFFIX = "auth.provider";

    // TODO(isan) document all configuration fields before merge.
    public static final String LAKEFS_AUTH_TOKEN_TTL_KEY_SUFFIX = "token.duration_seconds";
    public static final String TOKEN_AWS_CREDENTIALS_PROVIDER_ACCESS_KEY_SUFFIX = "token.aws.access.key";
    public static final String TOKEN_AWS_CREDENTIALS_PROVIDER_SECRET_KEY_SUFFIX = "token.aws.secret.key";
    public static final String TOKEN_AWS_CREDENTIALS_PROVIDER_SESSION_TOKEN_KEY_SUFFIX = "token.aws.session.token";
    public static final String TOKEN_AWS_CREDENTIALS_PROVIDER_TOKEN_DURATION_SECONDS = "token.aws.sts.duration_seconds";
    public static final String TOKEN_AWS_CREDENTIALS_PROVIDER_ADDITIONAL_HEADERS = "token.sts.additional_headers";
    public static final String TOKEN_AWS_STS_ENDPOINT = "token.aws.sts.endpoint";

    public static final String SESSION_ID = "session_id";

    public static enum AccessMode {
        SIMPLE,
        PRESIGNED;
    }


    public static final int DEFAULT_LIST_AMOUNT = 1000;
    public static final String SEPARATOR = "/";

    public static final long DEFAULT_BLOCK_SIZE = 32 * FileUtils.ONE_MB;
}
