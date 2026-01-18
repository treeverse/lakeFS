---
title: Login to lakeFS with AWS IAM Roles
description: This section covers how to authenticate to lakeFS using AWS IAM.
status: enterprise
search:
  boost: 2
---

# Authenticate to lakeFS with AWS IAM Roles

!!! info
    Available in **lakeFS Cloud** and **lakeFS Enterprise**<br/>
    If you're using the open-source version you can check the [pluggable APIs](./rbac.md#pluggable-authentication-and-authorization).

## Overview 

lakeFS supports authenticating users programmatically using AWS IAM roles instead of using static lakeFS access and secret keys.
The method enables you to bound IAM principal ARNs to lakeFS users.
A single lakeFS user may have many AWS principal ARNs attached to it. When a client is authenticated to a lakeFS server with an AWS session, the actions performed by the client are on behalf of the user attached to the ARN.

### Using Session Names

The bound ARN can be attached to a single lakeFS user with or without SessionName, serving different users.
For example, consider the following mapping: 

| Principal ARN                                       | lakeFS User |
|-----------------------------------------------------|-------------|
| `arn:aws:sts::123456:assumed-role/Dev`                | `foo`        |
| `arn:aws:sts::123456:assumed-role/Dev/john@acme.com`  | `john`        |

if the bound ARN were `arn:aws:sts::123456:assumed-role/Dev/<SessionName>` it would allow any principal assuming `Dev` role in AWS account `123456` to login to it.
If the `SessionName` is `john@acme.com` then lakeFS would return token for `john` user

### How AWS authentication works

The AWS STS API includes a method, `sts:GetCallerIdentity`, which allows you to validate the identity of a client. The client signs a GetCallerIdentity query using the AWS Signature v4 algorithm and sends it to the lakeFS server.

The `GetCallerIdentity` query consists of four pieces of information: the request URL, the request body, the request headers and the request method. The AWS signature is computed over those fields. The lakeFS server reconstructs the query using this information and forwards it on to the AWS STS service. Depending on the response from the STS service, the server authenticates the client.

Notably, clients don't need network-level access themselves to talk to the AWS STS API endpoint; they merely need access to the credentials to sign the request. However, it means that the lakeFS server does need network-level access to send requests to the STS endpoint.

Each signed AWS request includes the current timestamp to mitigate the risk of replay attacks. In addition, lakeFS allows you to require an additional header, `X-LakeFS-Server-ID` (added by default), to be present to mitigate against different types of replay attacks (such as a signed `GetCallerIdentity` request stolen from a dev lakeFS instance and used to authenticate to a prod lakeFS instance).

It's also important to note that Amazon does NOT appear to include any sort of authorization around calls to GetCallerIdentity. For example, if you have an IAM policy on your credential that requires all access to be MFA authenticated, non-MFA authenticated credentials will still be able to authenticate to lakeFS using this method.


## Server Configuration

!!! info
    lakeFS Helm chart supports the configuration below since version 1.5.0

To enable AWS IAM authentication in lakeFS Enterprise:

1. Enable external principals in lakeFS configuration
2. Configure external AWS authentication settings

**Helm Configuration (`values.yaml`):**

```yaml
ingress:
  enabled: true
  ingressClassName: <class-name>
  hosts:
    - host: <lakefs.ingress.domain>
      paths:
        - /

lakefsConfig: |
  auth:
    # Configure external AWS authentication
    external_aws_auth:
      enabled: true
      # the maximum age in seconds for the GetCallerIdentity request
      #get_caller_identity_max_age: 60
      # headers that must be present by the client when doing login request
      required_headers:
        # same host as the lakeFS server ingress
        X-LakeFS-Server-ID: <lakefs.ingress.domain>
```

!!! note
    By default, lakeFS clients will add the parameter `X-LakeFS-Server-ID: <lakefs.ingress.domain>` to the initial [login request][login-api] for STS.

**Direct Configuration File (`lakefs.yaml`):**

```yaml
auth:
  external_aws_auth:
    enabled: true
    # Optional: max age for GetCallerIdentity requests (default: 24h)
    get_caller_identity_max_age: 3600
    # Required headers for login requests
    required_headers:
      X-LakeFS-Server-ID: <lakefs.ingress.domain>
    # Optional headers that may be present
    optional_headers:
      X-Custom-Header: value
```

## Administration of IAM Roles in lakeFS

Administration refers to the management of the IAM roles that are allowed to authenticate to lakeFS.
Operations such as attaching and detaching IAM roles to a user, listing the roles attached to a user, and listing the users attached to a role.
This can be done through lakectl, the lakeFS [External Principals API][external-principal-admin], or generated clients.

### Using lakectl

```bash
# Attach IAM role(s) to a lakeFS user
lakectl auth users aws-iam attach [--id <lakefs-user>] \
    --principal-id 'arn:aws:sts::<id>:assumed-role/<role A>/<optional session name>'
lakectl auth users aws-iam attach [--id <lakefs-user>] \
    --principal-id 'arn:aws:sts::<id>:assumed-role/<role B>'

# Detach an IAM role from a user
lakectl auth users aws-iam detach [--id <lakefs-user>] \
    --principal-id 'arn:aws:sts::<id>:assumed-role/<role A>'

# List IAM roles attached to a user (omit --id to use current user)
lakectl auth users aws-iam list [--id <lakefs-user>]

# Lookup which user an IAM role is attached to
lakectl auth users aws-iam lookup --principal-id 'arn:aws:sts::<id>:assumed-role/<role>'
```

### Using Python SDK

```python
import lakefs_sdk as lakefs

configuration = lakefs.Configuration(host = "...",username="...",password="...")
username = "<lakefs-user>"
api = lakefs.ApiClient(configuration)
auth_api = lakefs.AuthApi(api)

# attach the role(s)to a lakeFS user
auth_api.create_user_external_principal(
    user_id=username, principal_id='arn:aws:sts::<id>:assumed-role/<role A>/<optional session name>')
auth_api.create_user_external_principal(
    user_id=username, principal_id='arn:aws:sts::<id>:assumed-role/<role B>')

# list the roles attached to the user
resp = auth_api.list_user_external_principals(user_id=username)
for p in resp.results:
    # do something
```


## Get lakeFS API Token

The login to lakeFS is done by calling the [login API][login-api] with the `GetCallerIdentity` request signed by the client.
Currently, the login operation is supported out of the box in:

- [lakectl](#login-with-lakectl)
- [lakeFS Hadoop FileSystem][lakefs-hadoopfs] version 0.2.4, see [Spark usage][lakefs-spark]
- [python](#login-with-python)
- [Everest mount](../reference/mount.md#authenticating-with-aws-iam-role.md)

For other use cases authenticated to lakeFS via login endpoint, this will require building the request input.

## Login with lakectl

### prerequisites

1. lakeFS must be configured to allow external principals to authenticate. The relevant IAM role should be attached to the appropriate lakeFS user. 
2. lakectl must be configured to use IAM authentication.

### lakectl configuration

To use IAM authentication, the following configuration fields are available:

- `credentials.provider.type` `(string: '')` - Settings this `aws_iam` will expect `aws_iam` block and try to use IAM.
- `credentials.provider.aws_iam.token_ttl_second` `(duration: 60m)` - Optional: lakeFS token duration.
- `credentials.provider.aws_iam.url_presign_ttl_seconds` `(duration: 15m)` - Optional: AWS STS's presigned URL validation duration.  
- `credentials.provider.aws_iam.refresh_interval` `(duration: 15m)` - Optional: Amount of time before token expiration that Everest will try to fetch a new session token instead of using the current one.  
- `credentials.provider.aws_iam.token_request_headers`: Map of required headers and their values to be signed by the AWS STS request as configured in your lakeFS server. If nothing is set the **default** behavior is adding `x-lakefs-server-id:<lakeFS host>`. If your lakeFS server doesn't require any headers (less secure) you can set this empty by setting `{}` empty map in your config. 

These configuration fields can be set via `.lakectl.yaml`: 

!!! example
    ```yaml
    credentials:
    provider:
        type: aws_iam          # Required
        aws_iam:
          token_ttl_seconds: 60m              # Optional, default: 1h
          url_presign_ttl_seconds: 15m        # Optional, default: 15m
          refresh_interval: 5m                # Optional, default: 5m
          token_request_headers:              # Optional, if omitted then will set x-lakefs-server-id: <lakeFS host> by default, to override default set to '{}'
          # x-lakefs-server-id: <lakeFS host>     Added by default if token_request_headers is not set	
          custome-key:  custome-val
    server:
    endpoint_url: <lakeFS endpoint url>
    ```

### Token Caching

To optimize lakectl's IAM authentication, a simple token caching mechanism was introduced.
Instead of performing a login request to AWS to get a JWT for lakeFS, lakectl will save the token to `$HOMEDIR/.lakectl/cache/lakectl_token_cache.json`.
The token will be used in the next lakectl operations up until one hour after writing. Later than that, lakectl will try and retrieve a new token.

!!! note
    Cache implementation does not support multiple AWS profiles. 
    When switching profiles and for general troubleshooting, try and delete the cache first.   

## Login with python

### prerequisites

1. lakeFS should be [configured](#server-configuration) to allow external principals to authenticate, and the used IAM role should be [attached](#administration-of-iam-roles-in-lakefs) to the relevant lakeFS user
2. The Python SDK requires additional packages to be installed to generate a lakeFS client with the assumed role.
To install the required packages, run the following command:

```shell
  pip install "lakefs[aws-iam]"
```

There are two ways in which external principals can be used to authenticate to lakeFS:

1. If no other authentication flow is provided, and the `credentials.provider.type` configuration is set to `aws_iam` in `.lakectl.yaml`, the client will use the machine's AWS role to authenticate with lakeFS:
    
    ```yaml
    credentials:
        provider:
        type: aws_iam
        aws_iam:
            token_ttl_seconds: 3600      # TTL for the temporary token (default: 3600)
            url_presign_ttl_seconds: 60  # TTL for presigned URLs (default: 60)
            token_request_headers:       # Optional headers for token requests
            HeaderName: HeaderValue
    ```
    Or using environment variables:
    ```bash
    export LAKECTL_CREDENTIALS_PROVIDER_TYPE="aws_iam"
    export LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_TTL_SECONDS="3600"
    export LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_PRESIGNED_URL_TTL_SECONDS="60"
    export LAKECTL_CREDENTIALS_PROVIDER_AWS_IAM_TOKEN_REQUEST_HEADERS='{"HeaderName":"HeaderValue"}'
    ```
    To use the client, merely `import lakefs` and use it as you would normally do:
    ```python
    import lakefs

    for branch in lakefs.repository("example-repo").branches():
    print(branch)
    ```

    !!! warning
        Please note, using the IAM provider configurations will not work with the lakectl command line tool, and will stop you from running it.


2. Generate a lakeFS client with the assumed role by initiating a boto3 session with the desired role and call `lakefs.client.frow_aws_role`:

    ```python
    import lakefs
    import boto3    

    session = boto3.Session()
    my_client = lakefs.client.from_aws_role(session=session, ttl_seconds=7200, host="<lakefs-host>")

    # list repositories
    repos = lakefs.repositories(client=my_client)
    for r in repos:
        print(r)
    ```

[external-principal-admin]:  ../reference/api.md#external
[login-api]: ../reference/api.md#auth/externalPrincipalLogin
[lakefs-hadoopfs]:  ../integrations/spark.md#lakefs-hadoop-filesystem
[lakefs-spark]:  ../integrations/spark.md#usage-with-temporaryawscredentialslakefstokenprovider