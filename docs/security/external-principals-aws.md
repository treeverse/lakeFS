---
title: Login to lakeFS with AWS IAM Roles
description: This section covers how to authenticate to lakeFS using AWS IAM.
parent: Security
redirect_from:
  - /reference/external-principals-aws.html
---

# Authenticate to lakeFS with AWS IAM Roles
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

lakeFS Enterprise
{: .label .label-purple }

{: .note}
> External principals API is available for lakeFS Enterprise. If you're using the open-source version you can check the [pluggable APIs](https://docs.lakefs.io/security/rbac.html#pluggable-authentication-and-authorization).

{% include toc.html %}

## Overview 

lakeFS supports authenticating users programmatically using AWS IAM roles instead of using static lakeFS access and secret keys.
The method enables you to bound IAM principal ARNs to lakeFS users.
A single lakeFS user may have many AWS's principle ARNs attached to it. When a client is authenticating to a lakeFS server with an AWS's session, the actions performed by the client are on behalf of the user attached to the ARN.

### Using Session Names

The bound ARN can be attached to a single lakeFS user with or without SessionName, serving different users.
For example, consider the following mapping: 

| Principal ARN                                       | lakeFS User |
|-----------------------------------------------------|-------------|
| arn:aws:sts::123456:assumed-role/Dev                | foo         |
| arn:aws:sts::123456:assumed-role/Dev/john@acme.com  | john        |

if the bound ARN were `arn:aws:sts::123456:assumed-role/Dev/<SessionName>` it would allow any principal assuming `Dev` role in AWS account `123456` to login to it.
If the `SessionName` is `john@acme.com` then lakeFS would return token for `john` user

### How AWS authentication works

The AWS STS API includes a method, `sts:GetCallerIdentity`, which allows you to validate the identity of a client. The client signs a GetCallerIdentity query using the AWS Signature v4 algorithm and sends it to the lakeFS server. 

The `GetCallerIdentity` query consists of four pieces of information: the request URL, the request body, the request headers and the request method. The AWS signature is computed over those fields. The lakeFS server reconstructs the query using this information and forwards it on to the AWS STS service. Depending on the response from the STS service, the server authenticates the client.

Notably, clients don't need network-level access themselves to talk to the AWS STS API endpoint; they merely need access to the credentials to sign the request. However, it means that the lakeFS server does need network-level access to send requests to the STS endpoint.

Each signed AWS request includes the current timestamp to mitigate the risk of replay attacks. In addition, lakeFS allows you to require an additional header, `X-LakeFS-Server-ID` (added by default), to be present to mitigate against different types of replay attacks (such as a signed `GetCallerIdentity` request stolen from a dev lakeFS instance and used to authenticate to a prod lakeFS instance). 

It's also important to note that Amazon does NOT appear to include any sort of authorization around calls to GetCallerIdentity. For example, if you have an IAM policy on your credential that requires all access to be MFA authenticated, non-MFA authenticated credentials will still be able to authenticate to lakeFS using this method.


## Server Configuration

{: .note}
> Note: lakeFS Helm chart supports the configuration since version `1.2.11` - see usage [values.yaml example](https://github.com/treeverse/charts/blob/master/examples/lakefs/enterprise/values-external-aws.yaml).

* in lakeFS `auth.authentication_api.external_principals_enabled` must be set to `true` in the configuration file, other configuration (`auth.authentication_api.*`) can be found at [configuration reference]({% link reference/configuration.md %})

For the full list of the Fluffy server configuration, see [Fluffy Configuration][fluffy-configuration] under `auth.external.aws_auth`


{: .note}
> By default, lakeFS clients will add the parameter `X-LakeFS-Server-ID: <lakefs.ingress.domain>` to the initial [login request][login-api] for STS.


**Example configuration with required headers:**

Configuration for `lakefs.yaml`: 

```yaml
auth:
  authentication_api:
    endpoint: http://<fluffy-sso>/api/v1
    external_principals_enabled: true
  api:
    endpoint: http://<fluffy-rbac>/api/v1
```

Configuration for `fluffy.yaml`:

```yaml
# fluffy address for lakefs auth.authentication_api.endpoint
# used by lakeFS to log in and get the token
listen_address: <fluffy-sso>
auth:
  # fluffy address for lakeFS auth.api.endpoint 
  # used by lakeFS to manage the lifecycle attach/detach of the external principals
  serve_listen_address: <fluffy-rbac>
  external:
    aws_auth:
      enabled: true
      # headers that must be present by the client when doing login request
      required_headers:
        # same host as the lakeFS server ingress
        X-LakeFS-Server-ID: <lakefs.ingress.domain>
```

## Administration of IAM Roles in lakeFS

Administration refers to the management of the IAM roles that are allowed to authenticate to lakeFS.
Operations such as attaching and detaching IAM roles to a user, listing the roles attached to a user, and listing the users attached to a role. 
Currently, this is done through the lakeFS [External Principals API][external-principal-admin] and generated clients.

Example of attaching an IAM roles to a user:

```python
import lakefs_sdk as lakefs  

configuration = lakefs.Configuration(host = "...",username="...",password="...")
username = "<lakefs-user>"
api = lakefs.ApiClient(configuration)
auth_api = lakefs.AuthApi(api)
# attach the role(s)to a lakeFS user
auth_api.create_user_external_principal(user_id=username, principal_id='arn:aws:sts::<id>:assumed-role/<role A>/<optional session name>')
auth_api.create_user_external_principal(user_id=username, principal_id='arn:aws:sts::<id>:assumed-role/<role B>')
# list the roles attached to the user
resp = auth_api.list_user_external_principals(user_id=username)
for p in resp.results:
    # do something
```


## Get lakeFS API Token

The login to lakeFS is done by calling the [login API][login-api] with the `GetCallerIdentity` request signed by the client.
Currently, the login operation is supported out of the box in:
- [lakeFS Hadoop FileSystem][lakefs-hadoopfs] version 0.2.4, see [Spark usage][lakefs-spark]
- [python](#login-with-python)

For other use cases authenticate to lakeFS via login endpoint, this will require building the request input.

## Login with python

### prerequisites

1. lakeFS should be [configured](#server-configuration) to allow external principals to authenticate and the used IAM role should be [attached](#administration-of-iam-roles-in-lakefs) to the relevant lakeFS user
2. The Python SDK requires additional packages to be installed in order to generate a lakeFS client with the assumed role.
To install the required packages, run the following command:

```sh
  pip install lakefs[aws-iam]
```

In order to generate a lakeFS client with the assumed role, initiate a boto3 session with the desired role and call `lakefs.client.frow_aws_role`:


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


[external-principal-admin]:  {% link reference/cli.md %}#external
[login-api]: {% link reference/api.md %}#auth/externalPrincipalLogin
[lakefs-hadoopfs]:  {% link integrations/spark.md %}#lakefs-hadoop-filesystem
[lakefs-spark]:  {% link integrations/spark.md %}#usage-with-temporaryawscredentialslakefstokenprovider
[fluffy-configuration]: {% link enterprise/configuration.md %}#fluffy-server-configuration
