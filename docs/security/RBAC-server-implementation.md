---
title: RBAC Server Implementation 
description: Instructions for implementing an RBAC server to manage permissions in lakeFS OSS.
parent: Security
---

# RBAC Server Implementation

{% include toc_2-4.html %}

## Overview

This guide explains how to implement an **RBAC (Role-Based Access Control) server** and configure **lakeFS OSS** to 
work with it.

Contents:
1. Required APIs for implementing an RBAC server.
2. How to configure lakeFS OSS to connect to your RBAC server.
3. How to run lakeFS OSS with your RBAC server.

{: .note}
> For a detailed explanation of the RBAC model in lakeFS, see [RBAC in lakeFS](./rbac.md).

## What is RBAC?

Role-Based Access Control (RBAC) in lakeFS manages permissions based on roles rather than individual users. 
Permissions are grouped into roles consisting of specific actions. Users are assigned roles aligned with their 
responsibilities, granting appropriate access without configuring permissions per user.

## Setting Up the RBAC Server

Follow these steps to implement an RBAC server compatible with lakeFS.

### 1. Implementation

To implement the RBAC server, you need to implement a subset of the APIs defined in the 
[authentication.yaml specification](./authorization-yaml.md).
Not all APIs in the specification are required — only those listed below, grouped into the following categories:

- **Credentials**
- **Users**
- **Groups**
- **Policies**

Implement all APIs under these categories.

{: .note}
> For detailed descriptions of the different schemas and each API, including their input and output parameters, 
> refer to each API in the [authentication.yaml specification](./authorization-yaml.md).

#### Credentials APIs

Implement the following endpoints under the `credentials` tag in the 
[authentication.yaml specification](./authorization-yaml.md):

- `GET /auth/users/{userId}/credentials`
- `POST /auth/users/{userId}/credentials`
- `DELETE /auth/users/{userId}/credentials/{accessKeyId}`
- `GET /auth/users/{userId}/credentials/{accessKeyId}`
- `GET /auth/credentials/{accessKeyId}`

#### Users APIs

Implement the following endpoints under the `users` tag in the
[authentication.yaml specification](./authorization-yaml.md):

- `GET /auth/users`
- `POST /auth/users`
- `GET /auth/users/{userId}`
- `DELETE /auth/users/{userId}`
- `PUT /auth/users/{userId}/friendly_name`
- `GET /auth/users/{userId}/groups`
- `GET /auth/users/{userId}/policies`
- `PUT /auth/users/{userId}/policies/{policyId}`
- `DELETE /auth/users/{userId}/policies/{policyId}`

#### Groups APIs

Implement the following endpoints under the `groups` tag:

- `GET /auth/groups`
- `POST /auth/groups`
- `GET /auth/groups/{groupId}`
- `DELETE /auth/groups/{groupId}`
- `GET /auth/groups/{groupId}/members`
- `PUT /auth/groups/{groupId}/members/{userId}`
- `DELETE /auth/groups/{groupId}/members/{userId}`
- `GET /auth/groups/{groupId}/policies`
- `PUT /auth/groups/{groupId}/policies/{policyId}`
- `DELETE /auth/groups/{groupId}/policies/{policyId}`

#### Policies APIs

Details about the expected structure of policies can be found [here](./rbac.md).
Implement the following endpoints under the `policies` tag:

- `GET /auth/policies`
- `POST /auth/policies`
- `GET /auth/policies/{policyId}`
- `PUT /auth/policies/{policyId}`
- `DELETE /auth/policies/{policyId}`

### 2. LakeFS Configuration

Update your lakeFS configuration file (`config.yaml`) to include:

```yaml
auth:
  encrypt:
    secret_key: "some_string"
  ui_config:
    rbac: internal
  api:
    endpoint: {ENDPOINT_TO_YOUR_RBAC_SERVER} # e.g., http://localhost:9006/api/v1
    token:
```

{: .note}
> The `auth.api.token` parameter is optional. If unspecified, lakeFS uses the `auth.encrypt.secret_key` as 
> the token. If specified, provide a JWT token directly or via the environment variable `LAKEFS_AUTH_API_TOKEN`.

### Setup Considerations

When lakeFS starts for the first time, it initializes users, groups, and policies. Once initialized, 
the authorization method cannot change. If lakeFS starts without an RBAC server and later tries connecting to one, 
it will fail to authenticate. You must re-initialize lakeFS from scratch to connect to a new RBAC server.

### 3. Running the Server

1. Run your RBAC server.
2. Run lakeFS with the updated configuration file:

```shell
./lakefs -c {PATH_TO_YOUR_CONFIG_FILE} run
```
