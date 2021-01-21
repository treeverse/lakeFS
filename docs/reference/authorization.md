---
layout: default
title: Authentication & Authorization
description: This section covers authorization (using AWS IAM) and Authentication of your lakeFS server. 
parent: Reference
nav_order: 4
has_children: false
---

# Authentication & Authorization
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Authentication

### API Server Authentication

Authenticating against the API server is done using a key-pair, passed via [Basic Access Authentication](https://en.wikipedia.org/wiki/Basic_access_authentication).

All HTTP requests must carry an `Authorization` header with the following structure:

```text
Authorization: Basic <base64 encoded access_key_id:access_secret_key>
```

For example, assuming my access_key_id is `my_access_key_id` and my secret_access_key is `my_secret_access_key`, we'd send the following header with every request:

```text
Authorization: Basic bXlfYWNjZXNzX2tleV9pZDpteV9hY2Nlc3Nfc2VjcmV0X2tleQ==
```


### S3 Gateway Authentication

To provide API compatibility with Amazon S3, authentication with the S3 Gateway supports both [SIGv2](https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html){:target="_blank"} and [SIGv4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html){:target="_blank"}.
Clients such as the AWS SDK that implement these authentication methods should work without modification.

See [this example for authenticating with the AWS CLI](../using/aws_cli.md).

## Authorization

### Authorization Model

Access to resources is managed very much like [AWS IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/intro-structure.html){:target="_blank"}.

There are 4 basic components to the system:

**Users** - Representing entities that access and use the system. A user is given one or more **Access Credentials** for authentication.

**Actions** - Representing a logical action within the system - reading a file, creating a repository, etc.

**Resources** - A unique identifier representing a specific resource in the system - a repository, an object, a user, etc.

**Policies** - Representing a set of **Actions**, a **Resource** and an effect: whether or not these actions are `Allowed` or `Denied` for the given resource(s).

**Groups** - A named collection of users. Users can belong to multiple groups.


Controlling access is done by attaching **Policies**, either directly to **Users**, or to **Groups** they belong to.

### Authorization process

Every action in the system, be it an API request, UI interaction, S3 Gateway call or CLI command, requires a set of actions to be allowed for one or more resources.

When a user makes a request to perform that action, the following process takes place:

1. Authentication - The credentials passed in the request are evaluated, and the user's identity is extracted.
2. Action permission resolution - lakeFS would then calculate the set of allowed actions and resources that this request requires.
3. Effective policy resolution - the user's policies (either attached directly or through group memberships) are calculated
4. Policy/Permission evaluation - lakeFS will compare the given user policies with the request actions and determine whether or not the request is allowed to continue

### Policy Precedence

Each policy attached to a user or a group has an `Effect` - either `Allow` or `Deny`.
During evaluation of a request, `Deny` would take precedence over any other `Allow` policy. 

This helps us compose policies together. For example, we could attach a very permissive policy to a user and use `Deny` rules to then selectively restrict what that user can do.


### Resource naming - ARNs

lakeFS uses [ARN identifier](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html#identifiers-arns){:target="_blank"} - very similar in structure to those used by AWS.
The resource segment of the ARN supports wildcards: use `*` to match 0 or more characters, or `?` to match exactly one character.

Additionally, the current user's ID is interpolated in runtime into the ARN using the `${user}` placeholder.

Here are a few examples of valid ARNs within lakeFS:

 ```text
arn:lakefs:auth:::user/jane.doe
arn:lakefs:auth:::user/*
arn:lakefs:fs:::repository/myrepo/*
arn:lakefs:fs:::repository/myrepo/object/foo/bar/baz
arn:lakefs:fs:::repository/myrepo/object/*
arn:lakefs:fs:::repository/*
arn:lakefs:fs:::*
```
this allows us to create fine-grained policies affecting only a specific subset of resources. 

See below for a full reference of ARNs and actions





### Actions and Permissions

For the full list of actions and their required permissions see the following table:

|Action name                    |required action         |Resource                                                                |API endpoint                                                                       |S3 gateway operation                                                 |
|-------------------------------|------------------------|------------------------------------------------------------------------|-----------------------------------------------------------------------------------|---------------------------------------------------------------------|
|List Repositories              |`fs:ListRepositories`   |`*`                                                                     |GET /repositories                                                                  |ListBuckets                                                          |
|Get Repository                 |`fs:ReadRepository`     |`arn:lakefs:fs:::repository/{repositoryId}`                             |GET /repositories/{repositoryId}                                                   |HeadBucket                                                           |
|Get Commit                     |`fs:ReadCommit`         |`arn:lakefs:fs:::repository/{repositoryId}`                             |GET /repositories/{repositoryId}/commits/{commitId}                                |-                                                                    |
|Create Commit                  |`fs:CreateCommit`       |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           |POST /repositories/{repositoryId}/branches/{branchId}/commits                      |-                                                                    |
|Get Commit log                 |`fs:ReadBranch`         |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           |GET /repositories/{repositoryId}/branches/{branchId}/commits                       |-                                                                    |
|Create Repository              |`fs:CreateRepository`   |`arn:lakefs:fs:::repository/{repositoryId}`                             |POST /repositories                                                                 |-                                                                    |
|Delete Repository              |`fs:DeleteRepository`   |`arn:lakefs:fs:::repository/{repositoryId}`                             |DELETE /repositories/{repositoryId}                                                |-                                                                    |
|List Branches                  |`fs:ListBranches`       |`arn:lakefs:fs:::repository/{repositoryId}`                             |GET /repositories/{repositoryId}/branches                                          |ListObjects/ListObjectsV2 (with delimiter = `/` and empty prefix)    |
|Get Branch                     |`fs:ReadBranch`         |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           |GET /repositories/{repositoryId}/branches/{branchId}                               |-                                                                    |
|Create Branch                  |`fs:CreateBranch`       |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           |POST /repositories/{repositoryId}/branches                                         |-                                                                    |
|Delete Branch                  |`fs:DeleteBranch`       |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           |DELETE /repositories/{repositoryId}/branches/{branchId}                            |-                                                                    |
|Merge branches                 |`fs:CreateCommit`       |`arn:lakefs:fs:::repository/{repositoryId}/branch/{destinationBranchId}`|POST /repositories/{repositoryId}/refs/{sourceBranchId}/merge/{destinationBranchId}|-                                                                    |
|Diff branch uncommitted changes|`fs:ListObjects`        |`arn:lakefs:fs:::repository/{repositoryId}`                             |GET /repositories/{repositoryId}/branches/{branchId}/diff                          |-                                                                    |
|Diff refs                      |`fs:ListObjects`        |`arn:lakefs:fs:::repository/{repositoryId}`                             |GET /repositories/{repositoryId}/refs/{leftRef}/diff/{rightRef}                    |-                                                                    |
|Stat object                    |`fs:ReadObject`         |`arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`          |GET /repositories/{repositoryId}/refs/{ref}/objects/stat                           |HeadObject                                                           |
|Get Object                     |`fs:ReadObject`         |`arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`          |GET /repositories/{repositoryId}/refs/{ref}/objects                                |GetObject                                                            |
|List Objects                   |`fs:ListObjects`        |`arn:lakefs:fs:::repository/{repositoryId}`                             |GET /repositories/{repositoryId}/refs/{ref}/objects/ls                             |ListObjects, ListObjectsV2 (no delimiter, or "/" + non-empty prefix) |
|Upload Object                  |`fs:WriteObject`        |`arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`          |POST /repositories/{repositoryId}/branches/{branchId}/objects                      |PutObject, CreateMultipartUpload, UploadPart, CompleteMultipartUpload|
|Delete Object                  |`fs:DeleteObject`       |`arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`          |DELETE /repositories/{repositoryId}/branches/{branchId}/objects                    |DeleteObject, DeleteObjects, AbortMultipartUpload                    |
|Revert Branch                  |`fs:RevertBranch`       |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           |PUT /repositories/{repositoryId}/branches/{branchId}                               |-                                                                    |
|Create User                    |`auth:CreateUser`       |`arn:lakefs:auth:::user/{userId}`                                       |POST /auth/users                                                                   |-                                                                    |
|List Users                     |`auth:ListUsers`        |`*`                                                                     |GET /auth/users                                                                    |-                                                                    |
|Get User                       |`auth:ReadUser`         |`arn:lakefs:auth:::user/{userId}`                                       |GET /auth/users/{userId}                                                           |-                                                                    |
|Delete User                    |`auth:DeleteUser`       |`arn:lakefs:auth:::user/{userId}`                                       |DELETE /auth/users/{userId}                                                        |-                                                                    |
|Get Group                      |`auth:ReadGroup`        |`arn:lakefs:auth:::group/{groupId}`                                     |GET /auth/groups/{groupId}                                                         |-                                                                    |
|List Groups                    |`auth:ListGroups`       |`*`                                                                     |GET /auth/groups                                                                   |-                                                                    |
|Create Group                   |`auth:CreateGroup`      |`arn:lakefs:auth:::group/{groupId}`                                     |POST /auth/groups                                                                  |-                                                                    |
|Delete Group                   |`auth:DeleteGroup`      |`arn:lakefs:auth:::group/{groupId}`                                     |DELETE /auth/groups/{groupId}                                                      |-                                                                    |
|List Policies                  |`auth:ListPolicies`     |`*`                                                                     |GET /auth/policies                                                                 |-                                                                    |
|Create Policy                  |`auth:CreatePolicy`     |`arn:lakefs:auth:::policy/{policyId}`                                   |POST /auth/policies                                                                |-                                                                    |
|Update Policy                  |`auth:UpdatePolicy`     |`arn:lakefs:auth:::policy/{policyId}`                                   |POST /auth/policies                                                                |-                                                                    |
|Delete Policy                  |`auth:DeletePolicy`     |`arn:lakefs:auth:::policy/{policyId}`                                   |DELETE /auth/policies/{policyId}                                                   |-                                                                    |
|Get Policy                     |`auth:ReadPolicy`       |`arn:lakefs:auth:::policy/{policyId}`                                   |GET /auth/policies/{policyId}                                                      |-                                                                    |
|List Group Members             |`auth:ReadGroup`        |`arn:lakefs:auth:::group/{groupId}`                                     |GET /auth/groups/{groupId}/members                                                 |-                                                                    |
|Add Group Member               |`auth:AddGroupMember`   |`arn:lakefs:auth:::group/{groupId}`                                     |PUT /auth/groups/{groupId}/members/{userId}                                        |-                                                                    |
|Remove Group Member            |`auth:RemoveGroupMember`|`arn:lakefs:auth:::group/{groupId}`                                     |DELETE /auth/groups/{groupId}/members/{userId}                                     |-                                                                    |
|List User Credentials          |`auth:ListCredentials`  |`arn:lakefs:auth:::user/{userId}`                                       |GET /auth/users/{userId}/credentials                                               |-                                                                    |
|Create User Credentials        |`auth:CreateCredentials`|`arn:lakefs:auth:::user/{userId}`                                       |POST /auth/users/{userId}/credentials                                              |-                                                                    |
|Delete User Credentials        |`auth:DeleteCredentials`|`arn:lakefs:auth:::user/{userId}`                                       |DELETE /auth/users/{userId}/credentials/{accessKeyId}                              |-                                                                    |
|Get User Credentials           |`auth:ReadCredentials`  |`arn:lakefs:auth:::user/{userId}`                                       |GET /auth/users/{userId}/credentials/{accessKeyId}                                 |-                                                                    |
|List User Groups               |`auth:ReadUser`         |`arn:lakefs:auth:::user/{userId}`                                       |GET /auth/users/{userId}/groups                                                    |-                                                                    |
|List User Policies             |`auth:ReadUser`         |`arn:lakefs:auth:::user/{userId}`                                       |GET /auth/users/{userId}/policies                                                  |-                                                                    |
|Attach Policy To User          |`auth:AttachPolicy`     |`arn:lakefs:auth:::user/{userId}`                                       |PUT /auth/users/{userId}/policies/{policyId}                                       |-                                                                    |
|Detach Policy From User        |`auth:DetachPolicy`     |`arn:lakefs:auth:::user/{userId}`                                       |DELETE /auth/users/{userId}/policies/{policyId}                                    |-                                                                    |
|List Group Policies            |`auth:ReadGroup`        |`arn:lakefs:auth:::group/{groupId}`                                     |GET /auth/groups/{groupId}/policies                                                |-                                                                    |
|Attach Policy To Group         |`auth:AttachPolicy`     |`arn:lakefs:auth:::group/{groupId}`                                     |PUT /auth/groups/{groupId}/policies/{policyId}                                     |-                                                                    |
|Detach Policy From Group       |`auth:DetachPolicy`     |`arn:lakefs:auth:::group/{groupId}`                                     |DELETE /auth/groups/{groupId}/policies/{policyId}                                  |-                                                                    |
|List Config                    |`auth:ReadConfig`       |`*`                                                                     |GET /config                                                                        |-                                                                    |
|Delete Objects                 |`fs:DeleteObjects`      |`arn:lakefs:fs:::repository/{repositoryId}`                             |                                                                                   |DeleteObjects                                                        |


### Preconfigured Policies

The following Policies are created during initial setup:

##### FSFullAccess

Policy:

```json
{
  "Action": [
    "fs:*"
  ],
  "Effect": "Allow",
  "Resource": "*"
}
```

##### FSReadAll

Policy:

```json
{
  "Action": [
    "fs:List*",
    "fs:Read*"
  ],
  "Effect": "Allow",
  "Resource": "*"
}
```

##### FSReadWriteAll

Policy:

```json
{
    "statement": [
        {
            "action": [
                "fs:ListRepositories",
                "fs:ReadRepository",
                "fs:ReadCommit",
                "fs:ListBranches",
                "fs:ListObjects",
                "fs:DeleteObjects",
                "fs:ReadObject",
                "fs:WriteObject",
                "fs:DeleteObject",
                "fs:RevertBranch",
                "fs:ReadBranch",
                "fs:CreateBranch",
                "fs:DeleteBranch",
                "fs:CreateCommit"
            ],
            "effect": "Allow",
            "resource": "*"
        }
    ]
}
```

##### AuthFullAccess

Policy:

```json
{
  "Action": [
    "auth:*"
  ],
  "Effect": "Allow",
  "Resource": "*"
}
```

##### AuthManageOwnCredentials

Policy:

```json
{
  "Action": [
    "auth:CreateCredentials",
    "auth:DeleteCredentials",
    "auth:ListCredentials",
    "auth:ReadCredentials"
  ],
  "Effect": "Allow",
  "Resource": "arn:lakefs:auth:::user/${user}"
}
```


### Preconfigured Groups

##### Admins

Policies: `["FSFullAccess", "AuthFullAccess"]`

##### SuperUsers

Policies: `["FSFullAccess", "AuthManageOwnCredentials"]`

##### Developers

Policies: `["FSReadWriteAll", "AuthManageOwnCredentials"]`
 
##### Viewers

Policies: `["FSReadAll", "AuthManageOwnCredentials"]`