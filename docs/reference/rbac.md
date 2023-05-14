---
layout: default
title: Role-Based Access Control (RBAC)
description: This section covers authorization (using RBAC) of your lakeFS server.
parent: Reference
nav_order: 65
has_children: false
redirect_from:
  - /reference/authorization.html
---


# Role-Based Access Control (RBAC)
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

lakeFS Enterprise
{: .label .label-purple }
{: .no_toc }


{: .note}
> RBAC is available on [lakeFS Cloud](../cloud/) and [lakeFS Enterprise](../enterprise/).
>
> If you're using the open source version of lakeFS then the [ACL-based authorization mechanism](access-control-lists.html) is an alternative to RBAC.

{% include toc.html %}

## RBAC Model

Access to resources is managed very much like [AWS IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/intro-structure.html){:target="_blank"}.

There are five basic components to the system:

1. **Users** - Representing entities that access and use the system. A user is given one or more **Access Credentials** for authentication.

2. **Actions** - Representing a logical action within the system - reading a file, creating a repository, etc.

3. **Resources** - A unique identifier representing a specific resource in the system - a repository, an object, a user, etc.

4. **Policies** - Representing a set of **Actions**, a **Resource** and an effect: whether or not these actions are `allowed` or `denied` for the given resource(s).

5. **Groups** - A named collection of users. Users can belong to multiple groups.


Controlling access is done by attaching **Policies**, either directly to **Users**, or to **Groups** they belong to.

## Authorization process

Every action in the system - be it an API request, UI interaction, S3 Gateway call, or CLI command - requires a set of actions to be allowed for one or more resources.

When a user makes a request to perform that action, the following process takes place:

1. Authentication - the credentials passed in the request are evaluated and the user's identity is extracted.
1. Action permission resolution - lakeFS then calculates the set of allowed actions and resources that this request requires.
1. Effective policy resolution - the user's policies (either attached directly or through group memberships) are calculated.
1. Policy/Permission evaluation - lakeFS will compare the given user policies with the request actions and determine whether or not the request is allowed to continue.

## Policy Precedence

Each policy attached to a user or a group has an `Effect` - either `allow` or `deny`.
During evaluation of a request, `deny` would take precedence over any other `allow` policy.

This helps us compose policies together. For example, we could attach a very permissive policy to a user and use `deny` rules to then selectively restrict what that user can do.


## Resource naming - ARNs

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

See below for a full reference of ARNs and actions.


## Actions and Permissions

For the full list of actions and their required permissions see the following table:

| Action name                        | required action                             | Resource                                                               | API endpoint                                                                      | S3 gateway operation                                                |
|------------------------------------|---------------------------------------------|------------------------------------------------------------------------|-----------------------------------------------------------------------------------|---------------------------------------------------------------------|
| List Repositories                  | `fs:ListRepositories`                       |`*`                                                                     | GET /repositories                                                                 |ListBuckets                                                          |
| Get Repository                     | `fs:ReadRepository`                         |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repositoryId}                                                  |HeadBucket                                                           |
| Get Commit                         | `fs:ReadCommit`                             |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repositoryId}/commits/{commitId}                               |-                                                                    |
| Create Commit                      | `fs:CreateCommit`                           |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           | POST /repositories/{repositoryId}/branches/{branchId}/commits                     |-                                                                    |
| Get Commit log                     | `fs:ReadBranch`                             |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           | GET /repositories/{repositoryId}/branches/{branchId}/commits                      |-                                                                    |
| Create Repository                  | `fs:CreateRepository`                       |`arn:lakefs:fs:::repository/{repositoryId}`                             | POST /repositories                                                                |-                                                                    |
| Namespace Attach to Repository     | `fs:AttachStorageNamespace`                 |`arn:lakefs:fs:::namespace/{storageNamespace}`                          | POST /repositories                                                                |-                                                                    |
| Delete Repository                  | `fs:DeleteRepository`                       |`arn:lakefs:fs:::repository/{repositoryId}`                             | DELETE /repositories/{repositoryId}                                               |-                                                                    |
| List Branches                      | `fs:ListBranches`                           |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repositoryId}/branches                                         |ListObjects/ListObjectsV2 (with delimiter = `/` and empty prefix)    |
| Get Branch                         | `fs:ReadBranch`                             |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           | GET /repositories/{repositoryId}/branches/{branchId}                              |-                                                                    |
| Create Branch                      | `fs:CreateBranch`                           |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           | POST /repositories/{repositoryId}/branches                                        |-                                                                    |
| Delete Branch                      | `fs:DeleteBranch`                           |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           | DELETE /repositories/{repositoryId}/branches/{branchId}                           |-                                                                    |
| Merge branches                     | `fs:CreateCommit`                           |`arn:lakefs:fs:::repository/{repositoryId}/branch/{destinationBranchId}`| POST /repositories/{repositoryId}/refs/{sourceBranchId}/merge/{destinationBranchId} |-                                                                    |
| Diff branch uncommitted changes    | `fs:ListObjects`                            |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repositoryId}/branches/{branchId}/diff                         |-                                                                    |
| Diff refs                          | `fs:ListObjects`                            |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repositoryId}/refs/{leftRef}/diff/{rightRef}                   |-                                                                    |
| Stat object                        | `fs:ReadObject`                             |`arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`          | GET /repositories/{repositoryId}/refs/{ref}/objects/stat                          |HeadObject                                                           |
| Get Object                         | `fs:ReadObject`                             |`arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`          | GET /repositories/{repositoryId}/refs/{ref}/objects                               |GetObject                                                            |
| List Objects                       | `fs:ListObjects`                            |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repositoryId}/refs/{ref}/objects/ls                            |ListObjects, ListObjectsV2 (no delimiter, or "/" + non-empty prefix) |
| Upload Object                      | `fs:WriteObject`                            |`arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`          | POST /repositories/{repositoryId}/branches/{branchId}/objects                     |PutObject, CreateMultipartUpload, UploadPart, CompleteMultipartUpload|
| Delete Object                      | `fs:DeleteObject`                           |`arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`          | DELETE /repositories/{repositoryId}/branches/{branchId}/objects                   |DeleteObject, DeleteObjects, AbortMultipartUpload                    |
| Revert Branch                      | `fs:RevertBranch`                           |`arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`           | PUT /repositories/{repositoryId}/branches/{branchId}                              |-                                                                    |
| Get Branch Protection Rules        | `branches:GetBranchProtectionRules`         |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repository}/branch_protection                                  |-                                                                    |
| Set Branch Protection Rules        | `branches:SetBranchProtectionRules`         |`arn:lakefs:fs:::repository/{repositoryId}`                             | POST /repositories/{repository}/branch_protection                                 |-                                                                    |
| Delete Branch Protection Rules     | `branches:SetBranchProtectionRules`         |`arn:lakefs:fs:::repository/{repositoryId}`                             | DELETE /repositories/{repository}/branch_protection                               |-                                                                    |
| Create User                        | `auth:CreateUser`                           |`arn:lakefs:auth:::user/{userId}`                                       | POST /auth/users                                                                  |-                                                                    |
| List Users                         | `auth:ListUsers`                            |`*`                                                                     | GET /auth/users                                                                   |-                                                                    |
| Get User                           | `auth:ReadUser`                             |`arn:lakefs:auth:::user/{userId}`                                       | GET /auth/users/{userId}                                                          |-                                                                    |
| Delete User                        | `auth:DeleteUser`                           |`arn:lakefs:auth:::user/{userId}`                                       | DELETE /auth/users/{userId}                                                       |-                                                                    |
| Get Group                          | `auth:ReadGroup`                            |`arn:lakefs:auth:::group/{groupId}`                                     | GET /auth/groups/{groupId}                                                        |-                                                                    |
| List Groups                        | `auth:ListGroups`                           |`*`                                                                     | GET /auth/groups                                                                  |-                                                                    |
| Create Group                       | `auth:CreateGroup`                          |`arn:lakefs:auth:::group/{groupId}`                                     | POST /auth/groups                                                                 |-                                                                    |
| Delete Group                       | `auth:DeleteGroup`                          |`arn:lakefs:auth:::group/{groupId}`                                     | DELETE /auth/groups/{groupId}                                                     |-                                                                    |
| List Policies                      | `auth:ListPolicies`                         |`*`                                                                     | GET /auth/policies                                                                |-                                                                    |
| Create Policy                      | `auth:CreatePolicy`                         |`arn:lakefs:auth:::policy/{policyId}`                                   | POST /auth/policies                                                               |-                                                                    |
| Update Policy                      | `auth:UpdatePolicy`                         |`arn:lakefs:auth:::policy/{policyId}`                                   | POST /auth/policies                                                               |-                                                                    |
| Delete Policy                      | `auth:DeletePolicy`                         |`arn:lakefs:auth:::policy/{policyId}`                                   | DELETE /auth/policies/{policyId}                                                  |-                                                                    |
| Get Policy                         | `auth:ReadPolicy`                           |`arn:lakefs:auth:::policy/{policyId}`                                   | GET /auth/policies/{policyId}                                                     |-                                                                    |
| List Group Members                 | `auth:ReadGroup`                            |`arn:lakefs:auth:::group/{groupId}`                                     | GET /auth/groups/{groupId}/members                                                |-                                                                    |
| Add Group Member                   | `auth:AddGroupMember`                       |`arn:lakefs:auth:::group/{groupId}`                                     | PUT /auth/groups/{groupId}/members/{userId}                                       |-                                                                    |
| Remove Group Member                | `auth:RemoveGroupMember`                    |`arn:lakefs:auth:::group/{groupId}`                                     | DELETE /auth/groups/{groupId}/members/{userId}                                    |-                                                                    |
| List User Credentials              | `auth:ListCredentials`                      |`arn:lakefs:auth:::user/{userId}`                                       | GET /auth/users/{userId}/credentials                                              |-                                                                    |
| Create User Credentials            | `auth:CreateCredentials`                    |`arn:lakefs:auth:::user/{userId}`                                       | POST /auth/users/{userId}/credentials                                             |-                                                                    |
| Delete User Credentials            | `auth:DeleteCredentials`                    |`arn:lakefs:auth:::user/{userId}`                                       | DELETE /auth/users/{userId}/credentials/{accessKeyId}                             |-                                                                    |
| Get User Credentials               | `auth:ReadCredentials`                      |`arn:lakefs:auth:::user/{userId}`                                       | GET /auth/users/{userId}/credentials/{accessKeyId}                                |-                                                                    |
| List User Groups                   | `auth:ReadUser`                             |`arn:lakefs:auth:::user/{userId}`                                       | GET /auth/users/{userId}/groups                                                   |-                                                                    |
| List User Policies                 | `auth:ReadUser`                             |`arn:lakefs:auth:::user/{userId}`                                       | GET /auth/users/{userId}/policies                                                 |-                                                                    |
| Attach Policy To User              | `auth:AttachPolicy`                         |`arn:lakefs:auth:::user/{userId}`                                       | PUT /auth/users/{userId}/policies/{policyId}                                      |-                                                                    |
| Detach Policy From User            | `auth:DetachPolicy`                         |`arn:lakefs:auth:::user/{userId}`                                       | DELETE /auth/users/{userId}/policies/{policyId}                                   |-                                                                    |
| List Group Policies                | `auth:ReadGroup`                            |`arn:lakefs:auth:::group/{groupId}`                                     | GET /auth/groups/{groupId}/policies                                               |-                                                                    |
| Attach Policy To Group             | `auth:AttachPolicy`                         |`arn:lakefs:auth:::group/{groupId}`                                     | PUT /auth/groups/{groupId}/policies/{policyId}                                    |-                                                                    |
| Detach Policy From Group           | `auth:DetachPolicy`                         |`arn:lakefs:auth:::group/{groupId}`                                     | DELETE /auth/groups/{groupId}/policies/{policyId}                                 |-                                                                    |
| Read Storage Config                | `fs:ReadConfig`                             |`*`                                                                     | GET /config/storage                                                               |-                                                                    |
| Get Garbage Collection Rules       | `retention:GetGarbageCollectionRules`       |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repositoryId}/gc/rules                                         |-                                                                    |
| Set Garbage Collection Rules       | `retention:SetGarbageCollectionRules`       |`arn:lakefs:fs:::repository/{repositoryId}`                             | POST /repositories/{repositoryId}/gc/rules                                        |-                                                                    |
| Prepare Garbage Collection Commits | `retention:PrepareGarbageCollectionCommits` |`arn:lakefs:fs:::repository/{repositoryId}`                             | POST /repositories/{repositoryId}/gc/prepare_commits                              |-                                                                    |
| List Repository Action Runs        | `ci:ReadAction`                             |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repository}/actions/runs                                       |-                                                                    |
| Get Action Run                     | `ci:ReadAction`                             |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repository}/actions/runs/{run_id}                              |-                                                                    |
| List Action Run Hooks              | `ci:ReadAction`                             |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repository}/actions/runs/{run_id}/hooks                        |-                                                                    |
| Get Action Run Hook Output         | `ci:ReadAction`                             |`arn:lakefs:fs:::repository/{repositoryId}`                             | GET /repositories/{repository}/actions/runs/{run_id}/hooks/{hook_run_id}/output   |-                                                                    |

Some APIs may require more than one action.For instance, in order to
create a repository (`POST /repositories`), you need permission to
`fs:CreateRepository` for the _name_ of the repository and also
`fs:AttachStorageNamespace` for the _storage namespace_ used.

## Preconfigured Policies

The following Policies are created during initial setup:

### FSFullAccess

```json
{
  "statement": [
    {
      "action": [
        "fs:*"
      ],
      "effect": "allow",
      "resource": "*"
    }
  ]
}
```

### FSReadAll

```json
{
  "statement": [
    {
      "action": [
        "fs:List*",
        "fs:Read*"
      ],
      "effect": "allow",
      "resource": "*"
    }
  ]
}
```

### FSReadWriteAll

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
                "fs:ReadObject",
                "fs:WriteObject",
                "fs:DeleteObject",
                "fs:RevertBranch",
                "fs:ReadBranch",
                "fs:CreateBranch",
                "fs:DeleteBranch",
                "fs:CreateCommit"
            ],
            "effect": "allow",
            "resource": "*"
        }
    ]
}
```

### AuthFullAccess

```json
{
  "statement": [
    {
      "action": [
        "auth:*"
      ],
      "effect": "allow",
      "resource": "*"
    }
  ]
}
```

### AuthManageOwnCredentials

```json
{
  "statement": [
    {
      "action": [
        "auth:CreateCredentials",
        "auth:DeleteCredentials",
        "auth:ListCredentials",
        "auth:ReadCredentials"
      ],
      "effect": "allow",
      "resource": "arn:lakefs:auth:::user/${user}"
    }
  ]
}
```

### RepoManagementFullAccess

```json
{
    "statement": [
        {
            "action": [
                "ci:*"
            ],
            "effect": "allow",
            "resource": "*"
        },
        {
            "action": [
                "retention:*"
            ],
            "effect": "allow",
            "resource": "*"
        }
    ]
}
```
### RepoManagementReadAll

```json
{
    "statement": [
        {
            "action": [
                "ci:Read*"
            ],
            "effect": "allow",
            "resource": "*"
        },
        {
            "action": [
                "retention:Get*"
            ],
            "effect": "allow",
            "resource": "*"
        }
    ]
}
```

## Additional Policies

You can create additional policies to further limit user access. Use the web UI or the [lakectl auth](./cli.md#lakectl-auth-policies-create) command to create policies. Here is an example to define read/write access for a specific repository:

```json
{
    "statement": [
        {
            "action": [
                "fs:ReadRepository",
                "fs:ReadCommit",
                "fs:ListBranches",
                "fs:ListTags",
                "fs:ListObjects"
            ],
            "effect": "allow",
            "resource": "arn:lakefs:fs:::repository/<repository-name>"
        },
        {
            "action": [
                "fs:RevertBranch",
                "fs:ReadBranch",
                "fs:CreateBranch",
                "fs:DeleteBranch",
                "fs:CreateCommit"
            ],
            "effect": "allow",
            "resource": "arn:lakefs:fs:::repository/<repository-name>/branch/*"
        },
                {
            "action": [
                "fs:ListObjects",
                "fs:ReadObject",
                "fs:WriteObject",
                "fs:DeleteObject"
            ],
            "effect": "allow",
            "resource": "arn:lakefs:fs:::repository/<repository-name>/object/*"
        },
                {
            "action": [
                "fs:ReadTag",
                "fs:CreateTag",
                "fs:DeleteTag"
            ],
            "effect": "allow",
            "resource": "arn:lakefs:fs:::repository/<repository-name>/tag/*"
        },
        {
        	"action": ["fs:ReadConfig"],
        	"effect": "allow",
        	"resource": "*"
        }
    ]
}
```

## Preconfigured Groups

lakeFS has four preconfigured groups:

* Admins
* SuperUsers
* Developers
* Viewers

They have the following policies granted to them:

Policy                                                  | Admins | SuperUsers | Developers| Viewers |
--------------------------------------------------------|--------|------------|-----------|---------|
[`FSFullAccess`](#fsfullaccess)                         |   ✅   |     ✅     |           |         |
[`AuthFullAccess`](#authfullaccess)                     |   ✅   |            |           |         |
[`RepoManagementFullAccess`](#repomanagementfullaccess) |   ✅   |            |           |         |
[`AuthManageOwnCredentials`](#authmanageowncredentials) |        |     ✅     |    ✅     |   ✅    |
[`RepoManagementReadAll`](#repomanagementreadall)       |        |     ✅     |    ✅     |         |
[`FSReadWriteAll`](#fsreadwriteall)                     |        |            |    ✅     |         |
[`FSReadAll`](#fsreadall)                               |        |            |           |   ✅    |

## Pluggable Authentication and Authorization

Authorization and authentication is pluggable in lakeFS. If lakeFS is attached to a [remote authentication server](remote-authenticator.html) (or you are using lakeFS Cloud) then the [role-based access control](rbac.html) user interface can be used. If you are using RBAC with your self-managed lakeFS then the lakeFS configuration element `auth.ui_config.rbac` should be set to `external`. An enterprise (paid) solution of lakeFS should set `auth.ui_config.rbac` as `internal`.

If you are using self-managed lakeFS and do not have a [remote authentication server](remote-authenticator.html) then you should set `auth.ui_config.rbac` to `simplified` and refer to the [access control list](access-control-lists.html) documentation instead.
