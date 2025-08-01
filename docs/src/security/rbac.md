---
title: Role-Based Access Control (RBAC)
description: This section covers authorization (using RBAC) of your lakeFS server.
status: enterprise
search:
  boost: 2
---


# Role-Based Access Control (RBAC)

!!! info
    Available in **lakeFS Cloud** and **lakeFS Enterprise**


<iframe width="420" height="315" src="https://www.youtube.com/embed/Ekjv9FumIPg"></iframe>

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

Here are a some **examples** of valid ARNs within lakeFS and their meaning:

| ARN                                                    | Meaning                               |
|--------------------------------------------------------|---------------------------------------|
| `arn:lakefs:auth:::user/jane.doe`                      | A specific user                       |
| `arn:lakefs:auth:::user/*`                             | All users                             | 
| `arn:lakefs:fs:::repository/myrepo/*`                  | All resources under `myrepo`          | 
| `arn:lakefs:fs:::repository/myrepo/object/foo/bar/baz` | A single object ARN                   | 
| `arn:lakefs:fs:::repository/myrepo/object/*`           | All objects in `myrepo`               | 
| `arn:lakefs:fs:::repository/*`                         | All repositories                      | 
| `arn:lakefs:fs:::*`                                    | All resources under the fs ARN prefix |

Additionally, the current user's ID is interpolated in runtime into the ARN using the `${user}` placeholder.

This allows us to create fine-grained policies affecting only a specific subset of resources.

See below for a full reference of ARNs and actions.


## Actions and Permissions

For the full list of actions and their required permissions, see the following table:

| Action name                                      | required action                             | Resource                                                                 | API endpoint                                                                          | S3 gateway operation                                                   |
|--------------------------------------------------|---------------------------------------------|--------------------------------------------------------------------------|---------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| List Repositories                                | `fs:ListRepositories`                       | `*`                                                                      | GET `/repositories`                                                                   | ListBuckets                                                            |
| Get Repository                                   | `fs:ReadRepository`                         | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repositoryId}`                                                    | HeadBucket                                                             |
| Get Commit                                       | `fs:ReadCommit`                             | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repositoryId}/commits/{commitId}`                                 | -                                                                      |
| Create Commit                                    | `fs:CreateCommit`                           | `arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`            | POST `/repositories/{repositoryId}/branches/{branchId}/commits`                       | -                                                                      |
| Get Commit log                                   | `fs:ReadBranch`                             | `arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`            | GET `/repositories/{repositoryId}/branches/{branchId}/commits`                        | -                                                                      |
| Create Repository                                | `fs:CreateRepository`                       | `arn:lakefs:fs:::repository/{repositoryId}`                              | POST `/repositories`                                                                  | -                                                                      |
| Namespace Attach to Repository                   | `fs:AttachStorageNamespace`                 | `arn:lakefs:fs:::namespace/{storageNamespace}`                           | POST `/repositories`                                                                  | -                                                                      |
| Import From Source                               | `fs:ImportFromStorage`                      | `arn:lakefs:fs:::namespace/{storageNamespace}`                           | POST `/repositories/{repositoryId}/branches/{branchId}/import`                        | -                                                                      |
| Cancel Import                                    | `fs:ImportCancel`                           | `arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`            | DELETE `/repositories/{repositoryId}/branches/{branchId}/import`                      | -                                                                      |
| Delete Repository                                | `fs:DeleteRepository`                       | `arn:lakefs:fs:::repository/{repositoryId}`                              | DELETE `/repositories/{repositoryId}`                                                 | -                                                                      |
| List Branches                                    | `fs:ListBranches`                           | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repositoryId}/branches`                                           | ListObjects/ListObjectsV2 (with delimiter = `/` and empty` prefix)     |
| Get Branch                                       | `fs:ReadBranch`                             | `arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`            | GET `/repositories/{repositoryId}/branches/{branchId}`                                | -                                                                      |
| Create Branch                                    | `fs:CreateBranch`                           | `arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`            | POST `/repositories/{repositoryId}/branches`                                          | -                                                                      |
| Delete Branch                                    | `fs:DeleteBranch`                           | `arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`            | DELETE `/repositories/{repositoryId}/branches/{branchId}`                             | -                                                                      |
| Merge branches                                   | `fs:CreateCommit`                           | `arn:lakefs:fs:::repository/{repositoryId}/branch/{destinationBranchId}` | POST `/repositories/{repositoryId}/refs/{sourceBranchId}/merge/{destinationBranchId}` | -                                                                      |
| Diff branch uncommitted changes                  | `fs:ListObjects`                            | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repositoryId}/branches/{branchId}/diff`                           | -                                                                      |
| Diff refs                                        | `fs:ListObjects`                            | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repositoryId}/refs/{leftRef}/diff/{rightRef}`                     | -                                                                      |
| Stat object                                      | `fs:ReadObject`                             | `arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`           | GET `/repositories/{repositoryId}/refs/{ref}/objects/stat`                            | HeadObject                                                             |
| Get Object                                       | `fs:ReadObject`                             | `arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`           | GET `/repositories/{repositoryId}/refs/{ref}/objects`                                 | GetObject                                                              |
| List Objects                                     | `fs:ListObjects`                            | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repositoryId}/refs/{ref}/objects/ls`                              | ListObjects, ListObjectsV2 (no delimiter, or "/" + non-empty` prefix)  |
| Upload Object                                    | `fs:WriteObject`                            | `arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`           | POST `/repositories/{repositoryId}/branches/{branchId}/objects`                       | PutObject, CreateMultipartUpload, UploadPart`, CompleteMultipartUpload |
| Delete Object                                    | `fs:DeleteObject`                           | `arn:lakefs:fs:::repository/{repositoryId}/object/{objectKey}`           | DELETE `/repositories/{repositoryId}/branches/{branchId}/objects`                     | DeleteObject, DeleteObjects`, AbortMultipartUpload                     |
| Revert Branch                                    | `fs:RevertBranch`                           | `arn:lakefs:fs:::repository/{repositoryId}/branch/{branchId}`            | PUT `/repositories/{repositoryId}/branches/{branchId}`                                | -                                                                      |
| Get Branch Protection Rules                      | `branches:GetBranchProtectionRules`         | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repository}/branch_protection`                                    | -                                                                      |
| Set Branch Protection Rules                      | `branches:SetBranchProtectionRules`         | `arn:lakefs:fs:::repository/{repositoryId}`                              | POST `/repositories/{repository}/branch_protection`                                   | -                                                                      |
| Delete Branch Protection Rules                   | `branches:SetBranchProtectionRules`         | `arn:lakefs:fs:::repository/{repositoryId}`                              | DELETE `/repositories/{repository}/branch_protection`                                 | -                                                                      |
| Create User                                      | `auth:CreateUser`                           | `arn:lakefs:auth:::user/{userId}`                                        | POST `/auth/users`                                                                    | -                                                                      |
| List Users                                       | `auth:ListUsers`                            | `*`                                                                      | GET `/auth/users`                                                                     | -                                                                      |
| Get User                                         | `auth:ReadUser`                             | `arn:lakefs:auth:::user/{userId}`                                        | GET `/auth/users/{userId}`                                                            | -                                                                      |
| Delete User                                      | `auth:DeleteUser`                           | `arn:lakefs:auth:::user/{userId}`                                        | DELETE `/auth/users/{userId}`                                                         | -                                                                      |
| Get Group                                        | `auth:ReadGroup`                            | `arn:lakefs:auth:::group/{groupId}`                                      | GET `/auth/groups/{groupId}`                                                          | -                                                                      |
| List Groups                                      | `auth:ListGroups`                           | `*`                                                                      | GET `/auth/groups`                                                                    | -                                                                      |
| Create Group                                     | `auth:CreateGroup`                          | `arn:lakefs:auth:::group/{groupId}`                                      | POST `/auth/groups`                                                                   | -                                                                      |
| Delete Group                                     | `auth:DeleteGroup`                          | `arn:lakefs:auth:::group/{groupId}`                                      | DELETE `/auth/groups/{groupId}`                                                       | -                                                                      |
| List Policies                                    | `auth:ListPolicies`                         | `*`                                                                      | GET `/auth/policies`                                                                  | -                                                                      |
| Create Policy                                    | `auth:CreatePolicy`                         | `arn:lakefs:auth:::policy/{policyId}`                                    | POST `/auth/policies`                                                                 | -                                                                      |
| Update Policy                                    | `auth:UpdatePolicy`                         | `arn:lakefs:auth:::policy/{policyId}`                                    | POST `/auth/policies`                                                                 | -                                                                      |
| Delete Policy                                    | `auth:DeletePolicy`                         | `arn:lakefs:auth:::policy/{policyId}`                                    | DELETE `/auth/policies/{policyId}`                                                    | -                                                                      |
| Get Policy                                       | `auth:ReadPolicy`                           | `arn:lakefs:auth:::policy/{policyId}`                                    | GET `/auth/policies/{policyId}`                                                       | -                                                                      |
| List Group Members                               | `auth:ReadGroup`                            | `arn:lakefs:auth:::group/{groupId}`                                      | GET `/auth/groups/{groupId}/members`                                                  | -                                                                      |
| Add Group Member                                 | `auth:AddGroupMember`                       | `arn:lakefs:auth:::group/{groupId}`                                      | PUT `/auth/groups/{groupId}/members/{userId}`                                         | -                                                                      |
| Remove Group Member                              | `auth:RemoveGroupMember`                    | `arn:lakefs:auth:::group/{groupId}`                                      | DELETE `/auth/groups/{groupId}/members/{userId}`                                      | -                                                                      |
| List User Credentials                            | `auth:ListCredentials`                      | `arn:lakefs:auth:::user/{userId}`                                        | GET `/auth/users/{userId}/credentials`                                                | -                                                                      |
| Create User Credentials                          | `auth:CreateCredentials`                    | `arn:lakefs:auth:::user/{userId}`                                        | POST `/auth/users/{userId}/credentials`                                               | -                                                                      |
| Delete User Credentials                          | `auth:DeleteCredentials`                    | `arn:lakefs:auth:::user/{userId}`                                        | DELETE `/auth/users/{userId}/credentials/{accessKeyId}`                               | -                                                                      |
| Get User Credentials                             | `auth:ReadCredentials`                      | `arn:lakefs:auth:::user/{userId}`                                        | GET `/auth/users/{userId}/credentials/{accessKeyId}`                                  | -                                                                      |
| List User Groups                                 | `auth:ReadUser`                             | `arn:lakefs:auth:::user/{userId}`                                        | GET `/auth/users/{userId}/groups`                                                     | -                                                                      |
| List User Policies                               | `auth:ReadUser`                             | `arn:lakefs:auth:::user/{userId}`                                        | GET `/auth/users/{userId}/policies`                                                   | -                                                                      |
| Attach Policy To User                            | `auth:AttachPolicy`                         | `arn:lakefs:auth:::user/{userId}`                                        | PUT `/auth/users/{userId}/policies/{policyId}`                                        | -                                                                      |
| Detach Policy From User                          | `auth:DetachPolicy`                         | `arn:lakefs:auth:::user/{userId}`                                        | DELETE `/auth/users/{userId}/policies/{policyId}`                                     | -                                                                      |
| List Group Policies                              | `auth:ReadGroup`                            | `arn:lakefs:auth:::group/{groupId}`                                      | GET `/auth/groups/{groupId}/policies`                                                 | -                                                                      |
| Attach Policy To Group                           | `auth:AttachPolicy`                         | `arn:lakefs:auth:::group/{groupId}`                                      | PUT `/auth/groups/{groupId}/policies/{policyId}`                                      | -                                                                      |
| Detach Policy From Group                         | `auth:DetachPolicy`                         | `arn:lakefs:auth:::group/{groupId}`                                      | DELETE `/auth/groups/{groupId}/policies/{policyId}`                                   | -                                                                      |
| Attach External Principal to a User              | `auth:CreateUserExternalPrincipal`          | `arn:lakefs:auth:::user/{userId}`                                        | POST `/auth/users/{userId}/external/principals`                                       | -                                                                      |
| Delete External Principal Attachment from a User | `auth:DeleteUserExternalPrincipal`          | `arn:lakefs:auth:::user/{userId}`                                        | DELETE `/auth/users/{userId}/external/principals`                                     | -                                                                      |
| Get the User attached to an External Principal   | `auth:ReadExternalPrincipal`                | `arn:lakefs:auth:::externalPrincipal/{principalId}`                      | GET `/auth/external/principals`                                                       | -                                                                      |
| Read Storage Config                              | `fs:ReadConfig`                             | `*`                                                                      | GET `/config/storage`                                                                 | -                                                                      |
| Get Garbage Collection Rules                     | `retention:GetGarbageCollectionRules`       | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repositoryId}/gc/rules`                                           | -                                                                      |
| Set Garbage Collection Rules                     | `retention:SetGarbageCollectionRules`       | `arn:lakefs:fs:::repository/{repositoryId}`                              | POST `/repositories/{repositoryId}/gc/rules`                                          | -                                                                      |
| Prepare Garbage Collection Commits               | `retention:PrepareGarbageCollectionCommits` | `arn:lakefs:fs:::repository/{repositoryId}`                              | POST `/repositories/{repositoryId}/gc/prepare_commits`                                | -                                                                      |
| List Repository Action Runs                      | `ci:ReadAction`                             | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repository}/actions/runs`                                         | -                                                                      |
| Get Action Run                                   | `ci:ReadAction`                             | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repository}/actions/runs/{run_id}`                                | -                                                                      |
| List Action Run Hooks                            | `ci:ReadAction`                             | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repository}/actions/runs/{run_id}/hooks`                          | -                                                                      |
| Get Action Run Hook Output                       | `ci:ReadAction`                             | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repository}/actions/runs/{run_id}/hooks/{hook_run_id}/output`     | -                                                                      |
| Get Pull Request                                 | `pr:ReadPullRequest`                        | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repository}/pulls/{pull_request}`                                 | -                                                                      |
| Create Pull Request                              | `pr:WritePullRequest`                       | `arn:lakefs:fs:::repository/{repositoryId}`                              | POST `/repositories/{repository}/pulls`                                               | -                                                                      |
| Update Pull Request                              | `pr:WritePullRequest`                       | `arn:lakefs:fs:::repository/{repositoryId}`                              | PATCH `/repositories/{repository}/pulls/{pull_request}`                               | -                                                                      |
| Merge Pull Request                               | `pr:WritePullRequest` + Merge Branches      | `arn:lakefs:fs:::repository/{repositoryId}`                              | PUT `/repositories/{repository}/pulls/{pull_request}/merge`                           | -                                                                      |
| List Pull Requests                               | `pr:ListPullRequests`                       | `arn:lakefs:fs:::repository/{repositoryId}`                              | GET `/repositories/{repository}/pulls`                                                | -                                                                      |
| Login as Organization Admin _(Cloud only)_       | `admin:Login`                               | `*`                                                                      | POST `/admin/login` _(part of lakefs cloud, not lakefs endpoint)_                     | -                                                                      | 


Some APIs may require more than one action.  For instance, to create a repository (`POST /repositories`), 
you need permission to `fs:CreateRepository` for the _name_ of the repository and also
`fs:AttachStorageNamespace` for the _storage namespace_ used.

## Preconfigured Policies

The following `Policies` are created during initial setup:

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
                "fs:Read*",
                "fs:List*",
                "fs:WriteObject",
                "fs:DeleteObject",
                "fs:RevertBranch",
                "fs:CreateBranch",
                "fs:CreateTag",
                "fs:DeleteBranch",
                "fs:DeleteTag",
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

### AdminFullAccess (lakeFS Cloud only)

```json
{
  "statement": [
    {
      "action": [
        "admin:*"
      ],
      "effect": "allow",
      "resource": "*"
    }
  ]
}
```

## Additional Policies

You can create additional policies to further limit user access. Use the web UI or
the [lakectl auth](../reference/cli.md#lakectl-auth-policies-create) command to create policies.

Here is an example to define read/write access for a specific repository:

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

## Multiple Resources Statements

lakeFS supports specifying multiple resources in a single RBAC statement. This is available on lakeFS Cloud, or on lakeFS Enterprise if starting at version lakeFS v1.54.0 and Fluffy v0.12.0
In addition to a single resource. The resource field can contain a string representing a JSON-encoded list of resources.

```json
{
    "statement": [
        {
            "action": [
                "fs:Read*"
            ],
            "effect": "allow",
            "resource": "[\"arn:lakefs:fs:::repository/repo1\",\"arn:lakefs:fs:::repository/repo2\"]"
        }
    ]
}
```

The list must be properly encoded as a JSON string: quote each resource, and escape those quotes as shown.
Otherwise, the policy cannot be parsed.

### Multi-Resource Policy Creation Using Python SDK

Here is how you can leverage Python SDK to create a multiple resource policy:

```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk import models

configuration = lakefs_sdk.Configuration(
        host=lakefsEndPoint,
        username=lakefsAccessKey,
        password=lakefsSecretKey,
)
clt = LakeFSClient(configuration)

clt.auth_api.create_policy(
    policy=models.Policy(
        id='FSReadTwoRepos',
        statement=[models.Statement(
            effect="deny",
            resource=json.dumps(["arn:lakefs:fs:::repository/repo1","arn:lakefs:fs:::repository/repo2"]),
            action=["fs:ReadRepository"],
        ),
        ]
    )
)
```

## Preconfigured Groups

lakeFS has four preconfigured groups:

* Admins
* SuperUsers
* Developers
* Viewers

They have the following policies granted to them:

| Policy                                                  | Admins | SuperUsers | Developers | Viewers |
|---------------------------------------------------------|--------|------------|------------|---------|
| [`FSFullAccess`](#fsfullaccess)                         | ✅      | ✅          |            |         |
| [`AuthFullAccess`](#authfullaccess)                     | ✅      |            |            |         |
| [`RepoManagementFullAccess`](#repomanagementfullaccess) | ✅      |            |            |         |
| [`AuthManageOwnCredentials`](#authmanageowncredentials) |        | ✅          | ✅          | ✅       |
| [`RepoManagementReadAll`](#repomanagementreadall)       |        | ✅          | ✅          |         |
| [`FSReadWriteAll`](#fsreadwriteall)                     |        |            | ✅          |         |
| [`FSReadAll`](#fsreadall)                               |        |            |            | ✅       |
| [`AdminFullAccess` (_Cloud only)_](#adminfullaccess)    | ✅      |            |            |         |

## Pluggable Authentication and Authorization

Authorization and authentication is pluggable in lakeFS. 

If lakeFS is attached to a [remote authentication server](./remote-authenticator.md) (or you are using lakeFS Cloud) then the role-based access control user interface can be used. 

If you are using RBAC with your self-managed lakeFS then the lakeFS configuration element `auth.ui_config.rbac` should be set to `external`. 

An enterprise (paid) solution of lakeFS should set `auth.ui_config.rbac` as `internal`.
