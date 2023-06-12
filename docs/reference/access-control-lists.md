---
layout: default
title: Access Control Lists (ACLs)
parent: Reference
description: Access control lists (ACLs) are one of the resource-based options that you can use to manage access to your repositories and objects. There are limits to managing permissions using ACLs.
nav_order: 100
has_children: false
redirect_from: /reference/access-control-list.html
---

# Access Control Lists (ACLs)

{: .note}
> ACLs were introduced in their current form in v0.97 of lakeFS as part of [changes to the security model](/posts/security_update.html#whats-changing) in lakeFS. They are an alternative to the more granular control that [role-based access control](rbac.html) provides.


{% include toc.html %}

## ACLs

You can attach Permissions and scope them to groups in the Groups page.
There are 4 default groups, named after the 4 permissions. Each group is global (applies for all repositories).

| Group ID  | Allows                                     | 
|-----------|--------------------------------------------|
| **Read**  | Read operations, creating access keys      |
| **Write** | Allows all data read and write operations. |
| **Super** | Allows all operations except auth.         |
| **Admin** | Allows all operations.                     |

## Pluggable Authentication and Authorization

Authorization and authentication is pluggable in lakeFS. If lakeFS is attached to a [remote authentication server](remote-authenticator.html) (or you are using lakeFS Cloud) then the [role-based access control](rbac.html) user interface can be used.

If you are using ACL then the lakeFS configuration element `auth.ui_config.RBAC` should be set to `simplified`.

## Previous versions of ACL in lakeFS

Here's how the current ACL model compares to to that prior to [the changes introduced](/posts/security_update.html#whats-changing) in v0.97.

| Permission | Allows                                     | Previous Group Name       | Previous Policy Names and Actions                                                                                                                                                                                                                                                                                                                                                | 
|------------|--------------------------------------------|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Read**   | Read operations, creating access keys.     | Viewers                   | FSReadAll \[fs:List*, fs:Read*]                                                                                                                                                                                                                                                                                                                                                  |
| **Write**  | Allows all data read and write operations. | Developers                | FSReadWriteAll \[fs:ListRepositories, fs:ReadRepository, fs:ReadCommit, fs:ListBranches, fs:ListTags, fs:ListObjects, fs:ReadObject, fs:WriteObject, fs:DeleteObject, fs:RevertBranch, fs:ReadBranch, fs:ReadTag, fs:CreateBranch, fs:CreateTag, fs:DeleteBranch, fs:DeleteTag, fs:CreateCommit] RepoManagementReadAll \[ci:Read*, retention:Get*, branches:Get*, fs:ReadConfig] |
| **Super**  | Allows all operations except auth.         | SuperUsers (with changes) | FSFullAccess  \[fs:*] RepoManagementReadAll \[ci:Read*, retention:Get*, branches:Get*, fs:ReadConfig]                                                                                                                                                                                                                                                                            |
| **Admin**  | Allows all operations.                     | Admins                    | AuthFullAccess \[auth:*]  FSFullAccess \[fs:*]  RepoManagementFullAccess \[ci:*, retention:*, branches:*, fs:ReadConfig]                                                                                                                                                                                                                                                         |

### Migrating from the previous version of ACLs

Upgrading the lakeFS version will require migrating to the new ACL authorization model.

In order to run the migration run:
```
lakefs migrate up
```

The command will run the migration to ACL. The migration process might adjust the current authorization policies to fit ACL, in that case the command will not make any changes, only print warnings.  
In case of warnings to apply the migration, re-run with the  `--force`  flag

The upgrade will ensure that the 4 default groups exist, and modify existing groups to fit into the new ACLs model:
-  When creating the 4 default global groups: if another group exists and has the desired name, upgrading will rename it by appending ".orig". So after upgrading the 4 default global groups exist, with these known names.
- For any group, upgrading configured policies follows these rules, possibly increasing access:
    1. Any "Deny" rules are stripped, and a warning printed.
    2. "Manage own credentials" is added.
    3. If any actions outside of "fs:" and manage own credentials are allowed, the group becomes an Admin group, a warning is printed, and no further changes apply.
    4. The upgrade script unifies repositories: If a resource applies to a set of repositories, permissions are unified to all repositories.
    5. The upgrade script unifies actions: it selects the least permission of Read, Write, Super that contains all of the allowed actions.

The upgrade will detach every directly attached policy from users 

Note that moving to ACL from RBAC may only be performed once and **will** lose some configuration.  The upgrade script will detail the changes made by the transition.

For any question or concern during the upgrade, don't hesitate to get in touch with us through [Slack](https://lakefs.io/slack) or [email](mailto:support@treeverse.io).