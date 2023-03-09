### The new ACL model
As mentioned in our [Security Update](https://docs.lakefs.io/posts/security_update.html#whats-changing) the coming lakeFS version introduces a simplified authorization mechanism based on ACLs.

### Permissions
The new ACL model defines permissions at the group level, 
we'll review the new permissions compared to their previous representation:

| Permission | Allows                                     | Previous Group Name       | previous policies names and action list                                                                                                                                                                                                                                                                                                                                          | 
|------------|--------------------------------------------|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Read**   | Read operations, creating access keys.     | Viewers                   | FSReadAll \[fs:List*, fs:Read*]                                                                                                                                                                                                                                                                                                                                                  |
| **Write**  | Allows all data read and write operations. | Developers                | FSReadWriteAll \[fs:ListRepositories, fs:ReadRepository, fs:ReadCommit, fs:ListBranches, fs:ListTags, fs:ListObjects, fs:ReadObject, fs:WriteObject, fs:DeleteObject, fs:RevertBranch, fs:ReadBranch, fs:ReadTag, fs:CreateBranch, fs:CreateTag, fs:DeleteBranch, fs:DeleteTag, fs:CreateCommit] RepoManagementReadAll \[ci:Read*, retention:Get*, branches:Get*, fs:ReadConfig] |
| **Super**  | Allows all operations except auth.         | SuperUsers (with changes) | FSFullAccess  \[fs:*] RepoManagementReadAll \[ci:Read*, retention:Get*, branches:Get*, fs:ReadConfig]                                                                                                                                                                                                                                                                            |
| **Admin**  | Allows all operations.                     | Admins                    | AuthFullAccess \[auth:*]  FSFullAccess \[fs:*]  RepoManagementFullAccess \[ci:*, retention:*, branches:*, fs:ReadConfig]                                                                                                                                                                                                                                                         |

### Scopes

When granted to a group, permissions Read, Write, and Super may be scoped to a set of repositories.
Admin includes global abilities that apply across repos and cannot be scoped to a set of repositories

### ACL

You can now attach Permissions and scope them to groups in the Groups page
There are 4 default groups, named after the 4 permissions. Each group is global (applies for all repositories).
The group list initially would be

| Group ID    | Default Permission | Created at       | Repositories |
|-------------|--------------------|------------------|--------------|
| **Read**    | Read               | \<Creation date> | All          |
| **Write**   | Write              | \<Creation date> | All          |
| **Super**   | Super              | \<Creation date> | All          |
| **Admin**   | Admin              | \<Creation date> | All          |


### The migration

Upgrading the lakeFS version will require migrating to the new ACL authorization model

In order to run the migration run:
```
lakefs migrate auth-acl
```

The command defaults to dry-run.
We recommend running the script only after running the dry-run and going over the warnings
To apply the migration, re-run with the  `--yes`  flag

The upgrade will ensure that the 4 default groups exist, and modify existing groups to fit into the new ACLs model:
-  When creating the 4 default global groups: if another group exists and has the desired name, upgrading will rename it by appending ".orig". So after upgrading the 4 default global groups exist, with these known names.
- For any group, upgrading configured policies follows these rules, possibly increasing access:
    1. Any "Deny" rules are stripped, and a warning printed.
    2. "Manage own credentials" is added.
    3. If any actions outside of "fs:" and manage own credentials are allowed, the group becomes an Admin group, a warning is printed, and no further changes apply.
    4. The upgrade script unifies repositories: If a resource applies to a set of repositories with a wildcard, permissions are unified to all repositories. Otherwise they apply to the list of all repositories, in all the policies.
    5. The upgrade script unifies actions: it selects the least permission of Read, Write, Super that contains all of the allowed actions.
All warnings link to support@treeverse.io" and to Slack, For any question or concern during the upgrade, don't hesitate to get in touch with us through Slack or Support email.

### Plugable authentication authorization
As mentioned in the  [Security Update](https://docs.lakefs.io/posts/security_update.html#whats-changing) authorization and authentication is pluggable.
If lakeFS is attached to an external authorization server, the existing RBAC GUI may continue to be used. It is not possible to use both types of GUI at the same time: Moving from RBAC to simplified may only be performed once and **will** lose configuration.