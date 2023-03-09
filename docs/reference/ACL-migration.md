### The new ACL model
As mentioned in our [Security Update](https://docs.lakefs.io/posts/security_update.html#whats-changing) the coming lakeFS version introduces a simplified authorization mechanism based on ACLs.

### Permissions
In the ACL model Every group will have a permission attached to it,
lakeFS provides the following permissions:

| Permission | Allows | Existing Group |
|------------|--------------------------------------------|---------------------------|
| **Read** | Read operations, creating access keys. | Viewers |
| **Write** | Allows all data read and write operations. | Developers |
| **Super** | Allows all operations except auth. | SuperUsers (with changes) |
| **Admin** | Allows all operations. | Admins |

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


In the UI:

![Groups page has a dropdown to edit permission for each
group]( ../assets/img/ACL-groups-with-perms.png)



### The migration

Upgrading the lakeFS version will require running a migrating to our ACL authorization model in case:
- One or more of the basic poicies was modified
- New policies where defined
- Users have attached policies

In order to run the migration run:
```
TODO - Add the command
```

The command defaults to dry-run,
We recommend running the script only after running the dry-run and going over the warnings
In order to apply the migration you will need  `--yes`  flag

The upgrade will ensure that the 4 default groups exist, and modify existing groups to fit into the new ACLs model:
-  When creating the 4 default global groups: if another group exists and has the desired name, upgrading will rename it by appending ".orig". So after upgrading the 4 default global groups exist, with these known names.
- For any group, upgrading configured policies follows these rules, possibly increasing access:
    1. Any "Deny" rules are stripped, and a warning printed.
    2. "Manage own credentials" is added.
    3. If any actions outside of "fs:" are allowed, the group becomes an Admin group, a warning is printed, and no further changes apply.
    4. The upgrade script unifies repositories: If a permission applies to a set of repositories with a wildcard, permissions are unified to all repositories. Otherwise they apply to the list of all repositories, in all the policies.
    5. The upgrade script unifies actions: it selects the least permission of Read, Write, Super that contains all of the allowed actions. If any allowed action contains a wildcard this still makes sense.
       All warnings link to support@ and to Slack.

### Plugable authentication authorization
As mentioned in the  [Security Update](https://docs.lakefs.io/posts/security_update.html#whats-changing) authorization and authentication is pluggable.
If lakeFS is attached to an external authorization server, the existing RBAC GUI may continue to be used. It is not possible to use both types of GUI at the same time: Moving from RBAC to simplified may only be performed once and **will** lose configuration.