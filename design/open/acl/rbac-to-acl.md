# TLD for lakeFS Simplified Authorization: from RBAC to ACL

## Goal

As part of part of [simplifying auth*][auth-sec-update], simplify the lakeFS
authorization system to use ACLs instead of PBAC as exists currently.

A simple GUI will be available for placing users into groups and configuring
allowed access for each group.

The bedrock of authorization in lakeFS remains the policy engine.  This is a
well-tested proven solution.  Advanced users will be able to retain policies
if they deploy and configure a external authorization server.  However there
will be no authorization server in lakeFS itself.

## User experience

### Simplified user experience

This section does _not_ do mockups, we may have those separately.

#### Concepts

##### Permissions

A _permission_ allows a group of users to do something.  Users cannot create
permissions, lakeFS provides only these permissions:

| Permission | Allows                                     | Existing Group            |
|------------|--------------------------------------------|---------------------------|
| *Read*     | Read operations, creating access keys.     | Viewers                   |
| *Write*    | Allows all data read and write operations. | Developers                |
| *Super*    | Allows all operations except auth.         | SuperUsers (with changes) |
| *Admin*    | Allows all operations.                     | Admins                    |

In existing RBAC terms, the only permissions for non-admins are under `fs:*`
and those to manage own credentials(`AuthManageOwnCredentials`).  The latter
is given to all users.

Some service jobs such as GC must be run using a user with Admin permission.

##### Scopes

When granted to a group, permissions Read, Write, and Super may be _scoped_.
Then they apply only to a set of repositories.  It is not possible to scope
permission Admin: it includes global abilities that apply across repos, and
can modify granted permissions.

##### Grants

A group is _granted_ a single permission within a scope.  "Admin" permission
has no scope (grayed out in the GUI, and all repos are allowed).

#### Out: RBAC

Remove from the GUI the policy page and all references to policies, and from
`lakectl` all policy-related commands.  The API will no longer support these
operations, either.  It will return [405 Method Not Allowed][http-stat-405],
or alternatively [501 Unimplemented][http-stat-501] when called: it is now a
partial implementation of the auth* API.

#### In: ACLs

Every group on the list of groups on the Groups page uses the format:

| Group ID | Permission          | Created at | Repositories |
|----------|---------------------|------------|--------------|
| <name>   | Permission dropdown | datetime   | <number>     |

![Groups page has a dropdown to edit permission for each
group](./groups-with-perms.png)

This gives information about the group and allows editing its permission.

Clicking on the group takes us to the group's subpage with 2 tabs:

* *Members*: The current "Group Memberships" tab.
* *Repositories*: A list of configured repositories.

  This tab has an "all" toggle at the top.  If the permission is "Admin" the
  toggle is set and cannot be changed.  Otherwise it controls a selection of
  groups using the same GUI mechanism as used today for memberships (and for
  policies, ironically).

### Upgrade to simplified

### PBAC user experience with external authorization server

If lakeFS is attached to an external authorization server, the existing PBAC
GUI may continue to be used.  It is not possible to use both types of GUI at
the same time: Moving from PBAC to simplified may only be performed once and
**will** lose configuration.


[auth-sec-update]:  https://docs.lakefs.io/posts/security_update.html#whats-changing
[http-stat-405]:  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405
[http-stat-501]:  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/501
