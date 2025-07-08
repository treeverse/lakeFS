---
title: Access Control Lists (ACLs)
description: Access control lists (ACLs) are one of the resource-based options that you can use to manage access to your repositories and objects. There are limits to managing permissions using ACLs.
status: deprecated
---

# Access Control Lists (ACLs)

!!! warning "Deprecated"
    ACLs were [removed from core lakeFS](https://lakefs.io/blog/why-moving-acls-out-of-core-lakefs/).

    For a more robust authorization solution, please see [Role-Based Access Control](rbac.md), available in [lakeFS Cloud](../cloud/index.md) and [lakeFS Enterprise](../enterprise/index.md).  

    The following documentation is aimed for users with existing installations who wish to continue working with ACLs. 

## Basic Auth Functionality

New lakeFS versions will provide basic auth functionality featuring a single Admin user with a single set of credentials.
Existing lakeFS installations that have a single user and a single set of credentials will migrate seamlessly to the new version.  
Installations that have more than one user / credentials will require to run a command and choose which set of user + credentials to migrate
(more details [here](#migration-of-existing-user))

### Credentials Replacement

In a single user setup, replacing credentials can be done as follows:

1. Delete the existing user:

    ```shell
    lakectl auth users delete --id <user-id>
    ```

2. Shut down the lakeFS server - Required for invalidating the old credentials on the server
3. Create a new user, with the same name and new credentials:

    ```shell
    lakefs superuser --user-name <user-id>
    ```

    This will generate a new set of credentials, and will print it out to the screen:

    ```
    credentials:
      access_key_id: *** (omitted)
      secret_access_key: *** (omitted)
    ```

4. Re-run lakeFS server

!!! warning
    Calling the `superuser` command with pre-defined `--access-key-id` and `--secret-access-key` is possible,
    but should be done with caution. Make sure that `--secret-access-key` is **not empty**,
    as providing an access key without a secret key will trigger an ACL import flow
    (see [Migration of existing user](#migration-of-existing-user)).

    In case you already deleted the user by following step (1), this import operation will **fail** and result in an 
    **unrecoverable** state, from which a clean installation is the only way out.

## ACLs

ACL server was moved out of core lakeFS and into a new package under `contrib/auth/acl`.
Though we [decided](https://lakefs.io/blog/why-moving-acls-out-of-core-lakefs/) to move ACLs out, we are committed to making sure existing users who still need the use of ACLs can continue using
this feature.
In order to do that, users will need to run the separate ACL server as part of their lakeFS deployment environment and configure lakeFS to work with it.

### ACL server Configuration

Under the `contrib/auth/acl` you will be able to find an ACL server reference.

!!! warning
    This implementation is a reference and is not fit for production use.

    For a more robust authorization solution, please see [Role-Based Access Control](rbac.md), available in [lakeFS Cloud](../cloud/index.md) and [lakeFS Enterprise](../enterprise/index.md). 

The configuration of the ACL server is similar to lakeFS configuration, here's an example of an `.aclserver.yaml` config file:

```yaml
---
listen_address: "[ACL_SERVER_LISTEN_ADDRESS]"
database:
    type: "postgres"
    postgres:
    connection_string: "[DATABASE_CONNECTION_STRING]"

encrypt:
    # This should be the same encryption key as in lakeFS
    secret_key: "[ENCRYPTION_SECRET_KEY]"
```

It is possible to use environment variables to configure the server as in lakeFS. Use the `ACLSERVER_` prefix to do so.  

!!! info
    For full configuration reference see: [this](https://github.com/treeverse/lakeFS/blob/7b2a0ac2f1afedd2059284c32e7dacb945b2ae90/contrib/auth/acl/config.go#L26)

### lakeFS Configuration

For the ACL server to work, configure the following values in lakeFS:  

- `auth.ui_config.rbac`: `simplified`  
- `auth.api.endpoint`: `[ACL_SERVER_LISTEN_ADDRESS]`

### Migration of existing user

For installation with multiple users / credentials, upgrading to the new lakeFS version requires choosing which user + credentials will be used for the single user mode.
This is done via the `lakefs superuser` command.
For example, if you have a user with username `<my-username>` and credential key `<my-access-key-id>` use the following command to migrate that user:

```bash
lakefs superuser --user-name <my-username> --access-key-id <my-access-key-id>
```
  
After running the command you will be able to access the installation using the user's access key id and its respective secret access key.
