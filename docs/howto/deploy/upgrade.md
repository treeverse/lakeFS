---
title: Upgrade lakeFS
description: How to upgrade lakeFS to the latest version.
grand_parent: How-To
parent: Install lakeFS
redirect_from: 
  - /deploying-aws/upgrade.html
  - /reference/upgrade.html
  - /howto/upgrade.html
---

# Upgrading lakeFS

Note: For a fully managed lakeFS service with guaranteed SLAs, try [lakeFS Cloud](https://lakefs.cloud)
{: .note }

Upgrading lakeFS from a previous version usually just requires re-deploying with the latest image (or downloading the latest version if you're using the binary).
If you're upgrading, check whether the [release](https://github.com/treeverse/lakeFS/releases) requires a migration.

## When DB migrations are required

### lakeFS 0.103.0 or greater

Version 0.103.0 added support for rolling KV upgrade. This means that users who already migrated to the KV ref-store (versions 0.80.0 and above) no longer have to pass through specific versions for migration.
This includes [ACL migration](https://docs.lakefs.io/reference/access-control-lists.html#migrating-from-the-previous-version-of-acls) which was introduced in lakeFS version 0.98.0.
Running `lakefs migrate up` on the latest lakeFS version will perform all the necessary migrations up to that point.

### lakeFS 0.80.0 or greater (KV Migration)

Starting with version 0.80.2, lakeFS has transitioned from using a PostgreSQL based database implementation to a Key-Value datastore interface supporting
multiple database implementations. More information can be found [here](https://github.com/treeverse/lakeFS/blob/master/design/accepted/metadata_kv/index.md).  
Users upgrading from a previous version of lakeFS must pass through the KV migration version (0.80.2) before upgrading to newer versions of lakeFS.

> **IMPORTANT: Pre Migrate Requirements**  
> * **Users using OS environment variables for database configuration must define the `connection_string` explicitly or as environment variable before proceeding with the migration.**  
> * **Database storage free capacity of at least twice the amount of the currently used capacity**
> * It is strongly recommended to perform these additional steps:
>   * Commit all uncommitted data on branches
>   * Create a snapshot of your database
> * By default, old database tables are not being deleted by the migration process, and should be removed manually after a successful migration.
> To enable table drop as part of the migration, set the `database.drop_tables` configuration param to `true`
{: .note }

#### Migration Steps
For each lakeFS instance currently running with the database
1. Modify the `database` section under lakeFS configuration yaml:
   1. Add `type` field with `"postgres"` as value
   2. Copy the current configuration parameters to a new section called `postgres`

   ```yaml
   ---
   database:
    type: "postgres"
    connection_string: "postgres://localhost:5432/postgres?sslmode=disable"
    max_open_connections: 20
   
    postgres:
      connection_string: "postgres://localhost:5432/postgres?sslmode=disable"
      max_open_connections: 20
   ```

2. Stop all lakeFS instances
3. Using the `lakefs` binary for the new version (0.80.2), run the following:

   ```bash
   lakefs migrate up
   ```

4. lakeFS will run the migration process, which in the end should display the following message with no errors:

   ```shell
   time="2022-08-10T14:46:25Z" level=info msg="KV Migration took 717.629563ms" func="pkg/logging.(*logrusEntryWrapper).Infof" file="build/pkg/logging/logger.go:246" TempDir=/tmp/kv_migrate_2913402680
   ```

5. It is now possible to remove the old database configuration. The updated configuration should look as such:

   ```yaml
   ---
   database:
    type: "postgres"
   
    postgres:
      connection_string: "postgres://localhost:5432/postgres?sslmode=disable"
      max_open_connections: 20
   ```
 
6. Deploy (or run) the new version of lakeFS.

### lakeFS 0.30.0 or greater

In case migration is required, you first need to stop the running lakeFS service.
Using the `lakefs` binary for the new version, run the following:

```bash
lakefs migrate up
```

Deploy (or run) the new version of lakeFS.

Note that an older version of lakeFS cannot run on a migrated database.


### Prior to lakeFS 0.30.0

**Note:** with lakeFS < 0.30.0, you should first upgrade to 0.30.0 following this guide. Then, proceed to upgrade to the newest version.
{: .note .pb-3 }

Starting version 0.30.0, lakeFS handles your committed metadata in a [new way](https://docs.google.com/document/d/1jzD7-jun-tdU5BGapmnMBe9ovSzBvTNjXCcVztV07A4/edit?usp=sharing){: target="_blank" }, which is more robust and has better performance.
To move your existing data, you will need to run the following upgrade commands.

Verify lakeFS version == 0.30.0 (can skip if using Docker)

```shell
lakefs --version
```

Migrate data from the previous format:

```shell
lakefs migrate db
```

Or migrate using Docker image:

```shell
docker run --rm -it -e LAKEFS_DATABASE_CONNECTION_STRING=<database connection string> treeverse/lakefs:rocks-migrate migrate db
```

Once migrated, it is possible to now use more recent lakeFS versions. Please refer to their release notes for more information on ugrading and usage).


If you want to start over, discarding your existing data, you need to explicitly state this in your lakeFS configuration file.
To do so, add the following to your configuration (relevant **only** for 0.30.0):

```yaml
cataloger:
  type: rocks
```

## Data Migration for Version v0.50.0

If you are using a version before 0.50.0, you must first perform the [previous upgrade to that version](https://docs.lakefs.io/v0.50/reference/upgrade.html#data-migration-for-version-v0500). {: note: .note-warning }