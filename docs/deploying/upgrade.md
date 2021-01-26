---
layout: default
title: Upgrade lakeFS
description: Upgrading lakeFS from a previous version usually just requires re-deploying with the latest image or downloading the latest version
parent: AWS Deployment
nav_order: 50
has_children: false
---

# Upgrading lakeFS
{: .no_toc }
Upgrading lakeFS from a previous version usually just requires re-deploying with the latest image (or downloading the latest version, if you're using the binary).
There are cases where the database will require a migration - check whether the [release](https://github.com/treeverse/lakeFS/releases) you are upgrading to requires that.


# Migrating from lakeFS >= 0.30.0

In case a migration is required, first stop the running lakeFS service.
Using the `lakefs` binary for the new version, run the following:

```bash
lakefs migrate up
```

Deploy (or run) the new version of lakeFS.

Note that an older version of lakeFS cannot run on a migrated database.


# Migrating from lakeFS < 0.30.0

Starting version 0.30.0, lakeFS handles your committed metadata in a new way, which is more robust and has better performance.
To move your existing data, you will need to run the following upgrade command with the updated lakefs binary:

```shell
lakefs migrate db
```

If you want to start over, discarding your existing data, you need to explicitly state this in your lakeFS configuration file.
To do so, add the following to your configuration:

```yaml
cataloger:
  type: rocks
```

