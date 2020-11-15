---
layout: default
title: Upgrade lakeFS
parent: AWS Deployment
nav_order: 50
has_children: false
---

# Upgrade lakeFS
{: .no_toc }
Upgrading lakeFS from a previous version usually just requires re-deploying with the latest image (or downloading the latest version, if you're using the binary).
There are cases where the database will require a migration - check whether the [release](https://github.com/treeverse/lakeFS/releases) you are upgrading to requires that.

# Migrating

In case a migration is required, first stop the running lakeFS service.
Using the new lakeFS version, run the following:

```bash
lakefs migrate up
```

Run and/or deploy the new version of lakeFS.

Note that previous version of lakeFS will not run on new migrated database.
Rolling back changes will require running

```bash
lakefs migrate goto <number>
```

The <number> depends on the verison of lakeFS you roll back.
