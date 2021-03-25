---
layout: default
title: Upgrade lakeFS
description: Upgrading lakeFS from a previous version usually just requires re-deploying with the latest image or downloading the latest version
parent: Azure Deployment
nav_order: 50
has_children: false
---

# Upgrading lakeFS
{: .no_toc }
Upgrading lakeFS from a previous version usually just requires re-deploying with the latest image (or downloading the latest version, if you're using the binary).
There are cases where the database will require a migration - check whether the [release](https://github.com/treeverse/lakeFS/releases) you are upgrading to requires that.


In case a migration is required, first stop the running lakeFS service.
Using the `lakefs` binary for the new version, run the following:

```bash
lakefs migrate up
```

Deploy (or run) the new version of lakeFS.

Note that an older version of lakeFS cannot run on a migrated database.

**Note** If you are using Kubernetes with the lakeFS helm chart it will run migrate up automatically
{: .note .note-info }