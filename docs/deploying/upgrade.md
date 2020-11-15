---
layout: default
title: Upgrade lakeFS
parent: AWS Deployment
nav_order: 50
has_children: false
---

# Upgrade lakeFS
{: .no_toc }
Upgrade lakeFS from previous version usually require just downloading the latest release or deploy the latest version.
There are cases where lakeFS database will require a migrate - check the [releases](https://github.com/treeverse/lakeFS/releases) you are upgrade from, if any requires that.

# Migrating

In case migration is required, stop the running lakeFS service.
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


