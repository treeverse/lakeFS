---
title: Migrate from lakeFS OSS
description: How to migrate from lakeFS OSS to lakeFS Enterprise
parent: Get Started
grand_parent: lakeFS Enterprise
nav_order: 203
---

# Migrate From lakeFS Open-Source to lakeFS Enterprise

To migrate from lakeFS Open Source to lakeFS Enterprise, follow the steps below:

1. Make sure you have the Fluffy Docker token, if not [contact us](https://lakefs.io/contact-sales/) to gain access to Fluffy. You will be granted with a token that enables downloading *dockerhub/fluffy* from [Docker Hub](https://hub.docker.com/u/treeverse).
1. Sanity Test (Optional): Install a new test lakeFS Enterprise before moving your current production setup. Test the setup > login > Create repository etc. Once everything seems to work, delete and cleanup the test setup and we will move to the migration process.
1. Follow lakeFS [Enterprise installation guide][lakefs-enterprise-install]
   1. Make sure that you meet the [prerequisites][lakefs-enterprise-install-prerequisites]
   1. Update your existing `values.yaml` file for your deployment
1. DB Migration: we are going to use the same DB for both lakeFS and Fluffy, so we need to migrate the DB schema.
1. Make sure to SSH / exec into the lakeFS server (old pre-upgrade version), the point is to use the same lakefs configuration file when running a migration.
   1. If upgrading `lakefs` version do this or skip to the next step: Install the new lakeFS binary, if not use the existing one (the one you are running).
   1. Run the command: `LAKEFS_AUTH_UI_CONFIG_RBAC=internal lakefs migrate up` (use the **new binary** if upgrading lakeFS version).
   1. You should expect to see a log message saying Migration completed successfully.
   1. During this short db migration process please make sure not to make any policy / RBAC related changes.
1. Once the migration completed - Upgrade your helm release with the modified `values.yaml` and the new version and run `helm ugprade`.

[lakefs-enterprise-install]: {% link enterprise/getstarted/install.md %}
[lakefs-enterprise-install-prerequisites]: {% link enterprise/getstarted/install.md %}#prerequisites
