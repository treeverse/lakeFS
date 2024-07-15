---
title: Migrate from lakeFS OSS
description: How to migrate from lakeFS OSS to lakeFS Enterprise
parent: Get Started
grand_parent: lakeFS Enterprise
nav_order: 203
---

# Migrate From lakeFS Open-Source to lakeFS Enterprise

For upgrading from lakeFS enterprise to a newer version see [lakefs migration](https://docs.lakefs.io/howto/deploy/upgrade.html).

**To move from lakeFS Open Source to lakeFS Enterprise, follow the steps below:**

1. Sanity Test: Install fresh lakeFS enterprise: Test the setup > login > Create repository etc. Once everything seems to work delete and clean up, let's get to the real thing.
1. DB Migration: we are going to use the same DB for both lakeFS and Fluffy, so we need to migrate the DB schema.
1. Make sure to SSH / exec into the lakeFS server (old pre-upgrade version), the point is to use the same lakefs confugration file when running a migration.
   1. If upgrading `lakefs` version do this or skip to the next step: Install the new lakeFS binary, if not use the existing one (the one you are running).
   1. Run the command: `LAKEFS_AUTH_UI_CONFIG_RBAC=internal lakefs migrate up` (use the **new binary** if upgrading lakeFS version).
   1. You should expect to see a log message saying Migration completed successfully.
   1. During this short db migration process please make sure not to make any policy / RBAC related changes.
1. Once the migration completed - Upgrade your helm release to the new version and run `helm ugprade`, that’s it.