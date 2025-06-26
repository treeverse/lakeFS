---
title: Migrate from lakeFS OSS
description: How to migrate from lakeFS OSS to lakeFS Enterprise
---

# Migrate From lakeFS Open-Source to lakeFS Enterprise

To migrate from lakeFS Open Source to lakeFS Enterprise, follow the steps below:

1. Make sure you have the lakeFS Enterprise Docker token. if not, [contact us](https://lakefs.io/contact-sales/) to gain access to lakeFS Enterprise. You will be granted a token that enables downloading *dockerhub/lakeFS-Enterprise* from [Docker Hub](https://hub.docker.com/u/treeverse).
1. Update the lakeFS Docker image to the enterprise version. Replace `treeverse/lakefs` with `treeverse/lakefs-enterprise` in your configuration. The enterprise image can be pulled using your lakeFS Enterprise token.
1. Sanity Test (Optional): Install a new test lakeFS Enterprise before moving your current production setup. Test the setup > login > Create repository etc. Once everything seems to work, delete and cleanup the test setup and we will move to the migration process.
1. Follow lakeFS [Enterprise installation guide][lakefs-enterprise-install]
    1. Make sure that you meet the [prerequisites][lakefs-enterprise-install-prerequisites]
    1. Configure `features.local_rbac = true`
    1. Update your existing `values.yaml` file for your deployment
1. DB Migration: We are going to use the same database for lakeFS Enterprise, so we need to migrate the database schema.
1. Make sure to SSH / exec into the lakeFS server (old pre-upgrade version), the point is to use the same lakefs configuration file when running a migration.
    1. If upgrading `lakefs` version do this or skip to the next step: Install the new lakeFS binary, if not use the existing one (the one you are running).
    1. Run the command: `LAKEFS_AUTH_UI_CONFIG_RBAC=internal lakefs migrate up` (use the **new binary** if upgrading lakeFS version).
    1. You should expect to see a log message saying Migration completed successfully.
    1. During this short db migration process please make sure not to make any policy / RBAC related changes.
1. Once the migration completed - Upgrade your helm release with the modified `values.yaml` and the new version and run `helm ugprade`.
1. Login to the new lakeFS pod: Execute the following command, make sure you have proper credentials, or discard to get new ones:

    ```shell
    lakefs setup --user-name <admin> --access-key-id <key> --secret-access-key <secret> --no-check
    ```
!!! warning
    Please note that the newly set up lakeFS instance remains inaccessible to users until full setup completion, due to the absence of established credentials within the system.


[lakefs-enterprise-install]: install.md
[lakefs-enterprise-install-prerequisites]: install.md#prerequisites
