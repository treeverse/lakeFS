---
layout: default
title: Setup
parent: AWS Deployment
nav_order: 27
has_children: false
---

# Setup

Once we have lakeFS configured and running, open `https://<OPENAPI_SERVER_ENDPOINT>/setup` (e.g. [https://lakefs.example.com](https://lakefs.example.com){: target="_blank" }).

1. Follow the steps to create an initial administrator user. Save the credentials you've received somewhere safe, you won't be able to see them again!

   ![Setup](../assets/img/setup_done.png)

2. Follow the link and go to the login screen

   ![Login Screen](../assets/img/login.png)

3. Use the credentials from step #1 to login as an administrator
4. Click `Create Repository`
    
   ![Create Repository](../assets/img/create_repo_s3.png)

   Under `Storage Namespace`, be sure to set the path to the bucket you've configured in a [previous step](./bucket.md).
   
   
# Next steps

After creating a repo, you can import your existing data into it. lakeFS offers an [Import API](../reference/import.md) to bring your data without copying it.
Alternatively, if you wish to copy existing data from an S3 bucket to lakeFS, use [DistCp](../using/distcp.md) or [Rclone](../using/rclone.md).

Check out the usage guides under [using lakeFS with...](https://docs.lakefs.io/using/) for other options.
