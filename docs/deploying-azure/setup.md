---
layout: default
title: Setup
description: This section outlines how to setup your environment once lakeFS is configured and running
parent: Azure Deployment
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
    
   ![Create Repository](../assets/img/create_repo_azure.png)

   Under `Storage Namespace`, be sure to set the path to the container you've configured in a [previous step](./container.md).
   
   
# Next steps

After creating a repo, you can copy existing data from an Azure container to lakeFS using [DistCp](../using/distcp.md) or [Rclone](../using/rclone.md).

Check out the usage guides under [using lakeFS with...](https://docs.lakefs.io/using/) for other options.
