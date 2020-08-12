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
    
   ![Create Repository](../assets/img/repo_create.png)

   Under `Storage Namespace`, be sure to use the name of the bucket you've created in a [previous step](./bucket.md).
   
   
# Next steps

Check out the usage guides under [using lakeFS with...](../using) to start using lakeFS with your existing systems.
