---
layout: default
title: Create a Repository
description: This section outlines how to setup your environment once lakeFS is configured and running.
parent: Setup lakeFS
nav_order: 10
has_children: false
redirect_from:
   - ../deploying-aws/setup.html
---

# Create a Repository

A _repository_ contains all of your objects, including the revision history.
It can be considered the lakeFS analog of a bucket in an object store. Since it has version control characteristics, it's also analogous to a repository in Git.

## Create the first user

When you first open the lakeFS UI, you will be asked to create an initial admin user.

1. In your browser, open the address of your lakeFS server.
   Depending on how you deployed lakeFS, this can be a custom address pointing at your server (e.g., https://lakefs.example.com),
   the address of a load balancer, or something else. You should see the following page, prompting you to set up an admin user.

   ![Setup]({{ site.baseurl }}/assets/img/setup.png)

   Note: If you already have lakeFS credentials, log in and skip to [creating the repository](#create-the-repository).
   {: .note .pb-3 }

1. Follow the steps to create an initial administrator user. Save the credentials you've received somewhere safe, you won't be able to see them again!

   ![Setup]({{ site.baseurl }}/assets/img/setup.png)

1. Follow the link and go to the login screen.

1. Use the credentials to login as an administrator.

   ![Login Screen]({{ site.baseurl }}/assets/img/login.png)

## Create the repository

1. Click _Create Repository_.
    
   ![Create Repository]({{ site.baseurl }}/assets/img/create_repo_s3.png)

1. Fill in a repository name.

1. Set the _Storage Namespace_ to a location in the bucket you've configured in a [previous step](./storage/index.md).
   The _storage namespace_ is a location in the
   [underlying storage](../understand/object-model.md#concepts-unique-to-lakefs)
   where data for this repository will be stored.

1. Click _Create Repository_.

# Next steps

You've just created your first lakeFS repository!

* You may now want to [import data](import.md) into your repository.
* Check out how lakeFS [easily integrates with your other tools](../integrations/index.md).
* [Join us on Slack](https://lakefs.io/slack){:target="_blank"} to introduce yourself, discover best practices and share your own!
