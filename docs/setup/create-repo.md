---
layout: default
title: Create a Repository
description: This section outlines how to setup your environment once lakeFS is configured and running.
parent: Set up lakeFS
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

Note: If you already have lakeFS credentials, log in and skip to [creating the repository](#create-the-repository).
{: .note .pb-3 }

1. Open the lakeFS UI in your browser
   <span class="tooltip">(<a href="#">how?</a>)
     <span class="tooltiptext">
       Use an address pointing at your lakeFS server. Depending on how you deployed lakeFS, this can be a custom address (e.g., https://lakefs.example.com),
       the address of a load balancer, or something else.
     </span>
   </span>
   and choose a name for your admin user. 

   ![Setup]({{ site.baseurl }}/assets/img/setup.png){: style="padding:20px 40px"}


1. After clicking _Setup_, your lakeFS credentials will appear. Copy and store them somewhere safe, since you won't be able to see them again.

   ![Setup]({{ site.baseurl }}/assets/img/setup_done.png){: style="padding:20px 40px"}

1. Click the button to go to the login screen.

1. Use the credentials to login as an administrator.

## Create the repository

1. When logged in to lakeFS, click _Create Repository_.
    
1. In the shown dropdown, choose _Blank Repository_
   <span class="tooltip">(<a href="#">what are the other options?</a>)
     <span class="tooltiptext">
       The other options can help you integrate your existing tools with lakeFS.
     </span>
   </span>

   ![Create Repository]({{ site.baseurl }}/assets/img/create_repo_s3.png){: style="padding:20px 40px"}

1. Fill in a repository name.

1. Set the _Storage Namespace_ to a location in the bucket you've configured in a [previous step](./storage/index.md).
   The storage namespace is a location in the
   [underlying storage](../glossary.md#storage-namespace)
   where data for this repository will be stored.

   The storage namespace is a URI, and its scheme is determined by the storage type. For example, the storage namespace can be `s3://example-bucket/example-path/` if you're using AWS S3, or `gs://example-bucket/example-path` if you're using Google Cloud Storage.
   {: .note }
   
1. To finish creating the repository, click _Create Repository_.

# Next steps

You've just created your first lakeFS repository!

* You may now want to [import data](import.md) into your repository.
* Check out how lakeFS [easily integrates with your other tools](../integrations/index.md).
* [Join us on Slack](https://lakefs.io/slack){:target="_blank"} to introduce yourself, discover best practices and share your own!
