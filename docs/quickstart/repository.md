---
layout: default
title: Create a Repository
description: In this guide, we’re going to run an initial setup and then create a new repository using lakeFS.
parent: Quickstart
nav_order: 20
has_children: false
next: ["Add data", "./add_data.html"]
---

# Create a Repository
{: .no_toc }

A _repository_ contains all of your objects, including the revision history.
You can consider it as the lakeFS analog of a bucket in an object store. Since it has version control qualities, it is also analogous to a repository in Git.

## Create the first user

When you first open the lakeFS UI, you will be asked to create an initial admin user.

1. Open [http://127.0.0.1:8000/](http://127.0.0.1:8000/){:target="_blank"} in your web browser.

   ![Setup]({{ site.baseurl }}/assets/img/setup.png)

1. Follow the steps to create an initial administrator user. Save the credentials you've received somewhere safe, you won't be able to see them again!

   ![Setup Done]({{ site.baseurl }}/assets/img/setup_done.png)

1. Follow the link and go to the login screen:

   ![Login Screen]({{ site.baseurl }}/assets/img/login.png)

## Create the repository 

1. Use the credentials from the previous step to log in as an administrator.

1. Click _Create Repository_.
    
   ![Create Repository]({{ site.baseurl }}/assets/img/create_repo_local.png)

1. Fill in the repository name.

1. Under _Storage Namespace_, enter `local://`.
 
   In this tutorial, the underlying storage for lakeFS is the local disk. Accordingly, the value for _Storage Namespace_ should simply be `local://`.
   For a deployment that uses an object store as the underlying storage, this would be a location in the store - for example, `s3://example-bucket/prefix`.
   {: .note .note-info }

1. Click _Create Repository_.

### Next steps

You've just created your first lakeFS repository! You can now [add some data](add_data.md) to it.
