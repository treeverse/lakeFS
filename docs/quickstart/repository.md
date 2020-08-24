---
layout: default
title: Create Repository
parent: Quick Start
nav_order: 20
has_children: false
---

# Setting up a Repository
{: .no_toc }

1. Open [http://localhost:8000/setup](http://localhost:8000/setup){:target="_blank"} in your web browser to set up an initial admin user, used to login and send API requests.

   ![Setup](../assets/img/setup.png)

1. Follow the steps to create an initial administrator user. Save the credentials you've received somewhere safe, you won't be able to see them again!

   ![Setup Done](../assets/img/setup_done.png)

1. Follow the link and go to the login screen

   ![Login Screen](../assets/img/login.png)

1. Use the credentials from step #2 to login as an administrator
1. Click `Create Repository`
    
   ![Create Repository](../assets/img/repo_create.png)

   A [repository](../branching/model.md#repositories) is lakeFS's basic namespace, akin S3's Bucket. (Read more about the data model [here](../branching/model.md))
   Since we're using the `local` block adapter, the value used for `Storage Namespace` should be a static `local://`.
   For a real deployment that uses S3 as a block adapter, this would be a bucket name with optional prefix, i.e. `s3://my-bucket/prefix`. 
   {: .note .note-info }
   
### Next steps

You just created your first lakeFS repository! Go to [Copy Files](aws_cli.md) for adding data to your new repository.