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
    
   ![Create Repository](../assets/img/create_repo_local.png)

   A [repository](../branching/model.md#repositories) is lakeFS's basic namespace, akin to S3's Bucket (read more about the data model [here](../branching/model.md)).
   Since we're using the `local` block adapter, the value used for `Storage Namespace` should be a static `local://`.
   For a deployment that uses S3 as a block adapter, this would be a bucket name with an optional prefix, e.g. `s3://my-bucket/prefix`.
   Notice that lakeFS will only manage the data written under that prefix. All actions on the managed data must go through lakeFS endpoint.
   Data written directly to the bucket under different paths will be accessible in the same way it was before.   
   {: .note .note-info }
   
### Next steps

You just created your first lakeFS repository! Go to [Copy Files](aws_cli.md) for adding data to your new repository.