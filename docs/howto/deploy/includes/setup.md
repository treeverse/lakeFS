## Create the admin user

When you first open the lakeFS UI, you will be asked to create an initial admin user.

1. open `http://<lakefs-host>/` in your browser. If you haven't set up a load balancer, this will likely be `http://<instance ip address>:8000/`
1. On first use, you'll be redirected to the setup page:
   
   <img src="/assets/img/setup.png" alt="Create user">
   
1. Follow the steps to create an initial administrator user. Save the credentials you’ve received somewhere safe, you won’t be able to see them again!
   
   <img src="/assets/img/setup_done.png" alt="Setup Done">

1. Follow the link and go to the login screen. Use the credentials from the previous step to log in.

## Create your first repository

1. Use the credentials from the previous step to log in
1. Click *Create Repository* and choose *Blank Repository*.
   
   <img src="/assets/img/create-repo-no-sn.png" alt="Create Repo"/>
   
1. Under Storage Namespace, enter a path to your desired location on the object store. This is where data written to this repository will be stored.
1. Click *Create Repository*
1. You should now have a configured repository, ready to use!

   <img src="/assets/img/repo-created.png" alt="Repo Created" style="border: 1px solid #DDDDDD;"/>



**Congratulations!** Your environment is now ready 🤩
{: .note }