## The problem: 
When a user logs in to lakeFS we save their credentials in the auth-cache. 
This way, we avoid checking the user credentials through the DB on each call the user make.
Currently, when users try to reset their password, their old password is still stored in the auth-cache, so they have to wait till it's evicted before they log in using the new password.


## The suggested solution: 
The intuitive solution is just to evict the users from the auth cache when resetting their password. 
The problem with this solution is that we might not necessarily have a centralized cache, so we might need to evict the user from every auth cache he exists in. 
This is why the simpler solution here might be just to not use auth cache for email-password authentication. 
This way, we still won't need to authenticate when users use API calls that use access-key and secret for authentication, and we won't need to do the effort of evicting the user auth from all the auth cache instances.
