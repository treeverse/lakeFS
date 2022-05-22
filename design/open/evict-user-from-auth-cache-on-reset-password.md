## The problem: 
When a user logs in to lakeFS we save his credentials in the auth-cache. 
This way, we avoid checking the usr credentials through the DB on each call the user makes. 
Currently, when a user tries to reset his password, his old password is still stored in the auth-cache, so he has to wait till it is evicted to log in using the new password.


## The suggested solution: 
The intuitive solution is just to evict the user from the auth cache when resetting his password. 
The problem with this solution is that we might not necessarily have a centralized cache, so we might need to evict the user for every auth cache he exists in. 
This is why the simpler solution here might be just to not use auth cache for email-password authentication. 
This way, we still won't need to authenticate when a user uses API calls that use access-key and secret for authentication and won't need to do the effort of evicting the user auth from all the auth cache instances.
In case we decide to go with this solution, we have to enable email/password authentication only through the UI.
Otherwise, user might try to use email/password authentication with our Python/Java client and won't have their credentials cached.