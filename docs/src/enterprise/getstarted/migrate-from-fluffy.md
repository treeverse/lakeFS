---
title: Migrate from Fluffy to lakeFS Enterprise
description: How to Migrate from a Fluffy-Based Enterprise Version to New lakeFS Enterprise
---

# Migrate From Fluffy to lakeFS Enterprise

The new lakeFS Enterprise integrates all enterprise features directly into a single binary, eliminating the need for the separate Fluffy service. This simplifies deployment, configuration, and maintenance.

To migrate from fluffy to lakeFS Enterprise, follow the steps below:

1. Make sure you have the **lakeFS Enterprise Docker token**, and a suitable **lakeFS Enterprise license** if not, [contact us](https://lakefs.io/contact-sales/) to gain access to lakeFS Enterprise. You will be granted a token that enables downloading *dockerhub/lakeFS-Enterprise* from [Docker Hub](https://hub.docker.com/u/treeverse), and a licence to run lakeFS Enterprise.
1. Update the lakeFS Enterprise Docker image to the latest enterprise version.
1. Sanity Test (Optional): Install a new test lakeFS Enterprise before moving your current production setup. Test the setup > login > Create repository etc. Once everything seems to work, delete and cleanup the test setup and we will move to the migration process.
1. Update configuration: Unlike lakeFS + Fluffy, lakeFS Enterprise uses only one configuration file. See [Configuration Changes](#configuration-changes), make sure to add the [license](#license-configuration) to the configuration.

!!! warning
    Please note that there will be a short downtime while replacing the lakeFS instances.

## Configuration Changes


### Authentication configuration

Most Fluffy `auth.*` settings migrate directly to lakeFS Enterprise with the same structure. Below are the differences between the configurations.

!!! note "SAML"
    
    === "Fluffy (old)"
        
        ```yaml
        # fluffy.yaml
        auth:
          logout_redirect_url: "https://auth0.com/v2/logout"
          post_login_redirect_url: http://localhost:8000/
          saml:
            enabled: true 
            external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name
        ```
    
    === "lakeFS Enterprise (new)"
     
        ```yaml
        # lakefs.yaml
        auth:
          logout_redirect_url: "https://auth0.com/v2/logout"
          cookie_auth_verification:
            external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name # This field was moved here!
          providers:
            saml:
			  post_login_redirect_url: http://localhost:8000/ # This field was moved here!
              # enabled: true  // This field was dropped! 
        ```


!!! note "OIDC"
   
    === "Fluffy (old)"
        
        ```yaml
        # fluffy.yaml
        auth:
          logout_redirect_url: "https://auth0.com/v2/logout"
          post_login_redirect_url: http://localhost:8000/
          oidc:
            enabled: true
        ```
    
    === "lakeFS Enterprise (new)"
        
        ```yaml
        # lakefs.yaml
        auth:
          logout_redirect_url: "https://auth0.com/v2/logout"
          oidc:
            post_login_redirect_url: http://localhost:8000/ # This field was moved here!
            # enabled: true  // This field was dropped! 
        ```


!!! note "OIDC"
   
    === "Fluffy (old)"
        
        ```yaml
        # fluffy.yaml
        auth:
          logout_redirect_url: "https://auth0.com/v2/logout"
          post_login_redirect_url: http://localhost:8000/
          oidc:
            enabled: true
        ```
    
    === "lakeFS Enterprise (new)"
        
        ```yaml
        # lakefs.yaml
        auth:
          logout_redirect_url: "https://auth0.com/v2/logout"
          oidc:
            post_login_redirect_url: http://localhost:8000/ # This field was moved here!
            # enabled: true  // This field was dropped! 
        ```

!!! note "AWS"
    The structure of AWS STS authentication remains the same.


### Database Configuration

Use the same database configuration, but only in the lakeFS Enterprise config:

```yaml
database:
  type: postgres
  postgres:
    connection_string: "postgres://user:pass@host:5432/db?sslmode=disable"
```

### License Configuration 

lakeFS Enterprise requires a license to run. 

```yaml
license:
  # use one of the two below. Using both results in an error.
  contents: <enterprise license>
  path: <path to license>
```