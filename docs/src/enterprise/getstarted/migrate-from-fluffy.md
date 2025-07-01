---
title: Migrate from Fluffy to lakeFS Enterprise
description: How to Migrate from a Fluffy-Based Enterprise Version to New lakeFS Enterprise
---

# Migrate From Fluffy to lakeFS Enterprise


The new lakeFS Enterprise integrates all enterprise features directly into a single binary, eliminating the need for the separate Fluffy service. This simplifies deployment, configuration, and maintenance.

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


!!! note "AWS"

    Structure remains the same.


#### Database Configuration

Use the same database configuration, but only in the lakeFS Enterprise config:

```yaml
database:
  type: postgres
  postgres:
    connection_string: "postgres://user:pass@host:5432/db?sslmode=disable"
```
