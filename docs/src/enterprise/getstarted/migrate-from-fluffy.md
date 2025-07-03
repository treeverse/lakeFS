---
title: Migrate from Fluffy to lakeFS Enterprise
description: How to Migrate from a Fluffy-Based Enterprise Version to New lakeFS Enterprise
---

# Migrate From Fluffy to lakeFS Enterprise

The new lakeFS Enterprise integrates all enterprise features directly into a single binary, eliminating the need for the separate Fluffy service. This simplifies deployment, configuration, and maintenance.

## Prerequisites

<!-- TODO: insert lakefs version  -->
1. You're using lakeFS enterprise binary or the image in Dockerhub treeverse/lakefs-enterprise with fluffy.
1. Your lakeFS-Enterprise version is >= x.y.z.
1. You possess a lakeFS Enterprise license.

!!! note
    [Contact us](https://lakefs.io/contact-sales/) to gain access to lakeFS Enterprise. You will be granted a token that enables downloading *dockerhub/lakeFS-Enterprise* 
    from [Docker Hub](https://hub.docker.com/u/treeverse), and a license to run lakeFS Enterprise.


To migrate from fluffy to lakeFS Enterprise, follow the steps below:

<!-- TODO add license cfg link -->
1. Sanity Test (Optional): Install a new test lakeFS Enterprise before moving your current production setup. **Make sure to include your lakeFS Enterprise license in the configuration before setup**. Test the setup → login → create repository, etc. Once everything seems to work, delete and cleanup the test setup and we will move to the migration process.
1. Update configuration: Unlike lakeFS + Fluffy, lakeFS Enterprise uses only one configuration file. See [Configuration Changes](#configuration-changes), make sure to add the license to the configuration.
1. Spin down lakeFS and fluffy, and run lakeFS Enterprise!

!!! warning
    Please note that there will be a short downtime while replacing the lakeFS instances.

## Configuration Changes


### Authentication configuration

Most Fluffy `auth.*` settings migrate directly to lakeFS Enterprise with the same structure. Below are the differences between the configurations.


!!! note "SAML"
    
    === "lakeFS & Fluffy (old)"
        
        ```yaml
        # fluffy.yaml
        auth:  
          logout_redirect_url: https://lakefs.company.com
          post_login_redirect_url: https://lakefs.company.com
          saml:
            enabled: true 
            sp_root_url: https://lakefs.company.com
            sp_x509_key_path: dummy_saml_rsa.key
            sp_x509_cert_path: dummy_saml_rsa.cert
            sp_sign_request: true
            sp_signature_method: http://www.w3.org/2001/04/xmldsig-more#rsa-sha256
            idp_metadata_url: https://my.saml-provider.com/federationmetadata/2007-06/federationmetadata.xml
            # idp_authn_name_id_format: "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
            external_user_id_claim_name: samName
            # idp_metadata_file_path: 
            # idp_skip_verify_tls_cert: true
		```
        ```yaml
        # lakefs.yaml
        auth:
          logout_redirect_url: https://lakefs.company.com
          cookie_auth_verification:
            auth_source: saml
            friendly_name_claim_name: displayName
            persist_friendly_name: true
            external_user_id_claim_name: samName
            validate_id_token_claims:
              department: r_n_d
            default_initial_groups:
            - "Developers"
        ui_config:
          login_url: https://lakefs.company.com/sso/login-saml
          logout_url: https://lakefs.company.com/sso/logout-saml
          login_cookie_names:
          - internal_auth_session
          - saml_auth_session
		```

    
    === "lakeFS Enterprise (new)"
     
        ```yaml
        # lakefs.yaml
        auth:
          logout_redirect_url: https://lakefs.company.com/ # optional, URL to redirect to after logout
          cookie_auth_verification:
            auth_source: saml
            friendly_name_claim_name: displayName
            default_initial_groups: ["Admins"]
            external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name 
            validate_id_token_claims:
              department: r_n_d
          providers:
            saml:
              # enabled: true  # This field was dropped! 
              sp_root_url: https://lakefs.company.com
              sp_x509_key_path: dummy_saml_rsa.key
              sp_x509_cert_path: dummy_saml_rsa.cert
              sp_sign_request: true
              sp_signature_method: http://www.w3.org/2001/04/xmldsig-more#rsa-sha256
              idp_metadata_url: https://my.saml-provider.com/federationmetadata/2007-06/federationmetadata.xml
              post_login_redirect_url: / # Where to redirect after successful SAML login
              # external_user_id_claim_name: # This field was moved to auth.cookie_auth_verification
          ui_config:
            login_url: https://lakefs.company.com/sso/login-saml
            logout_url: https://lakefs.company.com/sso/logout-saml
            login_cookie_names:
              - internal_auth_session
              - saml_auth_session
        ```


!!! note "OIDC + OIDC STS"
   
    === "lakeFS + Fluffy (old)"
        
        ```yaml
        # fluffy.yaml
        auth:
          post_login_redirect_url: /
          logout_redirect_url: https://oidc-provider-url.com/logout/url
          oidc:
            enabled: true
            url: https://oidc-provider-url.com/
            client_id: <oidc-client-id>
            client_secret: <oidc-client-secret>
            callback_base_url: https://lakefs.company.com
            is_default_login: true
            logout_client_id_query_parameter: client_id
            logout_endpoint_query_parameters:
            - returnTo 
            - https://lakefs.company.com/oidc/login
        ```
    
        ```yaml
        # lakefs.yaml
        auth:
          oidc:
            friendly_name_claim_name: "name"
            persist_friendly_name: true
            default_initial_groups: ["Developers"]
          ui_config:
            login_url: /oidc/login
            logout_url: /oidc/logout
            login_cookie_names:
            - internal_auth_session
            - oidc_auth_session
        ```

    === "lakeFS Enterprise (new)"
        
        ```yaml
        # lakefs.yaml
        auth:
          logout_redirect_url:  https://oidc-provider-url.com/logout/url # optional, URL to redirect to after logout 
          ui_config:
            login_url: /oidc/login
            logout_url: /oidc/logout
            login_cookie_names:
              - internal_auth_session
              - oidc_auth_session
          oidc:
            friendly_name_claim_name: "nickname"
            default_initial_groups: ["Admins"]
          providers:
            oidc:
              # enabled: true  # This field was dropped!
              post_login_redirect_url: / # This field was moved here!
              url: https://oidc-provider-url.com/
              client_id: <oidc-client-id>
              client_secret: <oidc-client-secret>
              callback_base_url: https://lakefs.company.com
              logout_client_id_query_parameter: client_id
              logout_endpoint_query_parameters:
                - returnTo
                - http://lakefs.company.com/oidc/login
        ```


!!! note "LDAP"
   
    === "lakeFS + Fluffy (old)"
        
        ```yaml
        # fluffy.yaml
        auth:
          post_login_redirect_url: /
          ldap: 
            server_endpoint: ldaps://ldap.company.com:636
            bind_dn: uid=<bind-user-name>,ou=<some-ou>,o=<org-id>,dc=<company>,dc=com
            bind_password: '<ldap password>'
            username_attribute: uid
            user_base_dn: ou=<some-ou>,o=<org-id>,dc=<company>,dc=com
            user_filter: (objectClass=inetOrgPerson)
            connection_timeout_seconds: 15
            request_timeout_seconds: 7
	    ```
        ```yaml
    	# lakefs.yaml
        auth:
          remote_authenticator:
            enabled: true
            endpoint: http://<Fluffy URL>:<Fluffy http port>/api/v1/ldap/login
            default_user_group: "Developers" # Value needs to correspond with an existing group in lakeFS
          ui_config:
            logout_url: /logout
            login_cookie_names:
            - internal_auth_session
        ```
    
    === "lakeFS Enterprise (new)"
        
        ```yaml
        # lakefs.yaml
        auth:
          ui_config:
            logout_url: /logout
            login_cookie_names:
              - internal_auth_session
          providers:
            ldap:
              server_endpoint: ldaps://ldap.company.com:636
              bind_dn: uid=<bind-user-name>,ou=<some-ou>,o=<org-id>,dc=<company>,dc=com
              bind_password: '<ldap password>'
              username_attribute: uid 
              user_base_dn: ou=<some-ou>,o=<org-id>,dc=<company>,dc=com 
              user_filter: (objectClass=inetOrgPerson)
              connection_timeout_seconds: 15
              request_timeout_seconds: 7
              default_user_group: "Developers" # This field moved here!
        ```

!!! note "AWS IAM"
   
    === "lakeFS + Fluffy (old)"
        
        ```yaml
        # fluffy.yaml
        serve_listen: "localhost:9001"
        auth:
          external:
            aws_auth:
              enabled: true
              required_headers:
                X-LakeFS-Server-ID: "localhost"
        ```
        ```yaml
        # lakefs.yaml
        auth:
          authentication_api:
            endpoint: http://localhost:9001/api/v1
            external_principals_enabled: true
        ```
    
    === "lakeFS Enterprise (new)"
        
        ```yaml
        # lakefs.yaml
        auth:
          external_aws_auth:
            enabled: true
            required_headers:
              X-LakeFS-Server-ID: "localhost"

        ```


### Authorization configuration

!!! note "RBAC"
   
    === "lakeFS + Fluffy (old)"
        
        ```yaml
        # fluffy.yaml
        auth:
          serve_listen_address: "localhost:9000"
          cache:
            enabled: true
        ```
        
        ```yaml
        # lakefs.yaml
        auth:
          api:
           endpoint: http://localhost:9000/api/v1
        ```
    
    === "lakeFS Enterprise (new)"
        
        ```yaml
        # lakefs.yaml
        auth:
          # serve_disable_authentication: false      # this field was dropped!
          # serve_listen_address: "localhost:9000"   # this field was dropped!
          # api:                                     # this field was dropped! 
          #   endpoint: http://localhost:9000/api/v1 # this field was dropped!
          cache:
            enabled: true
        ```
