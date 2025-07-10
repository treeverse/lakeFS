---
title: Upgrade
description: How to upgrade lakeFS Enterprise
---

# Upgrade

For upgrading from lakeFS enterprise to a newer version see [lakefs migration](../howto/deploy/upgrade.md).


## Migrate From Fluffy to lakeFS Enterprise

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

## Migrating from Fluffy to lakeFS Enterprise Authentication

### Overview

Starting with **lakeFS Helm chart version 1.5.0**, the Fluffy authentication service has been deprecated and replaced with native lakeFS Enterprise authentication. This migration consolidates authentication into the main lakeFS application, simplifying deployment and maintenance.

#### What's Changing

When you upgrade to lakeFS Enterprise:

- **Fluffy Deployment Removed**: The separate Fluffy deployment, service, and associated Kubernetes resources are no longer needed
- **Simplified Architecture**: Authentication is now handled directly by lakeFS Enterprise, reducing the number of pods and services
- **Streamlined Ingress**: No more routing between Fluffy and lakeFS - all traffic goes directly to lakeFS
- **Updated values.yaml Structure**: Authentication configuration moves from `fluffy.*` to `enterprise.auth.*` and `lakefsConfig.auth.providers.*`
- **Enterprise Image Required**: The chart now uses `treeverse/lakefs-enterprise` image (minimum version 1.0.0)

#### Prerequisites

- Current lakeFS deployment using Fluffy authentication (chart version < 1.5.0)
- Access to update Helm values
- lakeFS Enterprise Docker Hub token
- Backup of your current values.yaml

### Step-by-Step Migration Guide

#### Step 1: Update Helm Repository

```bash
helm repo update lakefs
```

Verify you have access to chart version 1.5.0 or later:
```bash
helm search repo lakefs/lakefs --versions
```

#### Step 2: Review New Chart Values

Examine all available configuration options in the new chart:
```bash
helm show values lakefs/lakefs --version 1.5.0 > new-values-reference.yaml
```

#### Step 3: Update Your Image Configuration

If you're overriding the image in your values.yaml, update it to use lakeFS Enterprise:

```yaml
image:
  repository: treeverse/lakefs-enterprise
  tag: 1.0.0  # or later
  privateRegistry:
    enabled: true
    secretToken: <your-dockerhub-token>
```

**Note**: If you're not overriding the image, the chart will automatically use the correct Enterprise image.

#### Step 4: Migrate Your Authentication Configuration

Using the configuration examples below, update your values.yaml file:
1. Remove all `fluffy.*` configuration sections
2. Add the new `enterprise.auth.*` configuration for your authentication method
3. Move authentication settings to `lakefsConfig.auth.providers.*`

Refer to the complete examples in the [lakeFS Helm chart repository](https://github.com/treeverse/charts/tree/master/examples/lakefs/enterprise).

#### Step 5: Validate with Dry Run

Before applying changes, validate your configuration:
```bash
helm upgrade <release-name> lakefs/lakefs \
  --version 1.5.0 \
  --namespace <namespace> \
  --values <your-updated-values.yaml> \
  --dry-run
```

Review the output to ensure:
- No Fluffy resources are being created
- lakeFS Enterprise deployment is configured correctly
- Ingress configuration is simplified

#### Step 6: Perform the Upgrade

Once validated, perform the actual upgrade:
```bash
helm upgrade <release-name> lakefs/lakefs \
  --version 1.5.0 \
  --namespace <namespace> \
  --values <your-updated-values.yaml>
```

#### Step 7: Verify the Migration

After the upgrade completes:

1. **Check Pod Status**:
   ```bash
   kubectl get pods -n <namespace>
   # Fluffy pods should no longer exist
   ```

2. **Verify lakeFS Health**:
   ```bash
   kubectl exec -n <namespace> <lakefs-pod> -- curl http://localhost:8000/_health
   ```

3. **Check Logs**:
   ```bash
   kubectl logs -n <namespace> <lakefs-pod>
   # Look for successful authentication provider initialization
   ```

4. **Test Authentication**:
    - Navigate to your lakeFS URL
    - Verify SSO login works correctly
    - Confirm RBAC permissions are preserved

5. **Verify Fluffy Resources Removed**:
   ```bash
   kubectl get all -n <namespace> | grep fluffy
   # Should return no results
   ```

#### Step 8: Rollback (if needed)

If you encounter issues, rollback to the previous version:
```bash
# Find the previous revision
helm history <release-name> -n <namespace>

# Rollback to previous revision
helm rollback <release-name> <previous-revision> -n <namespace>
```

### Configuration Examples

Below are complete configuration examples for each authentication method, showing both the old (Fluffy) and new (Enterprise) configurations:

!!! note "SAML with Helm"

    === "lakeFS + Fluffy (old)"
        
        ```yaml
        ingress:
          enabled: true
          ingressClassName: <class-name>
          hosts:
            # the ingress that will be created for lakeFS
            - host: <lakefs.ingress.domain>
              paths: 
                - /

        fluffy:
          enabled: true
          image:
            privateRegistry:
              enabled: true
              secretToken: <dockerhub-token-fluffy-image>
          fluffyConfig: |
            auth:  
              # logout_redirect_url: https://<lakefs.ingress.domain>
              # post_login_redirect_url: https://<lakefs.ingress.domain>
              saml:
                sp_sign_request: true
                # depends on IDP
                sp_signature_method: "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
                # url to the metadata of the IDP
                idp_metadata_url: "https://<adfs-auth.company.com>/federationmetadata/2007-06/federationmetadata.xml"
                # IDP SAML claims format default unspecified
                # idp_authn_name_id_format: "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
                # claim name from IDP to use as the unique user name
                external_user_id_claim_name: samName
                # depending on IDP setup, if CA certs are self signed and not trusted by a known CA
                idp_skip_verify_tls_cert: true
          rbac:
            enabled: true
          secrets:
            create: true
          sso:
            enabled: true
            saml:
              enabled: true
              createSecret: true
              lakeFSServiceProviderIngress: https://<lakefs.ingress.domain>
              certificate:
                saml_rsa_public_cert: |
                  -----BEGIN CERTIFICATE-----
                  ...
                  -----END CERTIFICATE-----
                saml_rsa_private_key: |
                  -----BEGIN PRIVATE KEY-----
                  ...
                  -----END PRIVATE KEY-----

        lakefsConfig: | 
          blockstore:
            type: local
          auth:
            cookie_auth_verification:
            # claim name to display user in the UI
              friendly_name_claim_name: displayName
              # claim name from IDP to use as the unique user name
              external_user_id_claim_name: samName
              default_initial_groups:
                - "Developers"
            ui_config:
              login_cookie_names:
                - internal_auth_session
                - saml_auth_session
        ```

    === "lakeFS Enterprise (new)"
        
        ```yaml
        ingress:
          enabled: true
          ingressClassName: <class-name>
          hosts:
            # the ingress that will be created for lakeFS
            - host: <lakefs.ingress.domain>
              paths: 
                - /

        enterprise:
          enabled: true
          auth:
            saml:
              enabled: true
              createCertificateSecret: true
              certificate:
                samlRsaPublicCert: |
                  -----BEGIN CERTIFICATE-----
                  ...
                  -----END CERTIFICATE-----
                samlRsaPrivateKey: |
                  -----BEGIN PRIVATE KEY-----
                  ...
                  -----END PRIVATE KEY-----

        image:
          privateRegistry:
            enabled: true
            secretToken: <dockerhub-token>

        lakefsConfig: |
          blockstore:
            type: local
          features:
            local_rbac: true
          auth:
            logout_redirect_url: https://<lakefs.ingress.domain>
            cookie_auth_verification:
              auth_source: saml
              # claim name to display user in the UI
              friendly_name_claim_name: displayName
              # claim name from IDP to use as the unique user name
              external_user_id_claim_name: samName
              default_initial_groups:
                - "Developers"
            providers:
              saml:
                post_login_redirect_url: https://<lakefs.ingress.domain>
                sp_root_url: https://<lakefs.ingress.domain>
                sp_sign_request: true
                # depends on IDP
                sp_signature_method: "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
                # url to the metadata of the IDP
                idp_metadata_url: "https://<adfs-auth.company.com>/federationmetadata/2007-06/federationmetadata.xml"
                # IDP SAML claims format default unspecified
                idp_authn_name_id_format: "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
                # depending on IDP setup, if CA certs are self signed and not trusted by a known CA
                #idp_skip_verify_tls_cert: true
        ```

!!! note "OIDC with Helm"

    === "lakeFS + Fluffy (old)"
        
        ```yaml
        ingress:
          enabled: true
          ingressClassName: <class-name>
          hosts:
            # the ingress that will be created for lakeFS
            - host: <lakefs.ingress.domain>
              paths: 
                - /

        fluffy:
          enabled: true
          image:
            privateRegistry:
              enabled: true
              secretToken: <dockerhub-token-fluffy-image>
          fluffyConfig: |
            auth:
              logout_redirect_url: https://oidc-provider-url.com/logout/example
              oidc:
                enabled: true
                url: https://oidc-provider-url.com/
                client_id: <oidc-client-id>
                callback_base_url: https://<lakefs.ingress.domain>
                # the claim name that represents the client identifier in the OIDC provider (e.g Okta)
                logout_client_id_query_parameter: client_id
                # the query parameters that will be used to redirect the user to the OIDC provider (e.g Okta) after logout
                logout_endpoint_query_parameters:
                  - returnTo
                  - https://<lakefs.ingress.domain>/oidc/login
          secrets:
            create: true
          sso:
            enabled: true
            oidc:
              enabled: true
              # secret given by the OIDC provider (e.g auth0, Okta, etc)
              client_secret: <oidc-client-secret>
          rbac:
            enabled: true

        lakefsConfig: |
          database:
            type: local
          blockstore:
            type: local
          features:
            local_rbac: true
          auth:
            ui_config:
              login_cookie_names:
                - internal_auth_session
                - oidc_auth_session
            oidc:
              friendly_name_claim_name: <some-oidc-provider-claim-name>
              default_initial_groups: ["Developers"]
        ```

    === "lakeFS Enterprise (new)"
        
        ```yaml
        ingress:
          enabled: true
          ingressClassName: <class-name>
          hosts:
            # the ingress that will be created for lakeFS
            - host: <lakefs.ingress.domain>
              paths: 
                - /

        enterprise:
          enabled: true
          auth:
            oidc:
              enabled: true
              # secret given by the OIDC provider (e.g auth0, Okta, etc)
              client_secret: <oidc-client-secret>

        image:
          privateRegistry:
            enabled: true
            secretToken: <dockerhub-token>

        lakefsConfig: |
          blockstore:
            type: local
          features:
            local_rbac: true
          auth:
            logout_redirect_url: https://oidc-provider-url.com/logout/example
            oidc:
              friendly_name_claim_name: <some-oidc-provider-claim-name>
              default_initial_groups: ["Developers"]
            providers:
              oidc:
                post_login_redirect_url: /
                url: https://oidc-provider-url.com/
                client_id: <oidc-client-id>
                callback_base_url: https://<lakefs.ingress.domain>
                # the claim name that represents the client identifier in the OIDC provider (e.g Okta)
                logout_client_id_query_parameter: client_id
                # the query parameters that will be used to redirect the user to the OIDC provider (e.g Okta) after logout
                logout_endpoint_query_parameters:
                  - returnTo
                  - https://<lakefs.ingress.domain>/oidc/login
        ```

!!! note "LDAP with Helm"

    === "lakeFS + Fluffy (old)"
        
        ```yaml
        ingress:
          enabled: true
          ingressClassName: <class-name>
          hosts:
            # the ingress that will be created for lakeFS
            - host: <lakefs.ingress.domain>
              paths: 
               - /

        fluffy:
          enabled: true
          image:
            privateRegistry:
              enabled: true
              secretToken: <dockerhub-token-fluffy-image>
          fluffyConfig: |
            auth:
              post_login_redirect_url: /
              ldap: 
                server_endpoint: ldaps://ldap.company.com:636
                bind_dn: uid=<bind-user-name>,ou=Users,o=<org-id>,dc=<company>,dc=com
                username_attribute: uid
                user_base_dn: ou=Users,o=<org-id>,dc=<company>,dc=com
                user_filter: (objectClass=inetOrgPerson)
                connection_timeout_seconds: 15
                request_timeout_seconds: 7

          secrets:
            create: true
            
          sso:
            enabled: true
            ldap:
              enabled: true
              bind_password: <ldap bind password>
          rbac:
            enabled: true

        lakefsConfig: |
          blockstore:
            type: local
          auth:
            remote_authenticator:
              enabled: true
              default_user_group: "Developers"
            ui_config:
              login_cookie_names:
                - internal_auth_session
        ```

    === "lakeFS Enterprise (new)"
        
        ```yaml
        ingress:
          enabled: true
          ingressClassName: <class-name>
          hosts:
            # the ingress that will be created for lakeFS
            - host: <lakefs.ingress.domain>
              paths:
                - /

        enterprise:
          enabled: true
          auth:
            ldap:
              enabled: true
              bindPassword: <ldap bind password>

        image:
          privateRegistry:
            enabled: true
            secretToken: <dockerhub-token>

        lakefsConfig: |
          blockstore:
            type: local
          features:
            local_rbac: true
          auth:
            ui_config:
              login_cookie_names:
                - internal_auth_session
            providers:
              ldap:
                server_endpoint: ldaps://ldap.company.com:636
                bind_dn: uid=<bind-user-name>,ou=Users,o=<org-id>,dc=<company>,dc=com        
                username_attribute: uid
                user_base_dn: ou=Users,o=<org-id>,dc=<company>,dc=com        
                user_filter: (objectClass=inetOrgPerson)
                default_user_group: "Developers"
                connection_timeout_seconds: 15
                request_timeout_seconds: 7
        ```

!!! note "AWS IAM with Helm"

    === "lakeFS + Fluffy (old)"
        
        ```yaml
        lakefsConfig: |
          auth:
            authentication_api:
              external_principals_enabled: true
        ingress:
          enabled: true
          ingressClassName: <class-name>
          hosts:
            # the ingress that will be created for lakeFS
            - host: <lakefs.ingress.domain>
              paths:
                - /

        fluffy:
          enabled: true
          image:
            repository: treeverse/fluffy
            pullPolicy: IfNotPresent
            privateRegistry:
              enabled: true
              secretToken: <dockerhub-token-fluffy-image>
          fluffyConfig: |
            auth:
              external:
                aws_auth:
                  enabled: true
                  # the maximum age in seconds for the GetCallerIdentity request
                  #get_caller_identity_max_age: 60
                  # headers that must be present by the client when doing login request
                  required_headers:
                    # same host as the lakeFS server ingress
                    X-LakeFS-Server-ID: <lakefs.ingress.domain>
          secrets:
            create: true
          sso:
            enabled: true
          rbac:
            enabled: true
        ```

    === "lakeFS Enterprise (new)"
        
        ```yaml
        ingress:
          enabled: true
          ingressClassName: <class-name>
          hosts:
            # the ingress that will be created for lakeFS
            - host: <lakefs.ingress.domain>
              paths:
                - /

        lakefsConfig: |
          auth:
            external_aws_auth:
              enabled: true
              # the maximum age in seconds for the GetCallerIdentity request
              #get_caller_identity_max_age: 60
              # headers that must be present by the client when doing login request
              required_headers:
                # same host as the lakeFS server ingress
                X-LakeFS-Server-ID: <lakefs.ingress.domain>
        ```

### Important Notes

* Complete configuration examples for each authentication method are available in the [lakeFS Helm chart repository](https://github.com/treeverse/charts/tree/master/examples/lakefs/enterprise)
* The examples include local blockstore for quick-start - replace with S3/Azure/GCS for production deployments
* Minimum lakeFS Enterprise version: 1.0.0
* Configure the `image.privateRegistry.secretToken` with your DockerHub token for accessing enterprise images
* Update all placeholder values (marked with `<>`) with your actual configuration
* Enable `features.local_rbac: true` to use the enterprise RBAC features
* The simplified architecture reduces operational complexity and improves performance

### Troubleshooting

If you encounter issues during migration:

1. **Authentication Failures**: Check that all authentication settings have been properly moved to the new configuration structure
2. **Missing Permissions**: Verify that `features.local_rbac: true` is set in your lakefsConfig
3. **Image Pull Errors**: Ensure your DockerHub token has access to the lakeFS Enterprise image
4. **Ingress Issues**: Confirm that your ingress is pointing directly to lakeFS (not Fluffy)

For additional support, consult the [lakeFS documentation](https://docs.lakefs.io) or contact lakeFS support.