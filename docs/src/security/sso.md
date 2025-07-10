---
title: Single Sign On (SSO)
description: How to configure Single Sign On (SSO) for lakeFS Cloud and lakeFS Enterprise.
status: enterprise
search:
  boost: 2
---

# Single Sign On (SSO)

!!! info
    Available in **lakeFS Cloud** and **lakeFS Enterprise**<br/>
    If you're using the open-source version of lakeFS you can read more about the [authentication options available](./authentication.md).

## SSO for lakeFS Cloud

lakeFS Cloud uses Auth0 for authentication and thus supports the same identity providers as Auth0 including Active Directory/LDAP, ADFS, Azure Active Directory Native, Google Workspace, OpenID Connect, Okta, PingFederate, SAML, and Azure Active Directory.

=== "Okta"

    !!! note
        This guide is based on [Okta's Create OIDC app integrations guide](https://help.okta.com/en-us/Content/Topics/Apps/Apps_App_Integration_Wizard_OIDC.htm).

    **Steps:**

    1. Login to your Okta account
    2. Select **Applications > Applications**, then **Create App Integration**.
    3. Select Create New App and enter the following:
        1. For **Sign-in method**, choose OIDC.
        2. Under **Application type**, choose **Web app**.
        3. Select Next.
    4. Under General Settings:
        1. **App integration name**, enter a name for your application. (i.e lakeFS Cloud)
    5. In the **Sign-in redirect URIs** field, enter https://lakefs-cloud.us.auth0.com/login (United States) or https://lakefs-cloud.eu.auth0.com/login (Europe).
    6. Under **Sign-in redirect URIs**, click **Add URI**, enter https://lakefs-cloud.us.auth0.com/login/callback (United States) or https://lakefs-cloud.eu.auth0.com/login/callback (Europe).
    7. Under **Assignments**, choose the wanted **Controlled access**. (i.e Allow everyone in your organization to access)
    8. Uncheck **Enable immediate access with Federation Broker Mode**.
    9. Select **Save**.

    Once you finish registering your application with Okta, save the **Client ID**, **Client Secret** and your **Okta Domain**, send this to Treeverse's team to finish the integration.

=== "Active Directory Federation Services (AD FS)"

    **Prerequisites:**

    * Client's AD FS server should be exposed publicly or to Auth0's IP ranges (either directly or using Web Application Proxy)

    **Steps:**

    1. Connect to the AD FS server
    2. Open AD FS' PowerShell CLI as Administrator through the server manager
    3. Execute the following:
        ```sh
        (new-object Net.WebClient -property @{Encoding = [Text.Encoding]::UTF8}).DownloadString("https://raw.github.com/auth0/adfs-auth0/master/adfs.ps1") | iex

        AddRelyingParty "urn:auth0:lakefs-cloud" "https://lakefs-cloud.us.auth0.com/login/callback"
        ```

        !!! note
            If your organization data is located in Europe, use `lakefs-cloud.eu.auth0.com` instead of `lakefs-cloud.us.auth0.com`.

    Once you finish registering lakeFS Cloud with AD FS, save the **AD FS URL** and send this to Treeverse's team to finish the integration.

=== "Azure Active Directory (AD)"

    **Prerequisites:**

    * Azure account with permissions to manage applications in Azure Active Directory

    !!! note
        If you've already set up lakeFS Cloud with your Azure account, you can skip the [Register lakeFS Cloud with Azure](#register-lakefs-cloud-with-azure) and [Add client secret](#add-a-secret) and go directly to [Add a redirect URI](#add-a-redirect-uri).

    **Register lakeFS Cloud with Azure**

    **Steps:**

    1. Sign in to the Azure portal.
    2. If you have access to multiple tenants, use the Directories + subscriptions filter in the top menu to switch to the tenant in which you want to register the application.
    3. Search for and select Azure Active Directory.
    4. Under Manage, select App registrations > New registration.
    5. Enter a display Name for your application. Users of your application might see the display name when they use the app, for example during sign-in. You can change the display name at any time and multiple app registrations can share the same name. The app registration's automatically generated Application (client) ID, not its display name, uniquely identifies your app within the identity platform.
    6. Specify who can use the application, sometimes called its sign-in audience.
        
        !!! note
            don't enter anything for Redirect URI (optional). You'll configure a redirect URI in the next section.
        
    7. Select Register to complete the initial app registration.

    When registration finishes, the Azure portal displays the app registration's Overview pane. You see the Application (client) ID. Also called the client ID, this value uniquely identifies your application in the Microsoft identity platform.

    Important: new app registrations are hidden to users by default. When you are ready for users to see the app on their My Apps page you can enable it. To enable the app, in the Azure portal navigate to Azure Active Directory > Enterprise applications and select the app. Then on the Properties page toggle Visible to users? to Yes.

    **Add a secret**

    Sometimes called an application password, a client secret is a string value your app can use in place of a certificate to identity itself.

    **Steps:**

    1. In the Azure portal, in App registrations, select your application.
    2. Select Certificates & secrets > Client secrets > New client secret.
    3. Add a description for your client secret.
    4. Select an expiration for the secret or specify a custom lifetime.
        1. Client secret lifetime is limited to two years (24 months) or less. You can't specify a custom lifetime longer than 24 months.
        2. Microsoft recommends that you set an expiration value of less than 12 months.
    5. Select Add.
    6. Record the secret's value for use in your client application code. This secret value is never displayed again after you leave this page.

    **Add a redirect URI**
    A redirect URI is the location where the Microsoft identity platform redirects a user's client and sends security tokens after authentication.

    You add and modify redirect URIs for your registered applications by configuring their platform settings.

    Enter https://lakefs-cloud.us.auth0.com/login/callback as your redirect URI.

    Settings for each application type, including redirect URIs, are configured in Platform configurations in the Azure portal. Some platforms, like Web and Single-page applications, require you to manually specify a redirect URI. For other platforms, like mobile and desktop, you can select from redirect URIs generated for you when you configure their other settings.

    **Steps:**

    1. In the Azure portal, in App registrations, select your application.
    2. Under Manage, select Authentication.
    3. Under Platform configurations, select Add a platform.
    4. Under Configure platforms, select the web option.
    5. Select Configure to complete the platform configuration.

    Once you finish registering lakeFS Cloud with Azure AD send the following items to the Treeverse's team:

    1. **Client ID**
    2. **Client Secret**
    3. **Azure AD Domain**
    4. **Identity API Version** (v1 for Azure AD or v2 for Microsoft Identity Platform/Entra) 

## SSO for lakeFS Enterprise

Authentication in lakeFS Enterprise is handled directly by the lakeFS Enterprise service. 
lakeFS Enterprise supports the following identity providers:

* Active Directory Federation Services (AD FS) (using SAML)
* OpenID Connect
* LDAP
* External AWS Authentication (using IAM)

If you're using an authentication provider that is not listed, please [contact us](https://lakefs.io/contact-us/) for further assistance.

=== "Active Directory Federation Services (AD FS) (using SAML)"

    !!! note
        AD FS integration uses certificates to sign and encrypt requests going out from lakeFS Enterprise and decrypt incoming requests from AD FS server.

    If you'd like to generate a self-signed certificates using OpenSSL:

    ```sh
    openssl req -x509 -newkey rsa:2048 -keyout myservice.key -out myservice.cert -days 365 -nodes -subj "/CN=lakefs.company.com"
    ```
    In order for SAML authentication to work, configure the following values in your chart's `values.yaml` file:

    ```yaml
    ingress:
      enabled: true
      ingressClassName: <class-name>
      hosts:
        - host: <lakefs.ingress.domain>
          paths: 
            - /

    enterprise:
      enabled: true
      auth:
        saml:
          enabled: true
          createCertificateSecret: true  # NEW: Auto-creates secret
          certificate:
            samlRsaPublicCert: |           # RENAMED: from saml_rsa_public_cert
              -----BEGIN CERTIFICATE-----
              ...
              -----END CERTIFICATE-----
            samlRsaPrivateKey: |           # RENAMED: from saml_rsa_private_key
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

=== "OpenID Connect"

    In order for OIDC to work, configure the following values in your chart's `values.yaml` file:

    ```yaml
    ingress:
      enabled: true
      ingressClassName: <class-name>
      hosts:
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

=== "LDAP"

    lakeFS Enterprise provides direct LDAP authentication by querying the LDAP server for user information and authenticating the user based on the provided credentials.

    **Important:** An administrative bind user must be configured. It should have search permissions for the LDAP server.

    ```yaml
    ingress:
      enabled: true
      ingressClassName: <class-name>
      hosts:
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
      auth:
        providers:
          ldap:
            server_endpoint: ldaps://ldap.company.com:636
            bind_dn: uid=<bind-user-name>,ou=Users,o=<org-id>,dc=<company>,dc=com 
            username_attribute: uid
            user_base_dn: ou=Users,o=<org-id>,dc=<company>,dc=com  
            user_filter: (objectClass=inetOrgPerson)
            connection_timeout_seconds: 15
            request_timeout_seconds: 7
            # RBAC group for first time users
            default_user_group: "Developers"
    ```

    ### Troubleshooting LDAP issues

    #### Inspecting Logs

    If you encounter LDAP connection errors, inspect the lakeFS container logs to get more information.

    #### Authentication issues

    Auth issues (e.g. user not found, invalid credentials) can be debugged with the `ldapwhoami` CLI tool. 

    The examples are based on the configuration above:

    To verify that the main bind user can connect:
    
    ```sh 
    ldapwhoami -H ldaps://ldap.company.com:636 -D "uid=bind-user-name,ou=Users,o=org-id,dc=company,dc=com" -x -W
    ```

    To verify that a specific lakeFS user `dev-user` can connect:

    ```sh 
    ldapwhoami -H ldaps://ldap.company.com:636 -D "uid=dev-user,ou=Users,o=org-id,dc=company,dc=com" -x -W
    ```

    #### User not found issue

    Upon a login request, the bind user will search for the user in the LDAP server. If the user is not found it will be presented in the logs.

    We can search the user using [ldapsearch](https://docs.ldap.com/ldap-sdk/docs/tool-usages/ldapsearch.html) CLI tool. 

    Search ALL users in the base DN (no filters):

    !!! note
        `-b` is the `user_base_dn`, `-D` is `bind_dn` and `-w` is `bind_password` from the lakeFS configuration.

    ```sh
    ldapsearch -H ldaps://ldap.company.com:636 -x -b "ou=Users,o=org-id,dc=company,dc=com" -D "uid=bind-user-name,ou=Users,o=org-id,dc=company,dc=com" -w 'bind_user_pwd'
    ```

    If the user is found, we should now use filters for the specific user the same way lakeFS does it and expect to see the user. 

    For example, to reproduce the same search as lakeFS does:
    - user `dev-user` set from `uid` attribute in LDAP 
    - Configuration values: `user_filter: (objectClass=inetOrgPerson)` and `username_attribute: uid`

    ```sh
    ldapsearch -H ldaps://ldap.company.com:636 -x -b "ou=Users,o=org-id,dc=company,dc=com" -D "uid=bind-user-name,ou=Users,o=org-id,dc=company,dc=com" -w 'bind_user_pwd' "(&(uid=dev-user)(objectClass=inetOrgPerson))"
    ```

=== "External AWS Authentication"

    lakeFS Enterprise supports authentication using AWS IAM credentials. This allows users to authenticate using their AWS credentials via GetCallerIdentity.

    ```yaml
    ingress:
      enabled: true
      ingressClassName: <class-name>
      hosts:
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

### Common Configuration

All authentication methods share some common configuration options:

```yaml
# Enable enterprise features
enterprise:
  enabled: true

# Common lakeFS configuration
lakefsConfig: |
  # Basic auth configuration
  auth:
    encrypt:
      secret_key: <your-encryption-secret>  # Set via secrets.authEncryptSecretKey
    login_duration: 24h                      # Session duration
    login_max_duration: 168h                 # Maximum session duration
```

### Secrets Management

The Helm chart will automatically create Kubernetes secrets for sensitive values:

```yaml
secrets:
  # Required: encryption key for auth cookies
  authEncryptSecretKey: <your-random-secret-string>
  
  # For database connection (if using external DB)
  databaseConnectionString: <postgres-connection-string>

# Or use existing secrets
secrets:
  existingSecret: my-lakefs-secrets
  # Define the keys in your secret:
  authEncryptSecretKeyName: authEncryptSecretKey
  databaseConnectionStringKeyName: databaseConnectionString
```

Authentication provider secrets are managed separately via the `enterprise.auth.*` configuration.

### Environment Variables

All configurations can also be set via environment variables:

```bash
# SAML
LAKEFS_AUTH_PROVIDERS_SAML_SP_ROOT_URL=https://lakefs.company.com
LAKEFS_AUTH_COOKIE_AUTH_VERIFICATION_EXTERNAL_USER_ID_CLAIM_NAME=samName

# OIDC
LAKEFS_AUTH_PROVIDERS_OIDC_CLIENT_ID=your-client-id
LAKEFS_AUTH_OIDC_FRIENDLY_NAME_CLAIM_NAME=name

# LDAP
LAKEFS_AUTH_PROVIDERS_LDAP_SERVER_ENDPOINT=ldaps://ldap.company.com:636
LAKEFS_AUTH_PROVIDERS_LDAP_BIND_DN=uid=bind-user,ou=Users,o=org,dc=company,dc=com

# External AWS Auth
LAKEFS_AUTH_EXTERNAL_AWS_AUTH_ENABLED=true
```