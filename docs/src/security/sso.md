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

lakeFS Cloud uses Auth0 for authentication and thus support the same identity providers as Auth0 including Active Directory/LDAP, ADFS, Azure Active Directory Native, Google Workspace, OpenID Connect, Okta, PingFederate, SAML, and Azure Active Directory.

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

Details for configuring the supported identity providers with lakeFS Enterprise are shown below. In addition, please review the necessary [Helm configuration](#helm) to configure lakeFS.

* Active Directory Federation Services (AD FS) (using SAML)
* OpenID Connect
* LDAP

If you're using an authentication provider that is not listed please [contact us](https://lakefs.io/contact-us/) for further assistance.

=== "Active Directory Federation Services (AD FS) (using SAML)"
    !!! note
        AD FS integration uses certificates to sign & encrypt requests going out from lakeFS and decrypt incoming requests from AD FS server.
    
    In order for lakeFS Enterprise to work with AD FS, the following configuration must be set:
    1. Replace certificate paths (`sp_x509_key_path` and `sp_x509_cert_path`) with your actual certificate files
    2. Replace `https://lakefs.company.com` with your actual lakeFS server URL
    3. Replace `idp_metadata_url` with your AD FS metadata URL
    4. Update `external_user_id_claim_name` to match your AD FS claim configuration
    5. Set appropriate `default_initial_groups` for your users
    
    If you'd like to generate the certificates using OpenSSL, you can use the following example:
    ```sh
    openssl req -x509 -newkey rsa:2048 -keyout dummy_saml_rsa.key -out dummy_saml_rsa.cert -days 365 -nodes -subj "/CN=lakefs.company.com"
    ```
    
    lakeFS Enterprise Configuration:
    ```yaml
    security:
      check_latest_version_cache: false
    logging:
      format: json
      level: DEBUG
      output: "-"
    database:
      type: local
      local: 
        path: /tmp/lakefs/data
    auth:
      logout_redirect_url: https://lakefs.company.com
      cookie_auth_verification:
        auth_source: saml
        friendly_name_claim_name: displayName
        default_initial_groups: ["Admins"]
        external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name
        validate_id_token_claims:
          department: r_n_d
      providers:
        saml:
          sp_root_url: https://lakefs.company.com
          sp_x509_key_path: dummy_saml_rsa.key
          sp_x509_cert_path: dummy_saml_rsa.cert
          sp_sign_request: true
          sp_signature_method: "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
          idp_metadata_url: "https://adfs-auth.company.com/federationmetadata/2007-06/federationmetadata.xml"
          post_login_redirect_url: /
      encrypt:
        secret_key: "your-secret-key"
      ui_config:
        login_url: https://lakefs.company.com/sso/login-saml
        logout_url: /sso/logout-saml
        login_cookie_names:
          - internal_auth_session
          - saml_auth_session
        rbac: internal
    blockstore:
      type: local
    features:
      local_rbac: true
    installation:
      fixed_id: "your-installation-id"
    ```
  
=== "OpenID Connect"

    In order for lakeFS Enterprise to work with OIDC, the following configuration must be set:
    1. Replace `friendly_name_claim_name` with the right claim name from your OIDC provider
    2. Replace `default_initial_groups` with desired groups (See [pre-configured][rbac-preconfigured] groups for enterprise)
    3. Replace `logout_redirect_url` with your full OIDC logout URL (e.g `https://oidc-provider-url.com/v2/logout`)
    4. Replace `providers.oidc.url` with your OIDC provider URL (e.g `https://oidc-provider-url.com/`)
    5. Replace `providers.oidc.client_id` and `providers.oidc.client_secret` with the client ID & secret for OIDC
    6. Replace `logout_client_id_query_parameter` with the query parameter that represents the client_id required by your OIDC provider
    7. Replace `http://localhost:8000` with your actual lakeFS server URL
    8. Update `logout_endpoint_query_parameters` with parameters required by your OIDC provider for logout
    
    lakeFS Enterprise Configuration:
    ```yaml
    security:
      check_latest_version_cache: false
    database:
      type: local
      local:
        path: /tmp/lakefs/data
    logging:
        level: "DEBUG"
        audit_log_level: "INFO"
    blockstore:
      type: local
      local:
        path: /tmp/lakefs/data
    features: 
      local_rbac: true
    auth:
      logout_redirect_url: https://oidc-provider-url.com/v2/logout
      encrypt:
        secret_key: shared-secrey-key
      ui_config:
        login_url: https://lakefs.company.com/oidc/login
        logout_url: https://lakefs.company.com/oidc/logout
        login_cookie_names:
          - internal_auth_session
          - oidc_auth_session
        rbac: internal
      oidc:
        friendly_name_claim_name: "nickname"
        default_initial_groups: ["Admins"]
      providers:
        oidc:
          post_login_redirect_url: https://lakefs.company.com/
          url: https://oidc-provider-url.com/
          client_id: <your-client-id>
          client_secret: <your-client-secret>
          callback_base_url: https://lakefs.company.com
          logout_client_id_query_parameter: client_id
          logout_endpoint_query_parameters:
            - returnTo
            - https://lakefs.company.com/oidc/login
    ```

=== "LDAP"

    lakeFS Enterprise provides LDAP authentication by querying the LDAP server for user information and authenticating users based on the provided credentials.

    **Important:** An administrative bind user must be configured. It should have search permissions for the LDAP server that will be used to query for user information.

    **Configuration Requirements:**

    1. Replace `providers.ldap.server_endpoint` with your LDAP server endpoint (e.g `ldaps://ldap.company.com:636`)
    2. Replace `providers.ldap.bind_dn` with the LDAP bind user that has permissions to query your LDAP server
    3. Replace `providers.ldap.bind_password` with the bind user's password
    4. Replace `providers.ldap.user_base_dn` with the user base DN to search users in
    5. Update other LDAP settings as needed for your environment

    **lakeFS Enterprise Configuration:**

    ```yaml
    security:
      check_latest_version_cache: false
    database:
      type: local
      local:
        path: /tmp/lakefs/data
    logging:
        level: "DEBUG"
        audit_log_level: "INFO"
    blockstore:
      type: local
      local:
        path: /tmp/lakefs/data
    features: 
      local_rbac: true
    auth:
      encrypt:
        secret_key: shared-secrey-key
      ui_config:
        logout_url: /logout
        login_cookie_names:
          - internal_auth_session
        rbac: internal
      providers:
        ldap:
          server_endpoint: ldaps://ldap.company.com:636
          bind_dn: uid=bind-user-name,ou=Users,o=org-id,dc=company,dc=com
          bind_password: your-bind-password
          username_attribute: uid 
          user_base_dn: ou=Users,o=org-id,dc=company,dc=com 
          user_filter: (objectClass=inetOrgPerson)
          connection_timeout_seconds: 15
          request_timeout_seconds: 7
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

### Helm

In order to use lakeFS Enterprise, we provided out of the box setup, see [lakeFS Helm chart configuration](https://github.com/treeverse/charts).

Notes:


* Check the [examples on GitHub](https://github.com/treeverse/charts/tree/master/examples/lakefs/enterprise) we provide for each authentication method (oidc/adfs/ldap + rbac).
* The examples are provisioned with a Postgres pod for quick-start, make sure to replace that to a stable database once ready.
<!-- # TODO:  set correct version -->
* The lakeFS `image.tag` must be >= 1.0.0
* Change the `ingress.hosts[0]` from `lakefs.company.com` to a real host (usually same as lakeFS), also update additional references in the file (note: URL path after host if provided should stay unchanged).
* Update the `ingress` configuration with other optional fields if used
* lakeFS Enterprise docker image: replace the `lakefs-enterprise.image.privateRegistry.secretToken` with real token to dockerhub for the lakeFS Enterprise docker image.
