---
title: Single Sign On (SSO)
description: How to configure Single Sign On (SSO) for lakeFS Cloud and lakeFS Enterprise.
parent: Security
redirect_from:
  - /cloud/sso.html
  - /enterprise/sso.html
---

# Single Sign On (SSO)
{: .d-inline-block }
<a style="color: white;" href="#sso-for-lakefs-cloud">lakeFS Cloud</a>
{: .label .label-green }

{: .d-inline-block }
<a style="color: white;" href="#sso-for-lakefs-enterprise">lakeFS Enterprise</a>
{: .label .label-purple }

{: .note}
> SSO is available for lakeFS Cloud and lakeFS Enterprise. If you're using the open-source version of lakeFS you can read more about the [authentication options available]({% link security/authentication.md %}). 

## SSO for lakeFS Cloud

lakeFS Cloud uses Auth0 for authentication and thus support the same identity providers as Auth0 including Active Directory/LDAP, ADFS, Azure Active Directory Native, Google Workspace, OpenID Connect, Okta, PingFederate, SAML, and Azure Active Directory.

<div class="tabs">
  <ul>
    <li><a href="#okta">Okta</a></li>
    <li><a href="#adfs">AD FS</a></li>
    <li><a href="#azure-ad">Azure AD</a></li>
  </ul> 
  <div markdown="1" id="okta">
## Okta

{: .note}
> This guide is based on [Okta's Create OIDC app integrations guide](https://help.okta.com/en-us/Content/Topics/Apps/Apps_App_Integration_Wizard_OIDC.htm).

Steps:
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
  </div>
  <div markdown="1" id="adfs">
## Active Directory Federation Services (AD FS)

Prerequisites:
* Client's AD FS server should be exposed publicly or to Auth0's IP ranges (either directly or using Web Application Proxy)

Steps:
1. Connect to the AD FS server
2. Open AD FS' PowerShell CLI as Administrator through the server manager
3. Execute the following:

    ```sh
    (new-object Net.WebClient -property @{Encoding = [Text.Encoding]::UTF8}).DownloadString("https://raw.github.com/auth0/adfs-auth0/master/adfs.ps1") | iex

    AddRelyingParty "urn:auth0:lakefs-cloud" "https://lakefs-cloud.us.auth0.com/login/callback"
    ```

    **Note**: If your organization data is located in Europe, use `lakefs-cloud.eu.auth0.com` instead of `lakefs-cloud.us.auth0.com`.

Once you finish registering lakeFS Cloud with AD FS, save the **AD FS URL** and send this to Treeverse's team to finish the integration.
  </div>
  <div markdown="1" id="azure-ad">
## Azure Active Directory (AD)

Prerequisites:
* Azure account with permissions to manage applications in Azure Active Directory

**Note**: If you've already set up lakeFS Cloud with your Azure account, you can skip the [Register lakeFS Cloud with Azure](#register-lakefs-cloud-with-azure) and [Add client secret](#add-a-secret) and go directly to [Add a redirect URI](#add-a-redirect-uri).

### Register lakeFS Cloud with Azure

Steps:
1. Sign in to the Azure portal.
2. If you have access to multiple tenants, use the Directories + subscriptions filter in the top menu to switch to the tenant in which you want to register the application.
3. Search for and select Azure Active Directory.
4. Under Manage, select App registrations > New registration.
5. Enter a display Name for your application. Users of your application might see the display name when they use the app, for example during sign-in. You can change the display name at any time and multiple app registrations can share the same name. The app registration's automatically generated Application (client) ID, not its display name, uniquely identifies your app within the identity platform.
6. Specify who can use the application, sometimes called its sign-in audience.

   Note: don't enter anything for Redirect URI (optional). You'll configure a redirect URI in the next section.
7. Select Register to complete the initial app registration.

When registration finishes, the Azure portal displays the app registration's Overview pane. You see the Application (client) ID. Also called the client ID, this value uniquely identifies your application in the Microsoft identity platform.

Important: new app registrations are hidden to users by default. When you are ready for users to see the app on their My Apps page you can enable it. To enable the app, in the Azure portal navigate to Azure Active Directory > Enterprise applications and select the app. Then on the Properties page toggle Visible to users? to Yes.

### Add a secret
Sometimes called an application password, a client secret is a string value your app can use in place of a certificate to identity itself.

Steps:
1. In the Azure portal, in App registrations, select your application.
2. Select Certificates & secrets > Client secrets > New client secret.
3. Add a description for your client secret.
4. Select an expiration for the secret or specify a custom lifetime.
    1. Client secret lifetime is limited to two years (24 months) or less. You can't specify a custom lifetime longer than 24 months.
    2. Microsoft recommends that you set an expiration value of less than 12 months.
5. Select Add.
6. Record the secret's value for use in your client application code. This secret value is never displayed again after you leave this page.

### Add a redirect URI
A redirect URI is the location where the Microsoft identity platform redirects a user's client and sends security tokens after authentication.

You add and modify redirect URIs for your registered applications by configuring their platform settings.

Enter https://lakefs-cloud.us.auth0.com/login/callback as your redirect URI.

Settings for each application type, including redirect URIs, are configured in Platform configurations in the Azure portal. Some platforms, like Web and Single-page applications, require you to manually specify a redirect URI. For other platforms, like mobile and desktop, you can select from redirect URIs generated for you when you configure their other settings.

Steps:
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

  </div>
</div>

## SSO for lakeFS Enterprise

Authentication in lakeFS Enterprise is handled by a secondary service which runs side-by-side with lakeFS. With a nod to Hogwarts and their security system, we've named this service _Fluffy_. Details for configuring the supported identity providers with Fluffy are shown below. In addition, please review the necessary [Helm configuration](#helm) to configure Fluffy.

* Active Directory Federation Services (AD FS) (using SAML)
* OpenID Connect
* LDAP

If you're using an authentication provider that is not listed please [contact us](https://lakefs.io/contact-us/) for further assistance.

<div class="tabs">
  <ul>
    <li><a href="#adfs">AD FS</a></li>
    <li><a href="#oidc">OpenID Connect</a></li>
    <li><a href="#ldap">LDAP</a></li>
  </ul> 
  <div markdown="1" id="adfs">
## Active Directory Federation Services (AD FS) (using SAML)

{: .note}
> AD FS integration uses certificates to sign & encrypt requests going out from Fluffy and decrypt incoming requests from AD FS server.

In order for Fluffy to work, the following values must be configured. Update (or override) the following attributes in the chart's `values.yaml` file.
1. Replace `fluffy.saml_rsa_public_cert` and `fluffy.saml_rsa_private_key` with real certificate values
2. Replace `fluffyConfig.auth.saml.idp_metadata_url` with the metadata URL of the AD FS provider (e.g `adfs-auth.company.com`)
3. Replace `fluffyConfig.auth.saml.external_user_id_claim_name` with the claim name representing user id name in AD FS
4. Replace `lakefs.company.com` with your lakeFS server URL.

If you'd like to generate the certificates using OpenSSL, you can take a look at the following example:

```sh
openssl req -x509 -newkey rsa:2048 -keyout myservice.key -out myservice.cert -days 365 -nodes -subj "/CN=lakefs.company.com" -
```

lakeFS Server Configuration (Update in helm's `values.yaml` file):

```yaml
auth:
  cookie_auth_verification:
    auth_source: saml
    friendly_name_claim_name: displayName
    persist_friendly_name: true
    external_user_id_claim_name: samName
    default_initial_groups:
      - "Developers"
  logout_redirect_url: "https://lakefs.company.com/logout-saml"
  encrypt:
    secret_key: shared-secrey-key
  ui_config:
    login_url: "https://lakefs.company.com/sso/login-saml"
    logout_url: "https://lakefs.company.com/sso/logout-saml"
    login_cookie_names:
      - internal_auth_session
      - saml_auth_session
```

Fluffy Configuration (Update in helm's `values.yaml` file):

```yaml
logging:
  format: "json"
  level: "INFO"
  audit_log_level: "INFO"
  output: "="
auth:  
  encrypt:
    secret_key: shared-secrey-key    
  logout_redirect_url: https://lakefs.company.com
  post_login_redirect_url: https://lakefs.company.com
  saml:
    enabled: true 
    sp_root_url: https://lakefs.company.com
    sp_x509_key_path: '/etc/saml_certs/rsa_saml_private.cert'
    sp_x509_cert_path: '/etc/saml_certs/rsa_saml_public.pem'
    sp_sign_request: true
    sp_signature_method: "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
    idp_metadata_url: "https://adfs-auth.company.com/federationmetadata/2007-06/federationmetadata.xml"
    # idp_authn_name_id_format: "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
    external_user_id_claim_name: samName
    # idp_metadata_file_path: 
    # idp_skip_verify_tls_cert: true
```
  </div>
  <div markdown="1" id="oidc">

### OpenID Connect

In order for Fluffy to work, the following values must be configured. Update (or override) the following attributes in the chart's `values.yaml` file.
1. Replace `lakefsConfig.friendly_name_claim_name` with the right claim name.
1. Replace `lakefsConfig.default_initial_groups` with desired claim name (See [pre-configured][rbac-preconfigured] groups for enterprise)
2. Replace `fluffyConfig.auth.logout_redirect_url` with your full OIDC logout URL (e.g `https://oidc-provider-url.com/logout/path`)
3. Replace `fluffyConfig.auth.oidc.url` with your OIDC provider URL (e.g `https://oidc-provider-url.com`)
4. Replace `fluffyConfig.auth.oidc.logout_endpoint_query_parameters` with parameters you'd like to pass to the OIDC provider for logout.
5. Replace `fluffyConfig.auth.oidc.client_id` and `fluffyConfig.auth.oidc.client_secret` with the client ID & secret for OIDC.
6. Replace `fluffyConfig.auth.oidc.logout_client_id_query_parameter` with the query parameter that represent the client_id, note that it should match the the key/query param that represents the client id and required by the specific OIDC provider.
7. Replace `lakefs.company.com` with the lakeFS server URL.

lakeFS Server Configuration (Update in helm's `values.yaml` file):

```yaml
  # Important: make sure to include the rest of your lakeFS Configuration here!
auth:
  encrypt:
    secret_key: shared-secrey-key
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

Fluffy Configuration (Update in helm's `values.yaml` file):

```yaml
logging:
  format: "json"
  level: "INFO"
  audit_log_level: "INFO"
  output: "="
installation:
  fixed_id: fluffy-authenticator
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
  encrypt:
    secret_key: shared-secrey-key
```
  </div>
  <div markdown="1" id="ldap">
## LDAP

Fluffy is incharge of providing LDAP authentication for lakeFS Enterprise. 
The authentication works by querying the LDAP server for user information and authenticating the user based on the provided credentials.

**Important:** An administrative bind user must be configured. It should have search permissions for the LDAP server that will be used to query the LDAP server for user information.

**For Helm:** set the following attributes in the Helm chart values, for lakeFS `lakefsConfig.*` and `fluffyConfig.*` for fluffy. 

**No Helm:** If not using Helm use the YAML below to directly update the configuration file for each service.

**lakeFS Configuration:**

1. Replace `auth.remote_authenticator.enabled` with `true`
2. Replace `auth.remote_authenticator.endpoint` with the fluffy authentication server URL combined with the `api/v1/ldap/login` suffix (e.g `http://lakefs.company.com/api/v1/ldap/login`)

**fluffy Configuration:**

See [Fluffy configuration][fluffy-configuration] reference.

1. Repalce `auth.ldap.remote_authenticator.server_endpoint` with your LDAP server endpoint  (e.g `ldaps://ldap.ldap-address.com:636`)
2. Replace `auth.ldap.remote_authenticator.bind_dn` with the LDAP bind user/permissions to query your LDAP server.
3. Replace `auth.ldap.remote_authenticator.user_base_dn` with the user base to search users in.

**lakeFS Server Configuration file:**

`$lakefs run -c ./lakefs.yaml`

```yaml
# Important: make sure to include the rest of your lakeFS Configuration here!

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

Fluffy Configuration file:

`$fluffy run -c ./fluffy.yaml`

```yaml
logging:
  format: "json"
  level: "INFO"
  audit_log_level: "INFO"
  output: "="
installation:
  fixed_id: fluffy-authenticator
auth:
  post_login_redirect_url: /
  ldap: 
    server_endpoint: 'ldaps://ldap.company.com:636'
    bind_dn: uid=<bind-user-name>,ou=<some-ou>,o=<org-id>,dc=<company>,dc=com
    bind_password: '<ldap pwd>'
    username_attribute: uid
    user_base_dn: ou=<some-ou>,o=<org-id>,dc=<company>,dc=com
    user_filter: (objectClass=inetOrgPerson)
    connection_timeout_seconds: 15
    request_timeout_seconds: 7
```

## Troubleshooting LDAP issues

### Inspecting Logs

If you encounter LDAP connection errors, you should inspect the **fluffy container** logs to get more information.

### Authentication issues

Auth issues (e.g. user not found, invalid credentials) can be debugged with the [ldapwhoami](https://www.unix.com/man-page/osx/1/ldapwhoami) CLI tool. 

The Examples are based on the fluffy config above:

To verify that the main bind user can connect:
  
```sh 
ldapwhoami -H ldap://ldap.company.com:636 -D "uid=<bind-user-name>,ou=<some-ou>,o=<org-id>,dc=<company>,dc=com" -x -W
```

To verify that a specific lakeFS user `dev-user` can connect:

```sh 
ldapwhoami -H ldap://ldap.company.com:636 -D "uid=dev-user,ou=<some-ou>,o=<org-id>,dc=<company>,dc=com" -x -W
```

### User not found issue

Upon a login request in fluffy, the bind user will search for the user in the LDAP server. If the user is not found it will be presented in the logs.

We can search the user using [ldapsearch](https://docs.ldap.com/ldap-sdk/docs/tool-usages/ldapsearch.html) CLI tool. 

Search ALL users in the base DN (no filters):

**Note:** `-b` is the `user_base_dn`, `-D` is `bind_dn` and `-w` is `bind_password` from the fluffy configuration.

```sh
ldapsearch -H ldap://ldap.company.com:636 -x -b "ou=<some-ou>,o=<org-id>,dc=<company>,dc=com" -D "uid=<bind-user-name>,ou=<some-ou>,o=<org-id>,dc=<company>,dc=com" -w '<bind_user_pwd>'
```

If the user is found, we should now use filters for the specific user the same way fluffy does it and expect to see the user. 

For example, to repdocue the same search as fluffy does:
- user `dev-user` set from `uid` attribute in LDAP 
- Fluffy configuration values: `user_filter: (objectClass=inetOrgPerson)` and `username_attribute: uid`

```sh
ldapsearch -H ldap://ldap.company.com:636 -x -b "ou=<some-ou>,o=<org-id>,dc=<company>,dc=com" -D "uid=<bind-user-name>,ou=<some-ou>,o=<org-id>,dc=<company>,dc=com" -w '<bind_user_pwd>' "(&(uid=dev-user)(objectClass=inetOrgPerson))"
```

  </div>
</div>

### Helm

In order to use lakeFS Enterprise and Fluffy, we provided out of the box setup, see [lakeFS Helm chart configuration](https://github.com/treeverse/charts).

Notes:
* Check the [examples on GitHub](https://github.com/treeverse/charts/tree/master/examples/lakefs/enterprise) we provide for each authentication method (oidc/adfs/ldap + rbac).
* The examples are provisioned with a Postgres pod for quick-start, make sure to replace that to a stable database once ready.
* The encrypt secret key `secrets.authEncryptSecretKey` is shared between fluffy and lakeFS for authentication.
* The lakeFS `image.tag` must be >= 1.0.0
* The fluffy `image.tag` must be >= 0.2.7
* Change the `ingress.hosts[0]` from `lakefs.company.com` to a real host (usually same as lakeFS), also update additional references in the file (note: URL path after host if provided should stay unchanged).
* Update the `ingress` configuration with other optional fields if used
* Fluffy docker image: replace the `fluffy.image.privateRegistry.secretToken` with real token to dockerhub for the fluffy docker image.

[rbac-preconfigured]:  {% link security/rbac.md %}#preconfigured-groups
[fluffy-configuration]: {% link enterprise/configuration.md %}#fluffy-server-configuration