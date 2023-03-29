---
layout: default
title: Single Sign On (SSO)
description: This section covers authentication (using SSO) of your lakeFS Cloud or lakeFS Enterprise.
parent: Reference
nav_order: 65
has_children: false
---


# Single Sign On (SSO)
{: .d-inline-block }
Cloud
{: .label .label-green }

Enterprise
{: .label .label-purple }
{: .no_toc }


{: .note}
> SSO is only available for [lakeFS Cloud](../cloud.md) and [lakeFS Enterprise](../enterprise.md).
>
> Using the Open Source? Read more on [authentication](authentication.html). 

{% include toc.html %}

## Supported Protocols and Third Parties

lakeFS Cloud and lakeFS Enterprise support the commonly used protocols for authentication, such as OpenID Connect and SAML.

There are specific authentication utilities that we've already verified that are working, if you're using one that is not listed below but is based on OpenID Connect or SAML, it's probably a small effort for us to make sure it'll work, please [contact us](support@treeverse.io) for more information.

## Cloud

lakeFS Cloud leverage Auth0's capabilities for authentication, therefore, we're supporting Auth0's identity providers, such as: Active Directory/LDAP, ADFS, Azure Active Directory Native, Google Workspace, OpenID Connect, Okta, PingFederate, SAML and Azure Active Directory.

### Okta
{: .d-inline-block }
Cloud
{: .label .label-green }

> **Note**: this guide is based on [Okta's Create OIDC app integrations guide](https://help.okta.com/en-us/Content/Topics/Apps/Apps_App_Integration_Wizard_OIDC.htm).

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


### Active Directory Federation Services (AD FS)
{: .d-inline-block }
Cloud
{: .label .label-green }

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

### Azure AD
{: .d-inline-block }
Cloud
{: .label .label-green }

Prerequisites:
* Azure account with permissions to manage applications in Azure Active Directory

**Note**: If you've already onboarded to lakeFS Cloud with your Azure account, you can skip the [Register lakeFS Cloud with Azure](#register-lakefs-cloud-with-azure) and [Add client secret](#add-a-secret) and go directly to [Add a redirect URI](#add-a-redirect-uri).

#### Register lakeFS Cloud with Azure

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

#### Add a secret
Sometimes called an application password, a client secret is a string value your app can use in place of a certificate to identity itself.

Client secrets are considered less secure than certificate credentials. Application developers sometimes use client secrets during local app development because of their ease of use. However, you should use certificate credentials for any of your applications that are running in production.

Steps:
1. In the Azure portal, in App registrations, select your application.
2. Select Certificates & secrets > Client secrets > New client secret.
3. Add a description for your client secret.
4. Select an expiration for the secret or specify a custom lifetime.
    1. Client secret lifetime is limited to two years (24 months) or less. You can't specify a custom lifetime longer than 24 months.
    2. Microsoft recommends that you set an expiration value of less than 12 months.
5. Select Add.
6. Record the secret's value for use in your client application code. This secret value is never displayed again after you leave this page.

#### Add a redirect URI
A redirect URI is the location where the Microsoft identity platform redirects a user's client and sends security tokens after authentication.

You add and modify redirect URIs for your registered applications by configuring their platform settings.

Enter https://lakefs-cloud.us.auth0.com/login/callback or https://lakefs-cloud.eu.auth0.com/login/callback (depends on your organization data location) as your redirect URI.

Settings for each application type, including redirect URIs, are configured in Platform configurations in the Azure portal. Some platforms, like Web and Single-page applications, require you to manually specify a redirect URI. For other platforms, like mobile and desktop, you can select from redirect URIs generated for you when you configure their other settings.

Steps:
1. In the Azure portal, in App registrations, select your application.
2. Under Manage, select Authentication.
3. Under Platform configurations, select Add a platform.
4. Under Configure platforms, select the web option.
5. Select Configure to complete the platform configuration.

Once you finish registering lakeFS Cloud with Azure AD, save the **Application (Client) ID**, **Application Secret Value** and send this to Treeverse's team to finish the integration.

## Enterprise

lakeFS Enterprise provides a secondary service, running side-by-side with lakeFS which handles the authentication, this service is called Fluffy.
Once you've onboarded to [lakeFS Enterprise](../enterprise.md) and have access to the authentication service (Fluffy), you can configure one of the supported authentication methods below.
lakeFS Enterprise leverage Helm's capabilities to run the sidecar with it's configuration, on top of the specific-authentication method, please review the [Helm configuration](#helm).

### AD FS (using SAML)
{: .d-inline-block }
Enterprise
{: .label .label-purple }

> **Note**: AD FS integration is using certificates to sign & encrypt requests going out from Fluffy and decrypt incoming requests from AD FS server.

In order for fluffy to work, some values must be configured, update the relevant values section in this file and comment it out.
1. Replace `fluffy.saml_rsa_public_cert` and `fluffy.saml_rsa_private_key` with real certificate values
2. Replace `fluffyConfig.auth.saml.idp_metadata_url` to the metadata URL of the provider <adfs-auth.company.com>
3. Replace `fluffyConfig.auth.saml.external_user_id_claim_name` with the claim name representing user id name in ADFS
4. Replace `lakefs.company.com` with the lakeFS server endpoint.

If you'd like to generate the certificates using OpenSSL, you can take a look at the following example:
```sh
openssl req -x509 -newkey rsa:2048 -keyout myservice.key -out myservice.cert -days 365 -nodes -subj "/CN=lakefs.company.com" -
```

lakeFS Enterprise Configuration:
```yaml
lakefsConfig: |
  # Important: make sure to include the rest of your lakeFS Configuration here!

  auth:
    cookie_auth_verification:
      auth_source: saml
      friendly_name_claim_name: displayName
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
      rbac: external
```

Fluffy Configuration:
```yaml
fluffyConfig: &fluffyConfig |
  logging:
    format: "json"
    level: "INFO"
    audit_log_level: "INFO"
    output: "="
  installation:
    fixed_id: fluffy-authenticator
  auth:  
    # better set from secret FLUFFY_AUTH_ENCRYPT_SECRET_KEY must be equal to what is used in lakeFS for auth_encrypt_secret_key
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

### OpenID Connect
{: .d-inline-block }
Enterprise
{: .label .label-purple }

In order for fluffy to work some values must be configured, update the relevant values section in this file and comment it out. (lakefsConfig and fluffyConfig values)
1. Replace `lakefsConfig.friendly_name_claim_name` and `fluffyConfig.friendly_name_claim_name` with the right claim name
2. Replace `fluffyConfig.auth.logout_redirect_url`:  https://oidc-provider-url.com/logout/path with full URL to logout API of the OIDC provide
3. Replace `fluffyConfig.auth.oidc.url` with full url to OIDC provider  https://oidc-provider-url.com
4. Replace in `fluffyConfig.auth.oidc.logout_endpoint_query_parameters` with parameters for the target OIDC provider for logout. In this example it's API auth0 returnTo=https://lakefs.company.com/oidc/login
5. Replace the values in `fluffyConfig.auth.oidc.client_id` and `fluffyConfig.auth.oidc.client_secret` for OIDC. 
6. Replace the values in `logout_client_id_query_parameter`, note it should match the the key/query param that represents the client id and required by the specific OIDC provider
7. Replace `lakefs.company.com` with the lakeFS server endpoint.

lakeFS Enterprise Configuration:
```yaml
lakefsConfig: |
  # Important: make sure to include the rest of your lakeFS Configuration here!

  auth:
    encrypt:
      secret_key: shared-secrey-key
    oidc:
      friendly_name_claim_name: "name"
      default_initial_groups: ["Developers"]
    ui_config:
      login_url: /oidc/login
      logout_url: /oidc/logout
      login_cookie_names:
        - internal_auth_session
        - oidc_auth_session
      rbac: external
```

Fluffy Configuration:
```yaml
fluffyConfig: &fluffyConfig |
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
      default_initial_groups: ["Developers"]
      friendly_name_claim_name: "name"
      logout_client_id_query_parameter: client_id
      logout_endpoint_query_parameters:
        - returnTo 
        - https://lakefs.company.com/oidc/login
    # better set from secret FLUFFY_AUTH_ENCRYPT_SECRET_KEY must be equal to what is used in lakeFS for auth_encrypt_secret_key
    # encrypt:
    #   secret_key: shared-secrey-key
```

### LDAP
{: .d-inline-block }
Enterprise
{: .label .label-purple }

In order for fluffy to work some values must be configured, update the relevant values section in this file and comment it out. (lakefsConfig and fluffyConfig values)
1. Replace `lakefsConfig.auth.remote_authenticator.endpoint` to the ingress host with the following path http://lakefs.company.com/api/v1/ldap/login
2. Repalce `fluffyConfig.auth.ldap.remote_authenticator.server_endpoint` (ldaps://ldap.ldap-address.com:636) with a real LDAP server endpoint 
3. Replace `fluffyConfig.auth.ldap.remote_authenticator.bind_dn` and `fluffyConfig.auth.ldap.remote_authenticator.base_dn`
4. Replace optional `fluffyConfig.auth.ldap.remote_authenticator.bind_dn` example: '(objectClass=inetOrgPerson)'

lakeFS Enterprise Configuration:
```yaml
lakefsConfig: |
  # Important: make sure to include the rest of your lakeFS Configuration here!

  auth:
    remote_authenticator:
      enabled: true
      endpoint: https://lakefs.company.com/api/v1/ldap/login
      default_user_group: "Developers"
    ui_config:
      logout_url: /logout
      login_cookie_names:
        - internal_auth_session
      rbac: external
```

Fluffy Configuration:
```yaml
fluffyConfig: &fluffyConfig |
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
      bind_dn: uid=<bind-user-name>,ou=Users,o=<org-id>,dc=<company>,dc=com
      bind_password: '<ldap pwd>'
      username_attribute: uid
      user_base_dn: ou=Users,o=<org-id>,dc=<company>,dc=com
      user_filter: (objectClass=inetOrgPerson)
      connection_timeout_seconds: 15
      request_timeout_seconds: 7
```

### Helm

In order to use lakeFS Enterprise and Fluffy, see [lakeFS Helm chart configuration](https://github.com/treeverse/charts/tree/master/charts/lakefs).

Notes:
* `secrets.authEncryptSecretKey` is shared between fluffy & lakeFS and must be equal 
* lakeFS `image.tag` must be >= 0.95.0
* Change the `fluffy.ingress.host` from `lakefs.company.com` to a real host (usually same as lakeFS), also update additional references in the file (note: URL path after host if provided should stay unchanged).
* Update the `fluffy.ingress` configuration with other optional fields if used
* Fluffy docker image: replace the `fluffy.image.secretToken` with real token to dockerhub for the fluffy docker image.

```yaml
{% raw %}###############################################
###############################################
############ COMMON CONFIGURATION #############
###############################################
###############################################

fluffy:
  serviceAccountName: default
  name: &fluffyName lakefs-fluffy
  logLevel: INFO
  replicaCount: &fluffyReplicaCount 1
  resources: &resources
    limits:
      memory: 256Mi
    requests:
      memory: 128Mi
  ingress:
    enabled: &enableIngress true 
    host: &ingressHost lakefs.company.com
    ingressClassName: &ingressClassName ''
    annotations: &ingressAnnotations {}
  image: 
    repository: 'treeverse/fluffy'
    tag: '0.1.1'
    secretName: 'fluffy-dockerhub'
    secretToken: &fluffyImageSecretToken '<docker token>'
  saml_rsa_public_cert: |
    -----BEGIN CERTIFICATE-----
    -----END CERTIFICATE-----
  saml_rsa_private_key: |
    -----BEGIN PRIVATE KEY-----
    -----END PRIVATE KEY-----

###############################################
###############################################
###### ADVANCED DON'T EDIT VALUES BELOW #######
###############################################
###############################################

ingress:
  enabled: *enableIngress
  ingressClassName: *ingressClassName
  annotations: *ingressAnnotations
  hosts:
    - host: *ingressHost
      paths: 
       - /
      pathsOverrides: 
        # OIDC related overrides, fluffy and lakefs must be on the same host 
        - path: /oidc/
          serviceName: *fluffyName
          servicePort: 80
        - path: /api/v1/oidc/
          serviceName: *fluffyName
          servicePort: 80
        # SAML related overrides, fluffy and lakefs must be on the same host
        - path: /saml/
          serviceName: *fluffyName
          servicePort: 80
        - path: /sso/
          serviceName: *fluffyName
          servicePort: 80
      # Optional if fluffy and lakefs both shariong the same ingress then this override
        - path: /api/v1/ldap/
          serviceName: *fluffyName
          servicePort: 80
extraManifestsValues: 
  fluffyContainerPort: &fluffyContainerPort 8000
  dockerhubConfig:
    auths:
      https://index.docker.io/v1/: 
        username: externallakefs
        password: '{{ .Values.fluffy.image.secretToken }}'
  labels: &labels
    helm.sh/chart: '{{ include "lakefs.chart" . }}'
    app: fluffy
    app.kubernetes.io/name: fluffy
    app.kubernetes.io/instance: '{{ .Release.Name }}'
    app.kubernetes.io/version: '{{ .Chart.AppVersion }}'
    app.kubernetes.io/managed-by: '{{ .Release.Service }}'
  selectorLabels: &selectorLabels
    app: fluffy
    app.kubernetes.io/name: fluffy
    app.kubernetes.io/instance: '{{ .Release.Name }}'
extraManifests:
  - apiVersion: v1
    kind: Service
    metadata:
      name: '{{ .Values.fluffy.name }}'
      labels: 
        *labels
    spec:
      type: '{{ .Values.service.type }}'
      ports:
          - port: 80
            targetPort: http
            protocol: TCP
            name: http
      selector: *selectorLabels
  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: '{{ .Values.fluffy.name }}'
      labels:
        *labels
    spec:
      replicas: *fluffyReplicaCount
      selector:
        matchLabels:
          *selectorLabels
      template:
        metadata:
          annotations:
            checksum/config: '{{ .Values.fluffyConfig | sha256sum }}'
          labels:
            *selectorLabels
        spec:
          serviceAccountName: "{{ .Values.fluffy.serviceAccountName }}"
          imagePullSecrets:
            - name: "{{ .Values.fluffy.image.secretName }}"
          containers:
            - name: fluffy
              args: ["run"]
              image: "{{ .Values.fluffy.image.repository }}:{{ .Values.fluffy.image.tag }}"
              imagePullPolicy: '{{ .Values.image.pullPolicy }}'
              env:
                - name: FLUFFY_AUTH_ENCRYPT_SECRET_KEY
                  valueFrom:
                    secretKeyRef:
                      name: '{{ include "lakefs.fullname" . }}'
                      key: auth_encrypt_secret_key
              ports:
                - name: http
                  containerPort: *fluffyContainerPort
                  protocol: TCP
              resources:
                *resources
              volumeMounts:
                - name: fluffy-config
                  mountPath: /etc/fluffy/
                - name: secret-volume
                  readOnly: true
                  mountPath: "/etc/saml_certs/"
          volumes:
            - name: fluffy-config
              configMap:
                name: '{{ .Values.fluffy.name }}-config'
            - name: secret-volume
              secret:
                secretName: saml-certificates
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name: '{{ .Values.fluffy.name }}-config'
    data:
      config.yaml: *fluffyConfig
  - apiVersion: v1
    kind: Secret
    metadata:
      name: '{{ .Values.fluffy.image.secretName }}'
    data:
      .dockerconfigjson: '{{ tpl (.Values.extraManifestsValues.dockerhubConfig | toJson ) . | b64enc }}'
    type: kubernetes.io/dockerconfigjson
  - apiVersion: v1
    kind: Secret
    metadata:
      name: saml-certificates
    data:
      rsa_saml_public.pem: '{{ .Values.fluffy.saml_rsa_public_cert | b64enc }}'
      rsa_saml_private.cert: '{{ .Values.fluffy.saml_rsa_private_key | b64enc }}'{% endraw %}
```