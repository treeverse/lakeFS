---
layout: default
title: Single Sign On (SSO) in lakeFS Enterprise
description: How to configure Single Sign On in lakeFS Enterprise.
parent: lakeFS Enterprise
nav_order: 65
has_children: false
has_toc: false
---

# Single Sign On (SSO) in lakeFS Enterprise
{: .d-inline-block }
lakeFS Enterprise
{: .label .label-purple }

{: .note}
> SSO is also available on [lakeFS Cloud](../cloud/sso.html). Using the open-source version of lakeFS? Read more on [authentication](/reference/authentication.html). 

Authentication in lakeFS Enterprise is handled by a secondary service which runs side-by-side with lakeFS. With a nod to Hogwarts and their security system, we've named this service _Fluffy_. Details for configuring the supported identity providers with Fluffy are shown below. In addition, please review the necessary [Helm configuration](#helm) to configure Fluffy.

If you're using an authentication provider that is not listed below but is based on OpenID Connect or SAML please [contact us](support@treeverse.io) for further assistance.


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

Fluffy Configuration (Update in helm's `values.yaml` file):
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
  </div>
  <div markdown="1" id="oidc">
## OpenID Connect

In order for Fluffy to work, the following values must be configured. Update (or override) the following attributes in the chart's `values.yaml` file.
1. Replace `lakefsConfig.friendly_name_claim_name` and `fluffyConfig.friendly_name_claim_name` with the right claim name
2. Replace `fluffyConfig.auth.logout_redirect_url` with your full OIDC logout URL (e.g `https://oidc-provider-url.com/logout/path`)
3. Replace `fluffyConfig.auth.oidc.url` with your OIDC provider URL (e.g `https://oidc-provider-url.com`)
4. Replace `fluffyConfig.auth.oidc.logout_endpoint_query_parameters` with parameters you'd like to pass to theOIDC provider for logout.
5. Replace `fluffyConfig.auth.oidc.client_id` and `fluffyConfig.auth.oidc.client_secret` with the client ID & secret for OIDC. 
6. Replace `fluffyConfig.auth.oidc.logout_client_id_query_parameter` with the query parameter that represent the client_id, note that it should match the the key/query param that represents the client id and required by the specific OIDC provider.
7. Replace `lakefs.company.com` with the lakeFS server URL.

lakeFS Server Configuration (Update in helm's `values.yaml` file):
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

Fluffy Configuration (Update in helm's `values.yaml` file):
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
  </div>
  <div markdown="1" id="ldap">
## LDAP

In order for Fluffy to work, the following values must be configured. Update (or override) the following attributes in the chart's `values.yaml` file.
1. Replace `lakefsConfig.auth.remote_authenticator.endpoint` with the lakeFS server URL combined with the `api/v1/ldap/login` suffix (e.g `http://lakefs.company.com/api/v1/ldap/login`)
2. Repalce `fluffyConfig.auth.ldap.remote_authenticator.server_endpoint` with your LDAP server endpoint  (e.g `ldaps://ldap.ldap-address.com:636`)
3. Replace `fluffyConfig.auth.ldap.remote_authenticator.bind_dn` with the LDAP bind user/permissions to query your LDAP server.
4. Replace `fluffyConfig.auth.ldap.remote_authenticator.user_base_dn` with the user base to search users in.

lakeFS Server Configuration (Update in helm's `values.yaml` file):
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

Fluffy Configuration (Update in helm's `values.yaml` file):
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
  </div>
</div>

## Helm

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