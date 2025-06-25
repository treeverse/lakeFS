---
title: Architecture
description: lakeFS Enterprise architecture explained!
---


# Architecture


lakeFS Enterprise extends the open-source lakeFS foundation, delivering a complete data versioning and governance solution with seamlessly integrated enterprise features like SSO, RBAC, mounting capabilities, and more.

![img.png](../assets/img/enterprise/lakefs-enterprise-architecture.png)

[1] Any user request to lakeFS via Browser or Programmatic access (SDK, HTTP
API, lakectl).

[2] A reverse proxy (e.g., NGINX, Traefik, Kubernetes Ingress) will handle user requests between lakeFS server instances.

[3] lakeFS Enterprise - lakeFS with additional enterprise functionality, including advanced security, compliance, audit logging, and enterprise support.

1. SSO authentication (Browser login via Azure AD, Okta, Auth0)
1. RBAC authorization and policy enforcement

[4] The [KV Store](../understand/architecture.md) - Where metadata is stored, used by both core lakeFS and enterprise features.

[5] SSO IdP - External identity provider (e.g. Azure AD, Okta, JumpCloud). 
lakeFS Enterprise implements SAML, OAuth2, and OIDC protocols.

For more details and pricing, please [contact sales](https://lakefs.io/contact-sales/).


!!! info
    Setting up lakeFS enterprise with an SSO IdP (OIDC, SAML or LDAP) requires
    configuring access from the IdP too.
