---
title: lakeFS Enterprise
description: lakeFS Enterprise is an enterprise-ready lakeFS solution providing additional features including RBAC, SSO and Support SLA.
has_children: true
nav_order: 110
---

# lakeFS Enterprise

LakeFS Enterprise is a commercially-supported version of the open-source lakeFS project, 
offering additional features and functionalities targeted for businesses. 
It provides several benefits over the open-source version:

* Security - Advanced Authorization
    * [RBAC]({% link reference/security/rbac.md %}) -  implements role-based access control to manage user permissions. It allows for fine-grained control by associating permissions with users and groups, granting them specific actions on specific resources. This ensures data security and compliance within an organization.
* Security - Advanced Authentication
    * [SSO]({% link reference/security/sso.md %}) -  lets users sign in with existing credentials from a trusted provider, eliminating separate logins.
    * [STS Auth]({% link reference/security/sts-login.md %}) - offers temporary, secure logins using an Identity Provider, simplifying user access and enhancing security.
    * [Authenticate to lakeFS with AWS IAM Roles]({% link reference/security/external-principals-aws.md %}) - lets programs authenticate using AWS IAM roles instead of lakeFS credentials, granting access based on IAM policies.
    * [lakeFS Mount]({% link reference/mount.md %})

* Support SLA

[Contact Sales](https://lakefs.io/contact-sales/) to get the token for Fluffy.
{: .note}

## Overview

lakeFS Enterprise solution consists of 2 main components:
1. lakeFS - Open Source: [treeverse/lakeFS](https://hub.docker.com/r/treeverse/lakefs), 
release info found in [Github releases](https://github.com/treeverse/lakeFS/releases). 
2. Fluffy - Proprietary: In charge of the Enterprise features. Can be retrieved from
[Treeverse Dockerhub](https://hub.docker.com/u/treeverse) using the granted token.

You can learn nore about [lakeFS Enterprise architecture]({% link enterprise/architecture.md %}), or 
follow the examples in the [quickstart guide]({% link enterprise/orchestration.md %}).

## Architecture

---
title: Enterprise Architecture
description: Understand  lakeFS Enterprise Architecture
parent: lakeFS Enterprise
---

# Architecture

![img.png](../assets/img/enterprise/enterprise-arch.png)

[1] Any user request to lakeFS via Browser or Programmatic access (SDK, HTTP
API, lakectl).

[2] Reverse Proxy (e.g. NGINX, Traefik, K8S Ingress): will handle user requests
and proxy between lakeFS server and fluffy server based on the path prefix
while maintaining the same host.

[3] lakeFS server - the main lakeFS service.

[4] fluffy server - service that is responsible for the Enterprise features.,
it is separated by ports for security reasons.

1. SSO auth (i.e Browser login via Azure AD, Okta, Auth0), default port 8000.
1. RBAC authorization, default port 9000.

[5] The [KV Store]({% link understand/architecture.md %}) - Where metadata is stored used both by lakeFS and fluffy.

[6] SSO IdP - Identity provider (e.g. Azure AD, Okta, JumpCloud). fluffy
implements SAML and Oauth2 protocols.


For more details and pricing, please [contact sales](https://lakefs.io/contact-sales/).


**Note:** Setting up lakeFS enterprise with an SSO IdP (OIDC, SAML or LDAP) requires
configuring access from the IdP too.
{: .note }