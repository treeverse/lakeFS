---
title: lakeFS Enterprise
description: lakeFS Enterprise is an enterprise-ready lakeFS solution providing additional features including RBAC, SSO and Support SLA.
has_children: true
has_toc: false
parent: Understanding lakeFS
redirect_from:
- /understand/lakefs-enterprise.html
- /enterprise/index.html
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
* Support SLA

Contact Sales (https://lakefs.io/contact-sales/) to get the token for Fluffy.
{: .note}

## Overview

lakeFS Enterprise solution consists of 2 main components:
1. lakeFS - Open Source: [treeverse/lakeFS](https://hub.docker.com/r/treeverse/lakefs), 
release info found in [Github releases](https://github.com/treeverse/lakeFS/releases). 
2. Fluffy - Proprietary: In charge of the Enterprise features. Can be retrieved from
[Treeverse Dockerhub](https://hub.docker.com/u/treeverse) using the granted token.

You can learn nore about [lakeFS Enterprise architecture]({% link understand/enterprise/architecture.md %}), or 
follow the examples in the [quickstart guide]({% link understand/enterprise/orchestration.md %}).

