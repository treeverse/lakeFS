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

lakeFS Enterprise is an enterprise-ready lakeFS solution that provides a support SLA and additional features to the open-source version of lakeFS. The additional features are:

* [RBAC]({% link reference/security/rbac.md %})
* [SSO]({% link reference/security/sso.md %})
* [STS Auth]({% link reference/security/sts-login.md %})
* [External Principals AWS Auth]({% link reference/security/external-principals-aws.md %})
* Support SLA

## Overview

lakeFS Enterprise solution consists of 2 main components:
1. lakeFS - Open Source: [treeverse/lakeFS](https://hub.docker.com/r/treeverse/lakefs), 
release info found in [Github releases](https://github.com/treeverse/lakeFS/releases). 
2. Fluffy - Proprietary: In charge of the Enterprise features. Can be retrieved from
[Treeverse Dockerhub](https://hub.docker.com/u/treeverse) using the granted token.

You can learn nore about [lakeFS Enterprise architecture]({% link understand/enterprise/architecture.md %}), or 
follow the examples in the [quickstart guide]({% link understand/enterprise/orchestration.md %}).

