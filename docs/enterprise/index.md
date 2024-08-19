---
title: lakeFS Enterprise
description: lakeFS Enterprise is an enterprise-ready lakeFS solution providing additional features including RBAC, SSO and Support SLA.
has_children: true
has_toc: false
nav_order: 110
---

# lakeFS Enterprise

## What is lakeFS Enterprise?

lakeFS Enterprise is a commercially-supported version of lakeFS, offering additional features and functionalities that meet the needs of organizations from a production-grade system.

## Why did we build lakeFS Enterprise?

lakeFS Enterprise was built for organizations that require the support, security standards and features required of a production-grade system and
are not using public clouds, hence they cannot use [lakeFS Cloud]({% link cloud/index.md %}).

## What is the value of using lakeFS Enterprise?

1. Support: the lakeFS team is committed to supporting you under an SLA for both issues and product enhancements.
2. Security: Full support for a suite of security features and additional lakeFS functionality.

## What security features does lakeFS Enterprise provide?

With lakeFS Enterprise you’ll receive access to the security package containing the following features:
1. A rich [Role-Based Access Control]({% link security/rbac.md %}) permission system that allows for fine-grained control by associating permissions with users and groups, granting them specific actions on specific resources. This ensures data security and compliance within an organization.
2. To easily manage users and groups, lakeFS Enterprise provides SSO integration (including support for SAML, OIDC, ADFS, Okta, and Azure AD), supporting existing credentials from a trusted provider, eliminating separate logins.
3. lakeFS Enterprise supports [SCIM]({% link howto/scim.md %}) for automatically provisioning and deprovisioning users and group memberships to allow organizations to maintain a single source of truth for their user database.
4. [STS Auth]({% link security/sts-login.md %}) offers temporary, secure logins using an Identity Provider, simplifying user access and enhancing security.
5. [Authentication with AWS IAM Roles]({% link security/external-principals-aws.md %}) allows authentication using AWS IAM roles instead of lakeFS credentials, removing the need to maintain static credentials for lakeFS Enterprise users running on AWS.
6. [Auditing]({% link reference/auditing.md %}) provides a detailed action log of events happening within lakeFS, including who performed which action, on which resource - and when.

## What additional functionality does lakeFS Enterprise provide?

1. [lakeFS Mount]({% link reference/mount.md %}) allows users to virtually mount a remote lakeFS repository onto a local directory. Once mounted, users can access the data as if it resides on their local filesystem, using any tool, library, or framework that reads from a local filesystem.
2. [Transactional Mirroring]({% link howto/mirroring.md %}) - allows replicating lakeFS repositories into consistent read-only copies in remote locations.

| Feature                                   | OSS       | Enterprise     |
|------------------------------------------------|-----------|-----------|
| **Format-agnostic data version control**       | ✅         | ✅         |
| **Cloud-agnostic**                             | ✅         | ✅         |
| **Zero Clone copy for isolated environment**   | ✅         | ✅         |
| **Atomic Data Promotion (via merges)**         | ✅         | ✅         |
| **Data stays in one place**                    | ✅         | ✅         |
| **Configurable Garbage Collection**            | ✅         | ✅         |
| **Data CI/CD using lakeFS hooks**              | ✅         | ✅         |
| **Integrates with your data stack**            | ✅         | ✅         |
| **[Role Based Access Control (RBAC)](https://docs.lakefs.io/security/rbac.html)** |            | ✅         |
| **[Single Sign On (SSO)](https://docs.lakefs.io/security/sso.html)**                       |            | ✅         |
| **[SCIM Support](https://docs.lakefs.io/howto/scim.html)**                               |            | ✅         |
| **[IAM Roles](https://docs.lakefs.io/security/external-principals-aws.html)**                                  |            | ✅         |
| **[Mount Capability](https://docs.lakefs.io/reference/mount.html)**                           |            | ✅         |
| **[Audit Logs](https://docs.lakefs.io/reference/auditing.html)**                                 |            | ✅         |
| **[Transactional Mirroring (cross-region)](https://docs.lakefs.io/howto/mirroring.html)**     |            | ✅         |
| **Support SLA**                                |            | ✅         |



<br>
You can learn more about the [lakeFS Enterprise architecture]({% link enterprise/architecture.md %}), or follow the examples in the [Quickstart guide]({% link enterprise/getstarted/quickstart.md %}).
