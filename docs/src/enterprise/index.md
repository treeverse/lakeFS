---
title: lakeFS Enterprise Features
description: lakeFS Enterprise is an enterprise-ready lakeFS solution providing additional features including RBAC, SSO and Support SLA.
status: enterprise
---

# lakeFS Enterprise Features

## What is lakeFS Enterprise?

lakeFS Enterprise is a commercially-supported version of lakeFS, offering additional features and functionalities that meet the needs of organizations from a production-grade system.

## Why did we build lakeFS Enterprise?

lakeFS Enterprise was built for organizations that require the support, security standards and features required of a production-grade system.

## What is the value of using lakeFS Enterprise?

1. Support: the lakeFS team is committed to supporting you under an SLA for both issues and product enhancements.
2. Security: Full support for a suite of security features and additional lakeFS functionality.
3. Advanced Functionality for AI, ML and Data engineering use cases. See [Features](#lakefs-enterprise-features) below.

## What security features does lakeFS Enterprise provide?

With lakeFS Enterprise you’ll receive access to the security package containing the following features:

1. A rich [Role-Based Access Control](../security/rbac.md) permission system that allows for fine-grained control by associating permissions with users and groups, granting them specific actions on specific resources. This ensures data security and compliance within an organization.
1. To easily manage users and groups, lakeFS Enterprise provides SSO integration (including support for SAML, OIDC, ADFS, Okta, and Azure AD), supporting existing credentials from a trusted provider, eliminating separate logins.
3. lakeFS Enterprise supports [SCIM](../howto/scim.md) for automatically provisioning and de-provisioning users and group memberships, allowing organizations to maintain a single source of truth for their user database.
1. [STS Auth](../security/sts-login.md) offers temporary, secure logins using an Identity Provider, simplifying user access and enhancing security.
1. [Authentication with AWS IAM Roles](../security/external-principals-aws.md) allows authentication using AWS IAM roles instead of lakeFS credentials, removing the need to maintain static credentials for lakeFS Enterprise users running on AWS.
1. [Auditing](../reference/auditing.md) provides a detailed action log of events happening within lakeFS, including who performed which action, on which resource - and when.

## What additional functionality does lakeFS Enterprise provide?

1. [lakeFS Mount](../reference/mount.md) - allows users to virtually mount a remote lakeFS repository onto a local directory. Once mounted, users can access the data as if it resides on their local filesystem, using any tool, library, or framework that reads from a local filesystem.
1. [lakeFS Metadata Search](https://info.lakefs.io/metadata-search) - Allows a granular search API to filter and query versioned objects based on attached metadata. This is especially useful for machine learning environments to filter by labels and file attributes
1. [Iceberg REST Catalog](../integrations/iceberg.md) - Provides full support for managing Iceberg tables alongside other data formats in the same lakeFS repository. Built using open standards and works with any Iceberg client.
1. [lakeFS for Snowflake](https://info.lakefs.io/lakefs-for-snowflake) - Provides full integration into the Snowflake ecosystem, including full support for Iceberg managed tables.
1. [Transactional Mirroring](../howto/mirroring.md) - allows replicating lakeFS repositories into consistent read-only copies in remote locations.
1. [Multiple Storage Backends](../howto/multiple-storage-backends.md) - allows managing data stored across multiple storage locations: on-prem, hybrid, or multi-cloud.        


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
| **[Role Based Access Control (RBAC)](../security/rbac.md)** |            | ✅         |
| **[Single Sign On (SSO)](../security/sso.md)**                       |            | ✅         |
| **[SCIM Support](../howto/scim.md)**                               |            | ✅         |
| **[IAM Roles](../security/external-principals-aws.md)**                                  |            | ✅ <small>(AWS)</small>         |
| **[Mount Capability](../reference/mount.md)**                           |            | ✅         |
| **[Iceberg REST Catalog](../integrations/iceberg.md)**                           |            | ✅         |
| **[Audit Logs](../reference/auditing.md)**                                 |            | ✅         |
| **[Transactional Mirroring (cross-region)](../howto/mirroring.md)**     |            | ✅         |
| **Support SLA**                                |            | ✅         |



!!! tip
    You can learn more about the [lakeFS Enterprise architecture](./architecture.md), or follow the examples in the [Quickstart guide](./getstarted/quickstart.md).
