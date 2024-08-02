---
title: Security
description: Security reference documentation for lakeFS
has_children: true
has_toc: false
---

# lakeFS Security Reference

<img src="/assets/img/docs_logo.png" alt="lakeFS Docs" width=200 style="float: right; margin: 0 0 10px 10px;"/>

### Understanding Your Data Security ###

At lakeFS, we understand the critical nature of data security. Thousands of organizations worldwide rely on lakeFS to manage their data with confidence. Here's a few concepts we follow to ensure your data remains secure:

**Data Stays in Place**: The data you version control remains within your existing object storage. lakeFS creates metadata for your data without moving it. New data is stored in the bucket you designate within your object storage.

**lakeFS Servers Stores only Metadata**: The lakeFS Server (also in the case of lakeFS Cloud) only stores the metadata used for version control operations (i.e. diff, merge). It does not store any of your actual data.

**Minimal Permissions**: lakeFS requires minimal permissions to manage your data. We can even [version data we cannot directly access](https://lakefs.io/blog/pre-signed-urls/) by utilizing presigned URLs.

Learn more about some of the featured that help keep lakeFS Secure:

- [Authentication]({% link security/authentication.md %}) - An overview of the authentication and authorization mechanisms available in lakeFS, including built-in and external services, API and S3 Gateway authentication, and user permissions management.
- [Remote Authenticator]({% link security/remote-authenticator.md %}) - This feature allows organizations to leverage their existing identity infrastructure while using lakeFS, providing a flexible and secure authentication mechanism.
- [Role-Based Access Control (RBAC)]({% link security/rbac.md %}) - RBAC with lakeFS provides a flexible and granular approach to managing access and permissions, similar to other cloud-based systems like AWS IAM.
- [Presigned URL]({% link security/presigned-url.md %}) - This feature allows for more flexible and direct data access in lakeFS, particularly useful for scenarios where bypassing the lakeFS server for data retrieval or storage is beneficial.
- [Single Sign On (SSO)]({% link security/sso.md %}) - lakeFS provides administrators with the necessary information to set up SSO for both lakeFS Cloud and Enterprise editions, covering various authentication protocols and identity providers.
- [Short Lived Token (STS like)]({% link security/sts-login.md %}) - This feature allows lakeFS to leverage temporary credentials for secure and flexible authentication, integrating seamlessly with existing identity providers.
- [Login to lakeFS with AWS IAM]({% link security/external-principals-aws.md %}) - This feature enhances the integration between lakeFS and AWS by supporting authenticating users programmatically using AWS IAM roles instead of using static lakeFS access and secret keys.

### SOC2 Compliance ###
lakeFS Cloud is SOC2 compliant, demonstrating our commitment to stringent security standards.

### More questions? Contact us ###
For details on supported versions, security updates, and vulnerability reporting, please refer to our [security policy on GitHub]( https://github.com/treeverse/lakeFS/security/policy).

If you have additional questions regarding lakeFS Security, [talk to an expert](https://meetings.hubspot.com/iddo-avneri/lakefs-security-questions).