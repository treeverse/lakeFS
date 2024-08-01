---
title: Security
description: Security reference documentation for lakeFS
parent: Reference
has_children: true
has_toc: false
---

# lakeFS Security Reference

<img src="/assets/img/docs_logo.png" alt="lakeFS Docs" width=200 style="float: right; margin: 0 0 10px 10px;"/>

### Understanding Your Data Security ###

At lakeFS, we understand the critical nature of data security. Thousands of organizations worldwide rely on lakeFS to manage their data with confidence. Here's a few concepts we follow to ensure your data remains secure:

**Data Stays in Place**: The data you version control remains within your existing object storage. lakeFS creates metadata for your data without moving it. New data is stored in the bucket you designate within your object storage.

**lakeFS Servers Stores only Metadata**: The lakeFS Server (also in the case of lakeFS Cloud) only stores the metadata used for version control operations (diff, merge). It does not store any of your actual data, and if configured correctly, doesn't even access it directly.

**Minimal Permissions**: lakeFS requires minimal permissions to manage your data. We can even [version data we cannot directly access](https://lakefs.io/blog/pre-signed-urls/) by utilizing presigned URLs.

Learn more about some of the featured that help keep lakeFS Secure:

- [Authentication]({% link reference/security/authentication.md %})
- [Remote Authenticator]({% link reference/security/remote-authenticator.md %})
- [Role-Based Access Control (RBAC)]({% link reference/security/rbac.md %})
- [Presigned URL]({% link reference/security/presigned-url.md %})
- [Single Sign On (SSO)]({% link reference/security/sso.md %})
- [Short Lived Token (STS like)]({% link reference/security/sts-login.md %})
- [Login to lakeFS with AWS IAM]({% link reference/security/external-principals-aws.md %})

### More questions? Contact us ###
If you have additional questions regarding lakeFS Security, [talk to an expert](https://meetings.hubspot.com/iddo-avneri/lakefs-security-questions).