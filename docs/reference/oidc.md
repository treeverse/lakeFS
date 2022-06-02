---
layout: default
title: OIDC support
description: Use an OIDC provider to manage lakeFS users
parent: Reference
nav_order: 67
has_children: false
---

# OIDC support

{: .no_toc }

You can manage lakeFS users externally using an OpenID Connect (OIDC) compatible identity provider.

{% include toc.html %}

## Configuring lakeFS server for OIDC

To support OIDC, add the following configurations to your [lakeFS configuration](./configuration.md).
As always, you may choose to provide these configurations using [environment variables](./configuration.md#using-environment-variables).

```yaml
auth:
  oidc:
    client_id: example-client-id
    client_secret: exampleSecretValue
    domain: https://my-account.oidc-provider-example.com
```

Note that you may have other configuration values under the `auth` key, so make sure you combine them correctly.

## Adding policy claims to your users

## Logging in using OIDC

## Limitations

- Groups are not currently supported for externally managed users.
- Creating programmatic access credentials is not supported for externally managed users.
