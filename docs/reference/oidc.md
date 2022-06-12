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

To support OIDC, add the following configurations to your [lakeFS configuration](./configuration.md):

```yaml
auth:
  oidc:
    client_id: example-client-id
    client_secret: exampleSecretValue
    url: https://my-account.oidc-provider-example.com
    default_initial_groups: ["Developers"]
```

Once this configuration is provided, your login page will include a link to sign-in using the 
OIDC provider. When a user logs in through the OIDC provider, a corresponding lakeFS user is created.

#### Notes
{: .no_toc}
1. As always, you may choose to provide these configurations using [environment variables](./configuration.md#using-environment-variables).
2. You may already have other configuration values under the _auth_ key, so make sure you combine them correctly.

## User permissions

Authorization is still managed via [lakeFS groups and policies](./authorization.md).

By default, an externally managed user is assigned to the groups configured in the _default_initial_groups_ property above.
For a user to be assigned to other groups, add the _initial_groups_ claim to their **ID token** claims. The claim should contain a
comma-separated list of group names.

Once the user is created, you can manage their permissions from the Administration pages in the lakeFS UI.
