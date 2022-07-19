---
layout: default
title: OIDC support
description: Use an OIDC provider to manage lakeFS users.
parent: Reference
nav_order: 67
has_children: false
---

# OIDC support

{: .no_toc }

You can manage lakeFS users externally using an identity provider compatible with OpenID Connect (OIDC).

{% include toc.html %}

## Configuring lakeFS server for OIDC

To support OIDC, add the following to your [lakeFS configuration](./configuration.md):

```yaml
auth:
  oidc:
    enabled: true
    client_id: example-client-id
    client_secret: exampleSecretValue
    callback_base_url: https://lakefs.example.com       # The scheme, domain (and port) of your lakeFS installation
    url: https://my-account.oidc-provider-example.com
    default_initial_groups: ["Developers"]
    friendly_name_claim_name: name                      #  Optional: use the value from this claim as the user's display name 
```

Your login page will now include a link to sign in using the 
OIDC provider. When a user first logs in through the provider, a corresponding user is created in lakeFS.

#### Notes
{: .no_toc}
1. As always, you may choose to provide these configurations using [environment variables](./configuration.md#using-environment-variables).
2. You may already have other configuration values under the _auth_ key, so make sure you combine them correctly.

## User permissions

Authorization is still managed via [lakeFS groups and policies](./authorization.md).

By default, an externally managed user is assigned to the lakeFS groups configured in the _default_initial_groups_ property above.
For a user to be assigned to other groups, add the _initial_groups_ claim to their **ID token** claims. The claim should contain a
comma-separated list of group names.

Once the user has been created, you can manage their permissions from the Administration pages in the lakeFS UI or using _lakectl_.

### Using a different claim name

To supply the initial groups using another claim from your ID token, you can use the `auth.oidc.initial_groups_claim_name` 
lakeFS configuration. For example, to take the initial groups from the _roles_ claim, add:

```yaml
auth:
  oidc:
    # ... Other OIDC configurations
    initial_groups_claim_name: roles
```
