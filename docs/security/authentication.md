---
title: Authentication 
description: This section covers Authentication of your lakeFS server.
parent: Security
redirect_from:
  - /reference/authentication.html
---

# Authentication 

{% include toc_2-3.html %}

## Authentication

### User Authentication

lakeFS authenticates users from a built-in authentication database.

#### Built-in database

The built-in authentication database is always present and active. You can use the
Web UI at Administration / Users to create users. Users have an access key
`AKIA...` and an associated secret access key. These credentials are valid
for logging into the Web UI or authenticating programmatic requests to the API
Server or the S3 Gateway.

#### Remote Authenticator Service 

lakeFS server supports external authentication, the feature can be configured by providing an HTTP endpoint to an external authentication service. This integration can be especially useful if you already have an existing authentication system in place, as it allows you to reuse that system instead of maintaining a new one.
To configure a Remote Authenticator see the [configuration fields]({% link reference/configuration.md %}#authentication-and-authorization).

### API Server Authentication

Authenticating against the API server is done using a key-pair, passed via [Basic Access Authentication](https://en.wikipedia.org/wiki/Basic_access_authentication).

All HTTP requests must carry an `Authorization` header with the following structure:

```text
Authorization: Basic <base64 encoded access_key_id:secret_access_key>
```

For example, assuming my access_key_id is `my_access_key_id` and my secret_access_key is `my_secret_access_key`, we'd send the following header with every request:

```text
Authorization: Basic bXlfYWNjZXNzX2tleV9pZDpteV9hY2Nlc3Nfc2VjcmV0X2tleQ==
```


### S3 Gateway Authentication

To provide API compatibility with Amazon S3, authentication with the S3 Gateway supports both [SIGv2](https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html){:target="_blank"} and [SIGv4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html){:target="_blank"}.
Clients such as the AWS SDK that implement these authentication methods should work without modification.

See [this example for authenticating with the AWS CLI]({% link integrations/aws_cli.md %}).


## OIDC support

**Note**
This feature is deprecated. For single sign-on with lakeFS, try [lakeFS Cloud](https://lakefs.cloud)
{: .note }

OpenID Connect (OIDC) is a simple identity layer on top of the OAuth 2.0 protocol.
You can configure lakeFS to enable OIDC to manage your lakeFS users externally. 
Essentially, once configured, this enables you the benefit of OpenID connect, such as a single sign-on (SSO), etc. 

### Configuring lakeFS server for OIDC

To support OIDC, add the following to your [lakeFS configuration]({% link reference/configuration.md %}):

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
    persist_friendly_name: true                         #  Optional: persist friendly name to KV store so it can be displayed in the user list
```

Your login page will now include a link to sign in using the 
OIDC provider. When a user first logs in through the provider, a corresponding user is created in lakeFS.

#### Friendly Name Persistence

When the `persist_friendly_name` configuration property is set to `true` **and** `friendly_name_claim_name` is set to a valid claim name, which exists in the incoming `id_token`, the friendly name will be persisted to the KV store. This will allow users with access to the lakeFS administration section to see friendly names in the users list, when listing group members, and when adding/removing group members.  
The friendly name stored in KV is updated with each successful login, if the incoming value is different than the stored value. This means it will be kept up-to-date with changes to the user's profile or if `friendly_name_claim_name` is re-configured.

#### Notes
{: .no_toc}
1. As always, you may choose to provide these configurations using [environment variables]({% link reference/configuration.md %}).
2. You may already have other configuration values under the _auth_ key, so make sure you combine them correctly.

## User permissions

Authorization is managed via [lakeFS groups and policies]({% link security/rbac.md %}}).

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
