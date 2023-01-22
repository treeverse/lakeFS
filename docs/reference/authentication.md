---
layout: default
title: Authentication 
description: This section covers Authentication of your lakeFS server.
parent: Reference
nav_order: 60
has_children: false
---

# Authentication 

{: .no_toc }

{% include toc.html %}

## Authentication

### User Authentication

lakeFS authenticates users from a built-in authentication database, or,
optionally, from a configured LDAP server.

#### Built-in database

The built-in authentication database is always present and active. You can use the
Web UI at Administration / Users to create users. Users have an access key
`AKIA...` and an associated secret access key. These credentials are valid
for logging into the Web UI or authenticating programmatic requests to the API
Server or the S3 Gateway.

#### LDAP server

**Note**
This feature is deprecated. For single sign-on with lakeFS, try [lakeFS cloud](https://lakefs.cloud)
{: .note }

Configure lakeFS to authenticate users on an LDAP server. Once configured,
users can additionally log into lakeFS using their credentials LDAP. These
users may then generate an access key and a secret access key on the Web UI
at Administration / My Credentials. lakeFS generates an internal user once
logged in via the LDAP server. Adding this internal user to a group allows
assigning them a different policy.

Configure the LDAP server using the [configuration fields](../reference/configuration.md):

* `server_endpoint`: the `ldaps:` (or `ldap:`) URL of the LDAP server.
* `bind_dn`, `bind_password`: Credentials for lakeFS to use to query the
  LDAP server for users. They must identify a user with Basic
  Authentication, and are used to convert a user ID attribute to a full
  user DN.
* `default_user_group`: A group to add users the first time they log in
  using LDAP.  Typically "`Viewers`" or "`Developers`".

  Once logged in, LDAP users may be added as normal to any other group.
* `username_attribute`: Attribute on LDAP user to identify user when
  logging in.  Typically "`uid`" or "`cn`".
* `user_base_dn`: DN of root of DAP tree containing users,
  e.g. `ou=Users,dc=treeverse,dc=io`.
* `user_filter`: An additional filter for users allowed to login,
  e.g. `(objectClass=person)`.

LDAP users log in using the following flow:

1. Bind the lakeFS control connection to `server_endpoint` using `bind_dn`,
   `bind_password`.
1. Receive an LDAP user-ID (e.g. "joebloggs") and a password entered on the
   Web UI login page.
1. Attempt to log in as internally-defined users; fail.
1. Search the LDAP server using the control connection for the user: out of
   all users under `user_base_dn` that satisfy `user_filter`, there must be
   a single user whose `username_attribute` was specified by the user. Get
   their DN.

   In our example, this may be `uid=joebloggs,ou=Users,dc=treeverse,dc=io`
   (this entry must have `objectClass: person` because of `user_filter`).
1. Attempt to bind the received DN on the LDAP server using the password.
1. On success, the user is authenticated.
1. Create a new internal user with that DN if needed. When creating a user,
   add them to the internal group named `default_user_group`.

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

See [this example for authenticating with the AWS CLI](../integrations/aws_cli.md).


## OIDC support

**Note**
This feature is deprecated. For single sign-on with lakeFS, try [lakeFS cloud](https://lakefs.cloud)
{: .note }

OpenID Connect (OIDC) is a simple identity layer on top of the OAuth 2.0 protocol.
You can configure lakeFS to enable OIDC to manage your lakeFS users externally. 
Essentially, once configured, this enables you the benefit of OpenID connect, such as a single sign-on (SSO), etc. 

### Configuring lakeFS server for OIDC

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

Authorization is managed via [lakeFS groups and policies](./authorization.md).

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
