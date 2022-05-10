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

lakeFS authenticates users from a built-in authentication database, or
optionally from a configured LDAP server.

#### Built-in database

The built-in authentication database is always present and active.  Use the
Web UI at Administration / Users to create users.  Users have an access key
`AKIA...` and an associated secret access key.  These credentials are valid
to log into the Web UI, or to authenticate programmatic requests to the API
Server or the S3 Gateway.

#### LakeFS Invite

Lakefs supports inviting users via email.

If you were invited to lakeFS, follow these steps to create and activate your account:

1. Locate the email sent to you (be aware it may be in your spam folder).
1. Click on the link in the email to open the account activation page.
1. Set your account password.
1. Now you can login to the GUI using your email and the password you created for your account in the previous step.

You can now generate an access key and a secret access key on the Web UI
at Administration / My Credentials. Use the email / password for login to 
the GUI only.


#### LDAP server

Configure lakeFS to authenticate users on an LDAP server.  Once configured,
users can additionally log into lakeFS using their credentials LDAP.  These
users may then generate an access key and a secret access key on the Web UI
at Administration / My Credentials.  lakeFS generates an internal user once
logged in via the LDAP server.  Adding this internal user to a group allows
assigning them a different policy.

Configure the LDAP server using these [fields in
`auth.ldap`](configuration.html#ldap):

* `server_endpoint`: the `ldaps:` (or `ldap:`) URL of the LDAP server.
* `bind_dn`, `bind_password`: Credentials for lakeFS to use to query the
  LDAP server for users.  These must identify a user with Basic
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
   a single user whose `username_attribute` was specified by the user.  Get
   their DN.

   In our example this might be `uid=joebloggs,ou=Users,dc=treeverse,dc=io`
   (this entry must have `objectClass: person` because of `user_filter`).
1. Attempt to bind the received DN on the LDAP server using the password.
1. On success, the user is authenticated!
1. Create a new internal user with that DN if needed.  When creating a user
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

