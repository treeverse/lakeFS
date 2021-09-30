# Identity, Authentication, Authorization (and Audit)

## tl;dr.

Here is what we shall implement initially.  For definitions, the current
state, and rationale for why we choose this starting point, see the rest
of the document.

We shall add an external authentication flow that uses an OAuth2 server,
using standard flows. When configured with an identity provider, clients
will be able to identify against the provider to get a token that claims
some `groups`.  These groups will be identified as regular groups in the
current system, allowing every operation by the user to be verified with
the policies of those groups.

Externally-discovered users will be added to the internal tables so that
they will be able to create access keys.  Memberships of such users only
update on user login -- using the access key continues to use all groups
available to that user when they last logged in.

## IAAA defined

In a system, every operation is performed by some *thing*.  We care what
these things are.  The _identity_ is how we know that thing.  Identities
are a business entity and may belong to people or to bots or to machines
(or whatever the business decides).  At the very least an identity gives
some unique unchanging string.  In lakeFS the identity is the user name.

_Authentication_ is the process of identifying.  Typically, the business
needs include some security around authentication.  The thing may or may
not be directly involved in every authentication.  A human authenticates
to a web service using only periodic direct involvement, and can receive
a token (possibly time-limited) for use.  Authenticating lets the system
identify the thing.  A _credential_ is a piece of controlled information
that allows a thing to authenticate.

Not all things are allowed to perform all operations.  Before allowing a
thing to perform an action the system _authorizes_ it.  Often the system
also leaves some _audit_ trail of authentications (and attempts) as well
as some or all authorizations (and attempts).

## Required and existing AAA

Required support for S3 API means some AAA features must be identical to
those in S3. Our users are almost all S3 users so their familiarity with
other features means we prefer to support those features too.

We should provide support for users with other identity providers.  Most
important of these are Active Directory (and other LDAP) sources.  These
might also serve as a model for possible future integrations but that is
out of scope of this redesign.

### Identity

S3 supports users and roles.  Users are typically human-shaped, machines
and robots identify as their roles.

lakeFS currently has no support for role identities.  It is not clear if
there is any current user demand for such.

### Authentication and credentials

lakeFS performs these types of authentication:

* lakeFS supports the S3 API.  So it must support S3 authentication with
  no changes otherwise existing clients cannot connect.  The common form
  is to use a secret access key credential.  So this form must always be
  supported, and is the only form currently supported.
  
  Other forms of S3 authentication include shorter term tokens.  Another
  protocol for S3 API access uses presigned URLs.

* lakeFS API requires some authentication.  Currently credentials can be
  the AWS-style secret access key or a JWT.  The CLI uses an access key,
  the GUI uses a JWT.  The GUI gets a JWT by using an access key, but it
  is should be possible to change this.

### Authorization

S3 configures authorization using IAM policies.  This is (probably) most
commonly known to our users, and existing installations already use this
form.

There are (many) other kinds of authorization languages we might use but
IAM will probably need to be supported.

On the flip side, the (apparent) lack of extensibility of IAM within AWS
may lead to requiring parallel policies have to be stored within AWS and
lakeFS.  This is made worse by users of lakeFS not necessarily being the
owners of AWS IAM policies on their data when _not_ stored on lakeFS.

### Audit

Current lakeFS performs _no_ identity-linked audit.  Some lines at DEBUG
level give the API action but the access key appears only when is has to
be fetched from the database -- and appears on a separate unlinked line:

```log
TRACE  [2021-09-27T09:46:50+03:00]pkg/db/tx.go:93 pkg/db.(*dbTx).Get SQL query executed successfully               args="[AKIAIOSFODNN7EXAMPLE]" query="SELECT * FROM auth_credentials WHERE auth_credentials.access_key_id = $1" took="987.567µs" type=get
...
DEBUG  [2021-09-27T09:46:50+03:00]pkg/api/controller.go:2912 pkg/api.(*Controller).LogAction performing API action                         action=list_policies host="localhost:8000" message_type=action method=GET path="/api/v1/auth/policies?prefix=&after=&amount=100" request_id=ae2fbb59-f54f-4cb4-a25c-7dd1d7280538 service=api_gateway service_name=rest_api
```

A usable audit log would require at least:

1. Identities not credential IDs.
1. Identities linked to actions.
1. Explicit authentication events linking identities to credentials used
   to authenticate.

## Active Directory and LDAP support

Issue #2058 is to support Active Directory.  This splits naturally along
the 3 A's: use Active Directory to _authenticate_, to _authorize_, or to
receive _audit_ logs of user operations.

### Authentication

This section will initially apply to GUI clients.  We will have to allow
users to request access keys (or short-term tokens) to allow them to run
programs that use AWS S3 or the lakeFS API.

We may authenticate using an LDAP server directly or using some external
application.

* To authenticate directly, the lakeFS web server should use SASL.  That
  may also allow other methods in future.
  
  The advantage here is that *all* LDAP capabilities can be used as long
  as lakeFS understands them.  This probably helps most in enterprises.

* To use external applications, the lakeFS web server should use OAuth2.

  The advantage here is that other OAuth2 providers can be used: GitHub,
  Google Suites, and many others.  LDAP users can still use bridges such
  as Dex to authenticate over OAuth2.  The [Microsoft Connector][dex-ms]
  gives an example.

#### Decision

Add OAuth2 support, as it provides easier integration with more identity
providers by using bridges.  Allow users who authenticate in this way to
create access keys for their accounts, allowing them to use applications
over both the S3 gateway and the lakeFS API.

The lakeFS web UI will (optionally) authenticate users using OAuth2, and
place the result on their JWT.  Selecting between OAuth2 or internal can
be done once during the initial login.  After that the web app can reuse
that method.

### Authorization

There is room for business login when authorizing.  Typically users have
properties attached during authentication, which some business logic can
connect to attached policies.  This can be quite complex, but we already
have a groups mechanism in place which we might re-use.

When do we attach policies do these groups?  Configuration changes apply
at different times depending on this decision.  Authenticated users will
see only changes in mappings that are attached before the change.

#### Decision

lakeFS shall receive groups from OAuth2 and encode them as claims on the
JWT.  The remaining logic can proceed as today.

This business logic is quite simple.

### Audit

OAuth2 has no audit mechanism and currently there is no request to allow
such an audit.  Accordingly we shall not currently add audit logs.

<!-- references -->
[dex-ms]: https://dexidp.io/docs/connectors/microsoft/
