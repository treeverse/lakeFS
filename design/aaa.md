# Identity, Authentication, Authorization (and Audit)

## tl;dr.

Here is what we shall implement initially.  For definitions, the current
state, and rationale for why we choose this starting point, see the rest
of the document.

We shall add an authentication flow that uses an LDAP server.  The first
versions will do only that, creating a user upon authentication.  A user
thus added will automatically be added as a member of some default group
(configurably).  These are regular users apart from being members of the
group.  Administrators will be able to add them to other groups by using
the existing mechanisms.

Only Simple Authentication (username + password) will be supported.  All
other authentication forms (including 2FA and change password flows) may
be added later, as required.

These users will be able to create access keys if they have the required
IAM privileges.  The user and access key continue to work, until deleted
by an administrator.

Synchronizing LDAP and lakeFS users and groups will be performed at some
later point, using the lakeFS API.  This will allow many use-cases, such
as:

* *Propagate* user deletions from LDAP to their access keys.
* *Synchronize* group memberships for lakeFS groups: populate members of
  lakeFS groups from LDAP groups.
* *Synchronize* LDAP groups: ensure exactly LDAP groups matching a query
  exist on lakeFS and are populated from LDAP.

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

This initial design is for authentication (only).

### Authentication

This section will initially apply to GUI clients.  We will have to allow
users to request access keys (or short-term tokens) to allow them to run
programs that use AWS S3 or the lakeFS API.

#### Decision

The initial version will support (only) Simple Authentication on an LDAP
server.  This does not require developing new login screens.

When configured the lakeFS web UI will optionally authenticate users via
Simple Authentication on an LDAP server.  On success it shall ensure the
user exists; if the user does not exist it will create the user, mark as
created via LDAP, and place them in a configured group.  The user gets a
JWT and the flow continues as today.  Selecting between LDAP or internal
login can be done once during the initial login.  After that the web app
can reuse that method, providing a toggle to use the other one.  That is
important to avoid being locked out!

### Authorization

There is room for business logic when authorizing.  Typically users have
properties attached during authentication, which some business logic can
connect to attached policies.  This can be quite complex, but we already
have a groups mechanism in place which we might re-use.

Confusingly, LDAP offers multiple ways to query group membership.  While
many installations offer the `memberOf` attribute on users, there may be
limitations on what exactly it contains.  Microsoft Active Directory has
support [with limitations][ms-ad-memberof].  OpenLDAP supports it, if an
appropriate [overlay][open-ldap-memberof] is configured.  In the reverse
direction, if we are willing to perform operations in advance then _all_
LDAP servers should support reading groups.

In practice `memberOf` is less useful than it seems: group membership at
authentication time is not necessarily up-to-date.  Consider a user with
group memberships at authentication time who creates and starts using an
S3 access key, but never logs in again.  If LDAP is a source of truth of
group memberships then refreshes will be required.

#### Decision

lakeFS shall use locally-configured groups.  Existing IAM shall continue
to work.

In future we may add external facilities to synchronize groups from LDAP
servers into lakeFS, or to query group memberships for groups defined on
lakeFS.  Which we add will depend on user requirements.

### Audit

We currently have no requirements to audit API key usage in any way.  We
can add these on user request -- not at this time.

<!-- references -->
[ms-ad-memberof]: https://ldapwiki.com/wiki/MemberOf#section-MemberOf-BewareOfMemberOf
[open-ldap-memberof]: https://www.openldap.org/doc/admin24/overlays.html#Reverse%20Group%20Membership%20Maintenance
