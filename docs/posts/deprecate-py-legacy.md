---
parent: posts
date: 2024-10-28
search_exclude: true
---

# ⚠️  Deprecating `lakefs-client`, the legacy Python 🐍 Client SDKs

`lakefs-client` is now deprecated, and we will shortly publish its last
release to PyPI.

## What is changing?

Ever since we released lakeFS 1.0, we have supported these two SDKs to program for lakeFS with Python:
- [`lakefs`, the lakeFS High-Level Python SDK][pypi-lakefs-hi-lvl].  This is offers a structured easy way to use lakeFS from lakeFS.
- [`lakefs-sdk`, the lakeFS HTTP API][pypi-lakefs-sdk].  This is
  auto-generated from the lakeFS OpenAPI spec.  It is guaranteed to support
  all lakeFS API actions.  And it is covered by the lakeFS SDK interface
  guarantees.

**These clients will continue.**

Before lakeFS 1.0 we would publish only:

- [`lakefs-client`, a legacy older version of the lakeFS HTTP
  API][pypi-lakefs-legacy].  This too was auto-generated.  However it cannot
  provide interface guarantees.  **At no time could this client ever provide
  forward- or backward- compatibility at the source code level.**  

We have continued to publish `lakefs-client` since the 1.0 release, in order
to provide continued support for existing users of this legacy Python SDK
client.  But there are better alternatives, and its continued presence only
confuses our users.

Now it is time to deprecate this client, and we will stop publishing it at
or after the version 1.40 release of lakeFS.

## FAQ

### What will happen to existing users of `lakefs-client`?

All existing uses will continue to work.  Obviously we will not be pulling
these published versions because of this change.  We will stop publishing
new versions of these clients.

### how do I migrate from using `lakefs-client` to `lakefs-sdk`?

Migration is covered by [Migrating to 1.0][lakefs-py-migration].  In most
cases the changes are quite simple.

### Will new versions of `lakefs-client` be published?

No.

Version 1.40 will be the last release of lakeFS to include a new version of
the legacy client.  In order to use any _new_ features of the lakeFS API,
you will need to upgrade your programs to use `lakefs-sdk`.

### Will programs using old versions of `lakefs-client` continue to work with new versions of lakeFS?

Yes.

A program that uses any 1.x version of `lakefs-client` still uses 1.x
versions of lakeFS APIs.  Its usage of those APIs is still covered by the
lakeFS interface guarantees, so it will continue to work with future 1.x
releases.

### What about the legacy Java SDK generated client?

That is also going away.  However we see much less usage of it.

### Where can I ask another question not covered here?

As always, our Slack htttps://lakefs.io/slack is the best place to interact
with the lakeFS community!  Try asking on our `#dev` channel


[pypi-lakefs-hi-lvl]:  https://pypi.org/project/lakefs/
[pypi-lakefs-sdk]:  https://pypi.org/project/lakefs-sdk/
[pypi-lakefs-legacy]:  https://pypi.org/project/lakefs-client/
[lakefs-py-migration]:  https://docs.lakefs.io/project/code-migrate-1.0-sdk.html#migrating-sdk-code-for-python
