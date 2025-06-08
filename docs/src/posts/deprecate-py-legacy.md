---
date: 2024-10-28
search:
  exclude: true
---

# Deprecating `lakefs-client`, the legacy Python 🐍 Client SDKs

The `lakefs-client` is now deprecated; the last release published to PyPI was v1.44.0.

## What is changing?

Ever since we released lakeFS 1.0, we have supported these two SDKs to program for lakeFS with Python:
- [`lakefs`, the lakeFS High-Level Python SDK][pypi-lakefs-hi-lvl].  This is offers a structured easy way to use lakeFS from lakeFS.
- [`lakefs-sdk`, the lakeFS HTTP API][pypi-lakefs-sdk].  This is
  auto-generated from the lakeFS OpenAPI spec.  It is guaranteed to support
  all lakeFS API actions.  And it is covered by the lakeFS SDK interface
  guarantees.

**These clients will continue.**

Before lakeFS 1.0 we would publish only:

- [`lakefs-client`, a legacy older version of the lakeFS HTTP API][pypi-lakefs-legacy].
  This too was auto-generated. However it can't provide interface guarantees.
  **At no time could this client ever provide forward- or backward- compatibility at the source code level.**  

We have continued to publish `lakefs-client` since the 1.0 release, in order
to provide continued support for existing users of this legacy Python SDK
client. But there are better alternatives, and its continued presence only
confuses our users.

## FAQ

### What will happen to existing users of `lakefs-client`?

Current and previously available versions will remain accessible and functional despite upcoming changes.
These existing releases will not be withdrawn.
However, no further updates or new editions of these client versions will be published moving forward.

### How do I migrate from using `lakefs-client` to `lakefs-sdk`?

Transitioning to version 1.0 is detailed in the [Migrating to 1.0][lakefs-py-migration] guide.
Typically, the adjustments required are straightforward.

### Will programs using old versions of `lakefs-client` continue to work with new versions of lakeFS?

Yes.

A program that uses any 1.x version of `lakefs-client` still uses 1.x
versions of lakeFS APIs.  Its usage of those APIs is still covered by the
lakeFS interface guarantees, so it will continue to work with future 1.x
releases.

### Where can I ask another question not covered here?

As always, our Slack https://lakefs.io/slack is the best place to interact
with the lakeFS community!  Try asking on our `#dev` channel


[pypi-lakefs-hi-lvl]:  https://pypi.org/project/lakefs/
[pypi-lakefs-sdk]:  https://pypi.org/project/lakefs-sdk/
[pypi-lakefs-legacy]:  https://pypi.org/project/lakefs-client/
[lakefs-py-migration]:  https://docs.lakefs.io/project/code-migrate-1.0-sdk.html#migrating-sdk-code-for-python
