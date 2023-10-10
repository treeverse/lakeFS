# lakeFS SDK in 1.0

<img src="./images/1.0-is-coming.jpg" height="500px"
	alt="1.0 is coming: a double rainbow over a traffic jam in Tel Aviv"/>

## Goals

We are putting the finishing touches on the :sparkles: lakeFS 1.0 release
:sparkles:.  Part of the reason 1.0 is so important is that it promises
stability to lakeFS users and to lakeFS developers.

lakeFS version numbers generally follow [Semantic
Versioning](https://semver.org/).  Version **1.0.0** will be the first major
version, so it will be the first version to promise _major version
stability_.  A brief summary of semantic versioning: Starting with this
version,

* **Patch** version upgrades (for instance, from 1.0.0 to 1.0.1, or from
  1.3.11 to 1.3.12) can fix bugs but cannot add new API capabilities.  All
  programs that worked with the previous version continue to work with the
  new version.  All programs that work with the new version and are
  unaffected by the fixed bugs would work with the old version.
* **Minor** version upgrades (for instance, from 1.0.0 to 1.1.0, or from
  1.3.11 to 1.4.0) can add API capabilities but cannot change existing APIs.
  All programs that worked with the previous version continue to work with
  the new version.
* **Major** version upgrades (for instance, from 1.4.0 to 2.0.0, or from
  1.17.12 to 2.0.0) can add, remove, or change existing APIs entirely.  A
  program might stop working.

So it's a pretty important change!  Up until now, even a bump from 0.104.0
to 0.105.0 could break old programs.  Indeed that happened.  Starting with
1.0.0, a minor version that breaks old programs is a bug.

## 1.0.0 is a **major** version upgrade!

All guarantees here are for minor version upgrades.  The upgrade to 1.0.0 is
of course a **major version upgrade**, and it will necessarily break some
client programs.

## Interface guarantees

To detail what 1.0 really promises, we need to list what are the lakeFS
interfaces.  Obviously a program that relies on an internal implementation
detail of lakeFS can fail at any version.

Starting with the 1.0 release, we will maintain stability across the
following interfaces:

* Any call to the [REST OpenAPI API
  endpoint](https://docs.lakefs.io/reference/api.html) is stable, except for
  APIs tagged "experimental", "internal", or "deprecated".

  If your program only calls such SDKs, it will continue to work across
  minor version upgrades.[^1]
* Any call to such an interface through the published, generated SDKs.
  These include:

  * [Python `lakefs-sdk`](https://pypi.org/project/lakefs-sdk/)
  * [Java Maven `io.lakefs:lakefs-sdk`](https://search.maven.org/artifact/io.lakefs/sdk)

  If your program only uses such calls on the SDK, it will continue to
  compile and function across a minor version upgrade of the SDK.
* Any objects that were uploaded to some branch on a lakeFS server and then
  committed will continue to work after a minor version upgrade to the
  lakeFS server.  Running an upgrade script may be required.  However
  running this upgrade script on a cluster of more than a single server
  should not require downtime.[^2]
* The lakectl CLI flags will continue to work across minor version upgrades.
  However we do not currently guarantee the format of lakectl output.
* Obviously we do _not_ control the S3 API or its many clients.  If your
  program uses an S3 API that lakeFS supports, it should continue to work
  across a minor version upgrade of lakeFS.  A minor version upgrade will
  not remove support for a documented S3 API that is supported in the
  previous version.

## Why this is important

### For users

Programs that worked with one version >=1.0.0 of lakeFS should continue to
work with all higher versions <2.0.0.  A minor version upgrade only _adds_
to your capabilities, but you do not need to do anything if you don't use
the added capabilities.

### For developers

Don't use APIs tagged "experimental", "internal", or "deprecated".  If you
do use them, the rest of this section does not apply.  Take into account
that you may need to modify your program to match any lakeFS server upgrade.

Your program should continue to work with all lakeFS server versions that
are minor version upgrades from the version you used to develop them.

Additionally, if you depend on one of the published, generated SDKs, then
that dependency itself also honours semantic versioning.  Switching your
program to a minor version upgrade of the SDK dependency should be seamless.

#### Java

For Java and for other languages running on the JVM, the current generated
SDK is *not* stable with respect to a minor version upgrade.  The upgrade
from `io.lakefs:lakefs-client` to `io.lakefs:lakefs-sdk` will require you to
rewrite API calls in a new style.

Here is [one example of such a change][lakefsfs-new-sdk-sample].
Old calls looked like this:

```java
ObjectStats objectStat = objectsApi.statObject(
    objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath(),
    false, false);
```

A single call was a single function call, which had to include optional
parameters.  If an optional parameter were added, this call would no longer
compile.  The long argument lists were also a maintenance burden.

Conceptually, API calls in the SDK now follow a fluent style:

```java
ObjectStats objectStat = objectsApi
    .statObject(
        objectLoc.getRepository(), objectLoc.getRef(), objectLoc.getPath()
	)
	.userMetadata(true)
	.execute();
```

The `statObject` call of the objects API starts by passing all required
parameters.  Then any optional parameters may be changed.  Here
`userMetadata` is modified, but `presign` is kept at its default value.
Finally, the call is _executed_.  When an optional parameter is added, this
call will simply use its default value.  This keeps the code compatible with
minor version upgrades of the server.

### For administrators

Your users and your developers no longer need to keep their program versions
in lockstep with your lakeFS server versions, as long as you **apply only
minor version upgrades**.

And if your cluster of lakeFS servers has more than one member, you should
be able to perform most minor version upgrades with no downtime.


[^1]: Unless it depends on a bug that was fixed, of course.
[^2]: Of course this may not be possible with some severe bugfixes.
    Requiring server downtime is a measure of the severity of the bugfix.

[lakefsfs-new-sdk-sample]:  https://github.com/treeverse/lakeFS/pull/6529/files#diff-4c50b9ac3bf6bfc05e3b6ff0fbe2fd3214f31afb5b449732d90efe5f97f67167R666
