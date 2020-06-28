---
layout: default
title: Object Retention
parent: Reference
nav_order: 10
has_children: false
---
# Object Retention

LakeFS allows configuration of object retention policies using a
lifecycle configuration.  Concepts are similar to [S3 lifecycle
configuration][s3-lifecycle], however not all properties exist.  Most
notably current support is only for object expiration and not storage
class transition.

# Retention

## Configuration

Configuration uses a [YAML configuration][yaml-ref] file.  For example:
```yaml
---
rules:
  - filter:
      prefix: master/logs/
    status: enabled
    expiration:
      weeks: 2
    noncurrent_expiration:
      days: 7
  - filter:
      prefix: users/
    expiration:
      days: 3
```

## Operation

The command `lakefs expire` checks and expires any matching objects.
Make sure it runs occasionally (usually once per day).  Any expired
objects are removed from underlying storage.

## Canonical object names

An object can be seen from multiple branches.  However every visible
object was committed to a _single_ branch.  The path to the object
using that branch is the _canonical_ object name.  Retention rules
apply only according to that canonical name.

For example, if branch `staging` is rooted in branch `master` then
objects are visible in `staging` if they were committed to that branch
or to `master` and were not deleted from `staging`.  Objects expire
according to the branch to which they were committed:
- Objects committed to `staging` expire according to rules for prefix
  `staging/`.  After expiring, an object with the same name in
  `master` will become visible, or there will be no object visible
  with that name.
- Objects committed to `master` expire according to rules for prefix
  `master/`, but _not_ for prefix `staging/`.  After expiring, no
  object will be visible with that name.

## Filters

Filters consist currently of a two types, `prefix` and `uncommitted`.
This is a filename prefix that must match the object name.  Trailing
slashes are treated as part of the filename, so prefix `/master/logs/`
matches only objects inside "directory" `/master/logs`, but prefix
`/master/logs` will also match objects inside "directory"
`/master/logstash/`.

## Action

These 2 action types are supported:
- `expiration`: The "current" (currently visible) object with that
  prefix will expire after the given length of time, whether committed
  or not.
- `noncurrent_expiration`: Any "previous" (_not_ currently visible)
  objects that would previously have been visible will expire after
  the given length of time.

Each action takes a time specification.  These two time specification
types are supported:
- `days`: Number of days after which to expire.
- `weeks`: Number of weeks after which to expire.

# Differences from S3 Lifecycle Configuration

1. Object lifecycles respect the underlying branch model.
1. Only expiration is supported.
1. Lifecycle is configured in YAML format, not XML.
1. S3 object versioning is not supported by LakeFS (however _LakeFS_
   versions are of course supported).
1. Expiration on a [specific date][s3-lifecycle-specific-date] is not
   supported.
1. Tags are not currently supported on LakeFS.

[s3-lifecycle]: https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html
[s3-lifecycle-specific-date]: https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#intro-lifecycle-rules-date
[yaml-ref]: https://yaml.org/spec/1.2/spec.html
