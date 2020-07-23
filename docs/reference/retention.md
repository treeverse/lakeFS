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

Configuration uses a [JSON configuration][json-ref] input (which can
also be specified in more readable [YAML][yaml-ref]).  For example:

```json
{
  "rules": [
	{
	  "filter": {
		"prefix": "master/logs/"
      },
      "status": "enabled",
      "expiration": {
        "all": {
          "weeks": 2,
          "days": 1
        },
        "noncurrent": {
          "days": 7
        }
      }
    },
    {
      "filter": {
        "prefix": "users/"
      },
      "expiration": {
        "all": {
          "days": 3
        }
      }
    }
  ]
}
```

To view the retention policy for a bucket, use:

```sh
lakectl repo retention get lakefs://repo/
```

To load a new retention policy for a bucket, use:

```sh
lakectl repo retention set lakefs://repo/ --policy-file /path/to/policy.yml
```

You can also specify [YAML format][yaml-ref] with the `--yaml` flag.

### Format

Exact format is given in `swagger.yml` in the definition of
`retention_policy`.  *TODO(ariels):* Document this format properly!

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

  Attempting to open an expired object will fail with HTTP status code
  [410 Gone][http-gone].  This can happen e.g. via using an
  already-known path.

## Filters

Filters consist currently a single type `prefix`.  These is a filename
prefix that must match the object name.  Trailing slashes are treated
as part of the filename, so prefix `/master/logs/` matches only
objects inside "directory" `/master/logs`, but prefix `/master/logs`
will also match objects inside "directory" `/master/logstash/`.

## Action

These action types are supported:
- `expiration`: All currently _committed_ objects (whether latest or
  not) with that prefix will expire after the given length of time.
- `uncommitted_expiration`: Any _uncommitted_ objects with that prefix
  will expire after the given length of time.
- `noncurrent_expiration`: Any "previous" (_not_ currently visible)
  objects will expire after the given length of time.

Each action takes a time specification.  These two time specification
types are supported:
- `days`: Number of days after which to expire.
- `weeks`: Number of weeks after which to expire.

# Differences from S3 Lifecycle Configuration

1. Object lifecycles respect the underlying branch model.
1. Only expiration is supported.
1. Lifecycle is configured in JSON (or YAML) format, not XML.
1. S3 object versioning is not supported by LakeFS (however _LakeFS_
   versions are of course supported).
1. Expiration on a [specific date][s3-lifecycle-specific-date] is not
   supported.
1. Retention filtering on LakeFS currently supports only prefixes; S3
   has additional tag support.

[s3-lifecycle]: https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html
[s3-lifecycle-specific-date]: https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#intro-lifecycle-rules-date
[json-ref]: https://www.json.org/json-en.html
[yaml-ref]: https://yaml.org/spec/1.2/spec.html
[http-gone]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/410
