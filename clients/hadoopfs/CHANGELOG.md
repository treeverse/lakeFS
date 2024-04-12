# Changelog

## _Upcoming_

## 0.2.4

lakeFSFS: new Token Provider feature with IAM Role Support for lakeFS authentication (#7659 + #7604)

## 0.2.3

* Fix createDirectoryMarkerIfNotExists (#7510)

## 0.2.2

* Update lakeFS SDK to 1.12.0
* Use overwrite in lakeFSLinker to control If-None-Match

## 0.2.1

* Update lakeFS SDK to 1.0.0

## 0.2.0

### Breaking changes

* New required fields added to StorageConfig API (#6509).  **:warning: This
  is a breaking change:** Only lakeFS server versions >= 0.108.0 are
  supported.

### Bug fixes

* Better handling of quotes and encoding of ETags / checksums, especially
  relevant to Azure (#6756).
* Correctly parse physical URIs that contain whitespace (#5827).

### This release contains significant maintenance changes

None of these are user-visible, but all may be of interest.

* Unit test using mockserver (#6646).  Unit tests now more accurate _and_
  faster.
* Use upcoming future-proof SDK (#6737).  This change servers as an example
  of how you might transition other code to the new future-proof SDK.

  Additionally, :warning: if you do not use an assembled hadoopfs "Über" Jar
  then you will need to change your dependencies to include `io.lakefs:sdk`
  rather than `io.lakefs:api-client`.

## 0.1.15

* Fix presigned mode on Azure fails getting blockstore type (#6028)

## 0.1.14

* Fix bug #5603: fix issues with Delta tables on Databricks 9.1 ML.
* Reduce listObjects calls and results amount (#5683) 

## 0.1.13

* Presigned mode <sup>BETA</sup>: a secure, cloud-agnostic way to use lakeFS with Hadoop.

## 0.1.12

Notable bugfixes:
* Fixing CopyObject -> StageObject fallback mechanism to support old lakeFS versions

## 0.1.11

What's new:
* Upgrade lakeFS client to v0.91.0
* Prefer to use CopyObject instead of StageObject if possible in rename operation.

## 0.1.10

* Performs fewer API calls, leading to some performance improvement.
* More consistent shading: everything now under io.lakefs.hadoopfs.shade

Notable bugfixes:
* Don't link twice when close() is called twice after create() -- support
  lakeFS KV with uncommitted GC.
* Make Hadoop a provided dependency, improving compatibility with some
  existing Spark configurations.

## 0.1.9

Bump jackson-databind library dependency.

## 0.1.8

The FileSystem implementation can now work with any Hadoop version.

To use it, the classpath needs to include the _hadoop-common_ and _hadoop-aws_ packages, including and all their dependencies.

## 0.1.7

## Performance:

* Speed up recursive delete by using deleteObjects.
