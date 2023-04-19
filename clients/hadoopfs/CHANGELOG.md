# Changelog

## _Upcoming_

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
