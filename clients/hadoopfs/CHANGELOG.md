# Changelog

## _Upcoming_

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
