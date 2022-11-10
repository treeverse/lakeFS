# Changelog

## 0.1.9

Bump jackson-databind library dependency.

## 0.1.8

The FileSystem implementation can now work with any Hadoop version.

To use it, the classpath needs to include the _hadoop-common_ and _hadoop-aws_ packages, including and all their dependencies.

## 0.1.7

## Performance:

* Speed up recursive delete by using deleteObjects.
