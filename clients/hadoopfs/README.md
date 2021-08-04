# lakeFS Hadoop FileSystem

An implementation of `org.apache.hadoop.fs.FileSystem`, the lakeFS Hadoop Filesystem allows to run Spark jobs on lakeFS with data operations performed directly on the underlying storage.
It uses the lakeFS server for metadata operations only.

## Publishing

Use the Github [Action](https://github.com/treeverse/lakeFS/actions/workflows/publish-hadoop-lakefs.yaml) to publish a new version to Maven Central.
