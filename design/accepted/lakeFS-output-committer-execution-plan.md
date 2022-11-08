# LakeFS Output Committer - Execution Plan

[LakeFS Output Committer Proposal](https://github.com/treeverse/lakeFS/blob/master/design/open/spark-outputcommitter/committer.md)

## Overview

Implementing lakeFS Output Committer meaning implementing the class [FileOutputCommitter](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/lib/output/FileOutputCommitter.html) and using lakeFS client to create/merge branches as part of setup and commit job/tasks (this is the [sample flow](https://github.com/treeverse/lakeFS/blob/master/design/open/spark-outputcommitter/committer.md#sample-flow)).

### Milestone 1

The first milestone is focused on writing a file with single mode (probably text) and one file format (probably append) ([#4311](https://github.com/treeverse/lakeFS/issues/4311)).
At this stage we'll implement `commit`, `setup`, `abort` class methods.
Moreover, as part of this milestone we'll add testing (the way need to be determined) of both the file format and the writing mode, according to our Hadoop support matrix.

### Milestone 2

This milestone is focused on:
- writing a file in overwrite mode + testing ([#4517](https://github.com/treeverse/lakeFS/issues/4517), [#4528](https://github.com/treeverse/lakeFS/issues/4528))
- writing a file in parquet file format + testing ([#4518](https://github.com/treeverse/lakeFS/issues/4518), [#4527](https://github.com/treeverse/lakeFS/issues/4527))

These should be two separate tasks.

### Milestone 3

This milestone is focused on:
- writing all file formats, in all modes ([#4508](https://github.com/treeverse/lakeFS/issues/4508), [#4509](https://github.com/treeverse/lakeFS/issues/4509))
- implement the relevant remaining class methods ([#4507](https://github.com/treeverse/lakeFS/issues/4507))

The relevant file formats to add are:
* CSV
* ORC
* JSON

The relevant modes to add are:
* ErrorIfExists (default)
* Ignore

### Milestone 4

This milestone is focused on multiwriter support, first for overwrite save mode ([#4510](https://github.com/treeverse/lakeFS/issues/4510)) and then for other save modes ([#4511](https://github.com/treeverse/lakeFS/issues/4511)).

### Other

Several tasks need to be determined in which milestone they should be included:
* Easier configuration: use laekFS Output Committer in default in lakeFSFS
* Improve configuration for Spark 3: better configuration options exist in Spark 3 (default OC for FS).
* Enhance metadata

![Execution Plan](diagrams/lakeFS-OC-execution-plan.png)


