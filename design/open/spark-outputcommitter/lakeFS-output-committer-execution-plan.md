# LakeFS Output Committer - Execution Plan
[LakeFS Output Committer Proposal](https://github.com/treeverse/lakeFS/blob/master/design/open/spark-outputcommitter/committer.md)

## Overview
Implementing lakeFS Output Committer meaning implementing the class [FileOutputCommitter](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/lib/output/FileOutputCommitter.html) and using lakeFS client to create/merge branches as part of setup and commit job/tasks (this is the [sample flow](https://github.com/treeverse/lakeFS/blob/master/design/open/spark-outputcommitter/committer.md#sample-flow))

### Milestone 1
The first milestone is focused on writing a file with single mode (probably text) and one file format (probably append).
At this stage we'll implement `commit`, `setup`, `abort` class methods.

### Milestone 2
This milstone is focused on writing all file formats, in all modes. Also, implement the relevant remaining class methods.

The relevant file formats are:
* Text
* CSV
* Parquet *
* ORC
* JSON

The relevant modes are:
* ErrorIfExists (default)
* Append
* Overwrite
* Ignore

### Milestone 3
This milstone is focused on multiwriter support, first for overwrite save mode and then for other save modes.

### Other
Several tasks need to be determined in which milestone they should be included:
* Easier configuration: use laekFS Output Committer in default in lakeFSFS
* Improve configuration for Spark 3: better configuration options exist in Spark 3 (default OC for FS).
* Enhance metadata

![Execution Plan](diagrams/lakeFS-OC-execution-plan.png)


