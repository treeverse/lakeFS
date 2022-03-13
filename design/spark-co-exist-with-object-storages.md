# Spark: co-existing with existing underlying object store - Design proposal

This document includes a proposed solution for https://github.com/treeverse/lakeFS/issues/2625.

## Goals

### Must do

1. Provide a configurable set of overrides to translate paths at runtime from an old object store location to a new lakeFS location.
2. Support any object storage that lakeFS supports.
3. Do something that adds the least amount of friction to a Spark user as possible.

### Nice to have

Make the solution usable by non-lakeFS users by:
1. Supporting path translation from and to any type of path.
2. Make the solution a standalone component with no direct dependency on anything other than Spark itself. 

## Non-goals

1. Convert users from [accessing lakeFS from S3 gateway](../docs/integrations/spark.md#access-lakefs-using-the-s3a-gateway) to [accessing lakeFS with lakeFS-specific Hadoop filesystem](../docs/integrations/spark.md#access-lakefs-using-the-lakefs-specific-hadoop-filesystem).

## Proposal: Introducing RouterFileSystem

We would like to implement a [HadoopFileSystem](https://github.com/apache/hadoop/blob/2960d83c255a00a549f8809882cd3b73a6266b6d/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java) 
that translates object store URIs into lakeFS URIs according to a configurable mapping, and uses a relevant (configured) Hadoop file system to 
perform any file system operation.

For simplicity, this document uses `s3a` as the only URI scheme used inside the Spark application code, but this solution holds for any other URI scheme.  

### Handle any interaction with the underlying object store

To allow Spark users to integrate with lakeFS without changing their Spark code, we configure `RouterFileSystem` as the 
file system for the URI scheme (or schemes) their Spark application is using. For example, for a Spark application that 
reads and writes from S3 using `S3AFileSystem`, we configure `RouterFileSystem` to be the file system for URIs with `scheme=s3a`, as follows: 
```properties
fs.s3a.impl=RouterFileSystem
```
This will force any file system operation performed on an object URI with `scheme=s3a` to go through `RouterFileSystem` before it 
interacts with the underlying storage.

### URI translation

`RouterFileSystem` has access to a configurable mapping that maps any object store URI to any type of URI. This proposal 
uses s3 object store paths (with URI `scheme=s3a`) and lakeFS paths as examples.

#### Mapping configurations

The mapping configurations are spark properties of the following form:
```properties
routerfs.mapping.${toFsScheme}.${mappingIdx}.replace='^${fromFsScheme}://bucket/prefix'
routerfs.mapping.${toFsScheme}.${mappingIdx}.with='${toFsScheme}://another-bucket/prefix'
```

Where `mappingIdx` is an unbounded running index initialized for each `toFsScheme`, and `toFsScheme` is a URI scheme that
uses the `fs.toFsScheme.impl` property to point to the file system that handles the interaction with the underlying storage.
For example, the following mapping configurations
```properties
routerfs.mapping.lakefs.1.replace='^s3a://bucket/prefix'
routerfs.mapping.lakefs.1.with='lakefs://example-repo/dev/prefix'
```
together with 
```properties
fs.lakefs.impl=S3AFileSystem
```
make `RouterFileSystem` translate `s3a://bucket/prefix/foo.parquet` into `lakefs://example-repo/dev/prefix/foo.parquet`, 
and later use `S3AFileSystem` to interact with the underlying object storage which is S3 in this case. This example uses
[S3 gateway](../docs/integrations/spark.md#access-lakefs-using-the-s3a-gateway) as the Spark-lakeFS integration method.

##### Multiple mapping configurations   

Each `toFsScheme` can have any number of mapping configurations. E.g., below are two mapping configurations for `toFsScheme=lakefs`.
```properties
routerfs.mapping.lakefs.1.replace='^s3a://bucket/prefix'
routerfs.mapping.lakefs.1.with='lakefs://example-repo/dev/prefix'
routerfs.mapping.lakefs.2.replace='^s3a://another-bucket'
routerfs.mapping.lakefs.2.with='lakefs://another-repo/main'
```
Mapping configurations applied in order, therefore in case of a conflict in mapping configurations the prior configuration 
applies. 

##### Multiple `toFsScheme`s (and multiple mapping configuration groups)

With `RouterFileSystem`, Spark users can define any number of `toFsScheme`s. Each forms its own mapping configuration group,
and allows applying different set of Spark/Hadoop configurations. e.g. credentials, s3 endpoint, etc. Users would typically
define new `toFsScheme` while trying to migrate a collection from one storage space to another without changing their Spark code.

The example below demonstrates how routerFS mapping configurations and some Hadoop configurations will look like for
Spark application that accesses s3, MinIO, and lakeFS, but is using s3a as its sole URI scheme. 
```properties
# Mapping configurations
routerfs.mapping.lakefs.1.replace='^s3a://bucket/prefix'
routerfs.mapping.lakefs.1.with='lakefs://example-repo/dev/prefix'
routerfs.mapping.s3a1.1.replace='^s3a://bucket'
routerfs.mapping.s3a1.1.with='s3a1://another-bucket'
routerfs.mapping.minio.1.replace='^s3a://minio-bucket'
routerfs.mapping.minio.1.with='minio://another-minio-bucket'

# File System configurations
# Implementation
fs.s3a.impl=RouterFileSystem
fs.lakefs.impl=S3AFileSystem
fs.s3a1.impl=S3AFileSystem
fs.minio.impl=S3AFileSystem

# Access keys, the example `toFsScheme=s3a1` but required for each configured `toFsScheme`
fs.s3a1.access.key=AKIAIOSFODNN7EXAMPLE
fs.s3a1.secret.key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

##### Default mapping configuration 

`RouterFileSystem` requires a default mapping configuration in case that none of the mappings matches a URI. 
For example, the following default configuration states that in case or a URI with `scheme=s3a` that didn't match the
`^s3a://bucket/prefix` pattern, `RouterFileSystem` uses the default file system that's configured to be `S3AFileSystem`.  

```properties
routerfs.mapping.lakefs.1.replace='^s3a://bucket/prefix'
routerfs.mapping.lakefs.1.with='lakefs://example-repo/dev/prefix'

# Default mapping, should be applied last 
routerfs.mapping.s3a-default.replace='^s3a://'
routerfs.mapping.s3a-default.with='s3a-default://'

# File System configurations
fs.s3a.impl=RouterFileSystem
fs.lakefs.impl=S3AFileSystem
fs.s3a-default.impl=S3AFileSystem
```

This configuration is required because otherwise `RouterFileSystem` will get stuck in an infinite loop, by calling itself 
as the file system for `scheme=s3a` in the example. 

### Invoke file system operations

After translating URIs to their final form, `RouterFileSystem` will use the translated path and its relevant file system to 
perform file system operations against the relevant object store. See example in [Mapping configurations](#mapping-configurations).  

### Getting the relevant File System 

`RouterFileSystem` uses the `fs.<toFsScheme>.impl=S3AFileSystem` Hadoop configuration and the FileSystem method 
[path.getFileSystem()](https://github.com/apache/hadoop/blob/2960d83c255a00a549f8809882cd3b73a6266b6d/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Path.java#L365)
to access the correct file system at runtime, according to user configurations.

### Integrating with lakeFS

`RouterFileSystem` does not change the exiting [integration methods](../docs/integrations/spark.md#two-tiered-spark-support) 
lakeFS and Spark have. that is, one can use both `S3AFileSystem` and `LakeFSFileSystem` and to read and write objects from lakeFS
(See configurations reference below). 

#### Access lakeFS using S3 gateway
```properties
routerfs.mapping.lakefs.1.replace='^s3a://bucket/prefix'
routerfs.mapping.lakefs.1.with='lakefs://example-repo/dev/prefix'
```
together with
```properties
fs.s3a.impl=RouterFileSystem
fs.lakefs.impl=S3AFileSystem
```

#### Access lakeFS using lakeFS-specific Hadoop FileSystem

```properties
routerfs.mapping.lakefs.1.replace='^s3a://bucket/prefix'
routerfs.mapping.lakefs.1.with='lakefs://example-repo/dev/prefix'
```
together with
```properties
fs.s3a.impl=RouterFileSystem
fs.lakefs.impl=LakeFSFileSystem
```

**Note** There is a (solvable) open item here: during its initialization, LakeFSFileSystem [dynamically fetches](https://github.com/treeverse/lakeFS/blob/276ee87fe41841589d631aaeec1c4859308001c1/clients/hadoopfs/src/main/java/io/lakefs/LakeFSFileSystem.java#L93)
the underlying file system from the repository storage namespace. The file system configurations above will make `LakeFSFileSystem` 
fetch `RouterFileSystem` for storage namespaces on s3, preventing from `LakeFSFileSystem` delegate file system operations to 
the right underlying file system (`S3AFileSystem`). 

### Examples

#### Read an object managed by lakeFS

![RouterFS with lakeFS URI](diagrams/routerFS-by_lakefs.png)

#### Read an object directly from the object store

![RouterFS with S3 URI](diagrams/routerFS-by_s3.png)

### Pros & Cons

### Pros

1. We already have experience developing Hadoop file systems, therefore, the ramp up should not be significant.
2. `RouterFileSystem` suggests much simpler functionality than what lakeFSFS supports (it only needs to receive calls, translate paths, and route to the right file system), which reduces the estimated number of unknowns unknowns.
3. It does not change anything related to the existing Spark<>lakeFS integrations.
4. `RouterFileSystem` can support non-lakeFS use-cases because it does not relay on any lakeFS client.
5. `RouterFileSystem` can be developed in a separate repo and delivered as a standalone OSS product, we may be able to contribute it to Spark. 
6. `RouterFileSystem` does not rely on per-bucket configurations that are only supported for hadoop-aws (includes S3AFileSystem implementation) versions >= [2.8.0](https://hadoop.apache.org/docs/r2.8.0/hadoop-aws/tools/hadoop-aws/index.html#Configurations_different_S3_buckets).  

### Cons

1. Based on our experience with lakeFSFS, we already know that supporting a hadoop file system is difficult. There are many things that can go wrong in terms of dependency conflicts, and unexpected behaviours working with managed frameworks (i.e. Databricks, EMR)
2. It's complex.
3. `RouterFileSystem` is unaware of the number of mapping configurations every `toFsScheme` has and needs to figure this out at runtime. 
4. `toFsScheme` may be a confusing concept.
5. Requires adjustments of [LakeFSFileSystem](../clients/hadoopfs/src/main/java/io/lakefs/LakeFSFileSystem.java), see [this](#access-lakefs-using-lakefs-specific-hadoop-filesystem) for a reference.
6. Requires discovery and documentation of limitations some fs operation have in case of overlapping configurations. For example, a recursive delete operation can map paths to different file systems: recursive deletion of /data, while one file system is the complete package, and /data/lakefs is mapped to lakeFS.
