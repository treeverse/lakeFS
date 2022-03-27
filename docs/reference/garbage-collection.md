---
layout: default
title: Garbage Collection
description: Clean up unnecessary objects
parent: Reference
nav_order: 25
has_children: false
---

# Garbage Collection
{: .no_toc }

By default, lakeFS keeps all your objects forever. This allows you to travel back in time to previous versions of your data.
However, sometimes you may want to hard-delete your objects, namely delete them from the underlying storage. 
Reasons for this include cost-reduction and privacy policies.

Garbage collection rules in lakeFS define for how long to retain objects after they have been deleted (see more information [below](#considerations){: .button-clickable}).
lakeFS provides a Spark program to hard-delete objects that have been deleted and whose retention period has ended according to the GC rules.
The GC job does not remove any commits: you will still be able to use commits containing hard-deleted objects,
but trying to read these objects from lakeFS will result in a `410 Gone` HTTP status.

## Understanding Garbage Collection

For every branch, the GC job retains deleted objects for the number of days defined for the branch.
In the absence of a branch-specific rule, the default rule for the repository is used.
If an object is present in more than one branch ancestry, it is retained according to the rule with the largest number of days between those branches.
That is, it is hard-deleted only after the retention period has ended for all relevant branches.

Example GC rules for a repository:
```json
{
  "default_retention_days": 21,
  "branches": [
    {"branch_id": "main", "retention_days": 28},
    {"branch_id": "dev", "retention_days": 7}
  ]
}
```

In the above example, objects are retained for 21 days after deletion by default. However, if they are present in the branch `main`, they are retained for 28 days.
Objects present in the `dev` branch (but not in any other branch), are retained for 7 days after they are deleted.

## Configuring GC rules

Use the `lakectl` CLI to define the GC rules: 

```bash
cat <<EOT >> example_repo_gc_rules.json
{
  "default_retention_days": 21,
  "branches": [
    {"branch_id": "main", "retention_days": 28},
    {"branch_id": "dev", "retention_days": 7}
  ]
}
EOT

lakectl gc set-config lakefs://example-repo -f example_repo_gc_rules.json 
```

## Running the GC job

The GC job is a Spark program that can be run using `spark-submit` (or using your preferred method of running Spark programs).
The job will hard-delete objects that were deleted and whose retention period has ended according to the GC rules.

First, you'll have to download the lakeFS Spark client Uber-jar. The Uber-Jar can be found on a public S3 location:

For Spark 2.4.7: \
http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-247/${CLIENT_VERSION}/lakefs-spark-client-247-assembly-${CLIENT_VERSION}.jar

For Spark 3.0.1: \
http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-301/${CLIENT_VERSION}/lakefs-spark-client-301-assembly-${CLIENT_VERSION}.jar

`CLIENT_VERSION`s for Spark 2.4.7 can be found [here](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client-247){: .button-clickable}, and for Spark 3.0.1 they can be found [here](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client-301){: .button-clickable}.


Second, you should specify the Uber-jar path instead of `<APPLICATION-JAR-PATH>` and run the following command to make the garbage collector start running:
```bash
spark-submit --class io.treeverse.clients.GarbageCollector \
  -c spark.hadoop.lakefs.api.url=https://lakefs.example.com:8000/api/v1  \
  -c spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
  -c spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
  -c spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
  -c spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
  <APPLICATION-JAR-PATH> \
  example-repo us-east-1
```
## Considerations
1. In order for an object to be hard-deleted, it must be deleted from all branches.
   You should remove stale branches to prevent them from retaining old objects.
   For example, consider a branch that has been merged to `main` and has become stale.
   An object which is later deleted from `main` will always be present in the stale branch, preventing it from being hard-deleted.
   
1. lakeFS will never delete objects outside your repository's storage namespace.
   In particular, objects that were imported using `lakefs import` or `lakectl ingest` will not be affected by GC jobs.
   
1. In cases where deleted objects are brought back to life while a GC job is running, said objects may or may not be
   deleted. Such actions include:
   1. Reverting a commit in which a file was deleted.
   1. Branching out from an old commit.
   1. Expanding the retention period of a branch.
   1. Creating a branch from an existing branch, where the new branch has a longer retention period.
