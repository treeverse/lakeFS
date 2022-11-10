# Performance issues with GC

## About

This is a short summary of observed performance issues as of 2022-11-10.
Things are changing!

## The "Amazon reviews" test

I recreated the XXXX tests:

```scala
// import data from public data set
val AmazonReviews = spark.read.parquet("s3://amazon-reviews-pds/parquet/")
val partial = AmazonReviews.limit(117647)
partial.write.parquet("lakefs://test-1/main/amazon-reviews")
```

Now I run on a clean branch:

```scala
val reviews = spark.read.parquet("lakefs://test-1/main/amazon-reviews")

reviews.repartition(4000)
	.write
	.csv("lakefs://test-1/test-clean-2/amazon-reviews-repartition-clean2.csv/")
```

Notebook available to Treeversers on request.

I repeated the tests with PostgreSQL and DynamoDB, and with 16, 3 and a
single worker.  I did not cover the entire matrix.

This will perform 4000 merges concurrently across all the workers, followed
by a single uncontended merge.  Each of the 4000 merges adds a *single*
file, and the repo is almost empty.  The last merge adds 4001 files.

## Issues

### Performance

Concurrent merges timed out, so I added client-side retries.  This makes
sense for automated clients where failing the entire process is not an
option.  With this change things work.  But...

With PostgreSQL we get ~40m to write with 16 workers, worse that S3A.  Using
3 workers takes around the same time.  A single worker takes >1h.  With
DynamoDB we go over an hour for 16 workers, and much longer for a single
worker.

I quadrupled Pyramid local cache size and got the cache miss rate down from
20% to 8%.  This had no significant effect on execution time.

Median merge performance is *excellent*, higher percentiles are *awful*:
* p50 is 250ms (PostgreSQL) or 400ms (DynamoDB)
* p90 is 40s (PostgreSQL)
* p99 is 1m (PostgreSQL)

This suggests that p90 is really bad, p99 is surprisingly good for such a
p90.

### Effects of system load

I occasionally get bad range files in the lakeFS error output.  It seems
that Pyramid or something manages to skip them, and the process succeeds.

```json
{
  "error": "open range 73289a7422c1a5bda61f838ef844e896fbd9b2b0ca77a878f6038368e9aff46b: open sstable reader s3://yoni-test3/data/XXXX 73289a7422c1a5bda61f838ef844e896fbd9b2b0ca77a878f6038368e9aff46b: pebble/table: invalid table (bad magic number)",
  "file": "build/pkg/api/controller.go:1745",
  "func": "pkg/api.(*Controller).handleAPIError",
  "host": "XXXX.us-east-1.lakefscloud.ninja",
  "level": "error",
  "log_audit": "API",
  "method": "GET",
  "msg": "API call returned status internal server error",
  "operation_id": "StatObject",
  "path": "/api/v1/repositories/test-1/refs/lakeFS-OC--1l6fxphkzwaascr80nbp2tiour6kztr3asyov0k8stzt6u1wdn-lakefs---test-1-test-clean-2-amazon-reviews-repartition-clean2-csv-attempt_202211071322492411147324158189068_0003_m_003905_4056/objects/stat?path=amazon-reviews-repartition-clean2.csv%2Fpart-03905-43236b89-c813-467b-9cb4-2dd1608c0c63-c000.csv&user_metadata=false",
  "request_id": "e670b72d-068f-4d6c-876e-d5034907f6e8",
  "service": "api_gateway",
  "service_name": "rest_api",
  "time": "2022-11-07T13:58:15Z"
}
{
  "error": "open range 24a2ee4c613e69b47547259675b9077dbdada1bc2480181a64ebe949235274d7: open sstable reader s3://yoni-test3/data/XXXX 24a2ee4c613e69b47547259675b9077dbdada1bc2480181a64ebe949235274d7: pebble/table: invalid table (bad magic number)",
  "file": "build/pkg/api/controller.go:1745",
  "func": "pkg/api.(*Controller).handleAPIError",
  "host": "XXXX.us-east-1.lakefscloud.ninja",
  "level": "error",
  "log_audit": "API",
  "method": "GET",
  "msg": "API call returned status internal server error",
  "operation_id": "DiffRefs",
  "path": "/api/v1/repositories/test-1/refs/lakeFS-OC--1l6fxphkzwaascr80nbp2tiour6kztr3asyov0k8stzt6u1wdn-lakefs---test-1-test-clean-2-amazon-reviews-repartition-clean2-csv-attempt_202211071322498107897909273126199_0003_m_003334_3843/diff/lakeFS-OC--1l6fxphkzwaascr80nbp2tiour6kztr3asyov0k8stzt6u1wdn-lakefs---test-1-test-clean-2-amazon-reviews-repartition-clean2-csv?amount=1&type=two_dot&diff_type=two_dot",
  "request_id": "985c1378-32b5-439f-b4cf-2334c5126d59",
  "service": "api_gateway",
  "service_name": "rest_api",
  "time": "2022-11-07T13:56:17Z"
}
{
  "error": "open range c85d0ca8c208e200527f20fd78c58866e92e83e9f676da528b64e868fb13a091: open sstable reader s3://yoni-test3/data/XXXX c85d0ca8c208e200527f20fd78c58866e92e83e9f676da528b64e868fb13a091: pebble/table: invalid table (bad magic number)",
  "file": "build/pkg/api/controller.go:1745",
  "func": "pkg/api.(*Controller).handleAPIError",
  "host": "XXXX.us-east-1.lakefscloud.ninja",
  "level": "error",
  "log_audit": "API",
  "method": "GET",
  "msg": "API call returned status internal server error",
  "operation_id": "DiffRefs",
  "path": "/api/v1/repositories/test-1/refs/lakeFS-OC--1l6fxphkzwaascr80nbp2tiour6kztr3asyov0k8stzt6u1wdn-lakefs---test-1-test-clean-2-amazon-reviews-repartition-clean2-csv-attempt_202211071232381584428887595320545_0007_m_002678_6709/diff/lakeFS-OC--1l6fxphkzwaascr80nbp2tiour6kztr3asyov0k8stzt6u1wdn-lakefs---test-1-test-clean-2-amazon-reviews-repartition-clean2-csv?amount=1&type=two_dot&diff_type=two_dot",
  "request_id": "9b7b65ff-6278-4fc4-b6e3-b3377014f862",
  "service": "api_gateway",
  "service_name": "rest_api",
  "time": "2022-11-07T12:55:38Z"
}
```

RPS is more important for us here, so average latency is the key.  However
it is hard to estimate total merge time from merge operation latencies,
because most of the wasted work on a merge is work that occurs in parallel
with other merges.

## Suggestions

### Find "silver bullet" to boost merge performance on lakeFS Cloud

### Measure merge performance

Effects of retries are unclear; if we are using poor parameters we will be
going much too slowly.  Currently we just use the default backoff
parameters, which might not fit our use-case.  Measure and find good
parameters.

### Boost merge performance

Consider speeding up implementation of the algorithm suggested in [#4057].

### More magic-like LakeFSOutputCommitter

Use unlinked objects on S3 as task temporaries.  Requires modifying
LakeFSFileSystem or building something on top of it.  (Design forthcoming).

#### Alternative: move objects from task branch to job branch

### Don't do multiple merges as part of task but only at end of jobs

Followed up by multi-branch merge!

### Try to merge tasks one into another to reduce contention

### Merge task *commit* and not its branch

Avoid touching the staging area.

### Merge task branches during commitJob

commitJob is nonconcurrent, so has no races.  Task branch names all share
the job branch name as prefix.  commitTask does nothing, abortTask deletes
its task branch.  To commitJob we _list_ all task branches (everything with
prefix job branch) and merge them all into the job branch.

#### Speedup: concurrent merges

commitJob can merge concurrently _between_ the task branches and onto the
job branch.  These concurrent merges do not conflict, they use distinct
destination branches.
