# Changelog

## v0.7.1 - 2023-04-24

=== Performance improvements===

No user-visible parts inside, but some parameters...

Deletion now retries S3 deleteObjects, a **lot**.  Parameters
`lakefs.gc.s3.min_backoff_secs`, `lakefs.gc.s3.max_backoff_secs` control how
long it will try.

## v0.7.0 - 2023-03-13

=== Performance improvements===

No user-visible parts inside, but some parameters...

* Write expired addresses to fewer locations
  * Only write text-format expired addresses if new Hadoop config option
    `lakefs.gc.address.write_as_text` is set.
	* Remove one (unused) expired address location.
* Handle huge metaranges: New Hadoop config option
  `lakefs.gc.address.approx_num_ranges_to_spread_per_partition` can be set
  when there are very many ranges.  Values 20..100 are probably best.
* For debugging performance

  Guaranteed to produce incorrect results!  You probably **never want to set
  this** in production or on any repository you care about.
  * New Hadoop config option `lakefs.debug.gc.addresses_sample_fraction` can
	be set below 1.0 _to debug performance **only**_.


## v0.6.5 - 2023-03-14

Bug fix:
* UGC fix uncommitted exists change (#5467)
* UGC fix list mark addresses

## v0.6.4 - 2023-03-13

Bug fix:
* UGC handle no uncommitted data on repository (#5451)

## v0.6.3 - 2023-03-09

Bug fix:
* UGC repartition by addresses to handle large repositories
* UGC use task context to delete temporary files
* UGC copy metadata to local without crc files

## v0.6.2 - 2023-02-23

Bug fix:
* Add exponential backoff retry to the S3 client

## v0.6.1 - 2023-01-30

What's new:
* Upgrade lakeFS client to v0.91.0
* Add UGC cutoff time to the report

## v0.6.0 - 2022-12-14

What's new:
* Beta feature: Uncommitted garbage collection. [Documentation](https://docs.lakefs.io/howto/garbage-collection.html#beta-deleting-uncommitted-objects)

Bug fixes:
* Correctly write GC JSON summary object using UTF-8 (#4644)

## v0.5.2 - 2022-11-29
Bug fixes:
* Identify the region of the S3 bucket if it's not reachable and update the initialized S3 client using that region.

## v0.5.1 - 2022-10-20
Bug fixes:
* Make GC backup and restore support expired addresses list including object not in the underlying object store (#4367)
* Don't package with hadoop-aws.  This removes many dependency failures and
  simplifies configuration.  But it also means that for plain Spark
  distributions such as that provided when downloading from the Apache Spark
  homepage you will need to add `--packages
  org.apache.hadoop:hadoop-aws:2.7.7` or `--packages
  org.apache.hadoop:hadoop-aws:3.2.1` or similar, to add in this package. (#4399)

## v0.5.0 - 2022-10-06
What's new:
* A utility for GC backup and restore. It allows users to copy objects that GC plans to delete or restore objects
from a previously created backup (#4318)

## v0.4.0 - 2022-09-30
What's new:
* Separate GC into a mark and sweep parts and add configuration parameters to control what phases to run (#4264)

Bug fixes:
* Fix the failure to write an empty dataframe into GC reports when running in mark-only mode (#4239)
* Only clean up relative path names (#4222) 

## v0.3.0 - 2022-09-21
What's new:
- Add retries mechanism (#4190)
- Improve performance (#4194)

## v0.2.3 - 2022-09-11
- Performance improvements (#4097, #4099, #4110)
- Fix bug: parsing problems in Azure (#4081)

## v0.2.2 - 2022-08-29
What's new:
- Added custom lakeFS client read timeout configuration (#3983)
- Rename custom lakeFS client timeout configuration keys (#4017)

Bug fixes:
- [GC] re-use http clients to limit the number of open connections and fix a resource leak (#3998)   

## v0.2.1 - 2022-08-18
Bug fixes:
- [GC] Added configuration flag of lakeFS client timeout to spark-submit command (#3905)

## v0.2.0 - 2022-08-01
What's new:
- Garbage Collection on Azure (#3733, #3654)

Bug fixes:
- [GC] Respect Hadoop AWS access key configuration in S3Client (#3762)
- exit GC in case that no GC rules are configured for a repo (#3779)
