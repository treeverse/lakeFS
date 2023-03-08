# Changelog

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
