# Changelog

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
