# lakeFS on KV - Testing Plan

## Global Testing

### Migration
* Global tests to verify and benchmark the migration process
* Need to verify that the system behavior is not affected by the move to the KV DB
* [Question] - Do we need to dispose of these once migration to KV is globally completed?

#### Operational Testing
* Set of tests that will run DB migration as part of the test
* Currently, system tests assume lakeFS is up and running. Need to add ability to perform migration in the middle of the test (stop lakeFS -> run&verify migration -> start lakeFS ?)
* Maybe something that can be 

#### Data Level Testing
* Dump (all) data before and after migration and compare it
* Verifies data is preserved through migration, and is readable by the KV Store
* Need to write appropriate dumpers. Can be done per package, as we go
* Can be done both on DB package level, to verify the package itself, and on the using packages level, to verify data usage is not affected
* [Question] - How do we define a covering data set, to test? How do we create

### Performance
* **A benchmark for the migration process itself:**
  * How do we load the DB with data to migrate for benchmarking?
  * Ideally, run on several data sizes (tens / thousands / millions branches & commits, various numbers of users, etc.) to get sense of how scaling will affect
  * What is the expected/acceptable outcome?
  * Should be built gradually, as we support and migrate additional package

* **Benchmark comparison of both DBs**
  * Compare performance before and after migration - does not include the migration itself
  * Define package specific set of operation to benchmark
  * Run with both Table DB and KV Store and compare the results
  * What is the accepted degradation, if any?
  * Need to consider various scales

### Feature Flag
* Dummy tests to verify the feature flag works as expected
* Package level panic in case the wrong DB logic is used? Is that sufficient?

## Per Package DB Testing

### ```pkg/gateway/multiparts```
* DB is used to track the start and end of a multipart upload. All DB accesses are done via `mutltiparts.Tracker`. Entries are created once, read-accesses multiple times and deleted upon completion
* Currently unit tests cover correctness of DB accesses in both good and error paths.
* 83.8% coverage
* Currently system tests cover a simle multipart upload - a single object with 7 parts, good path only
* What is missing:
  * Performance - Can that sustain heavy loads: multi-multipart uploads with a lot of parts
  * Concurrency, that is derived from the previous bullet (can we increase concurrency by using smaller parts?)
  * Migration - Verify that data migrated from Table to KV, in the middle of multipart upload, is still usable


### ```pkg/actions```
* DB is used to store actions runs and hook runs with results. Db is read by the actions service to handle requests from the various clients. For each run there is a new entry in `actions_run` table and an entry for each hook in `actions_run_hooks` table.
Updates are done for commitID, as part of `post` hooks
* Currently unit tests cover hooks run and verify the results in DB using the service reading functions. There are also error testings (hook failures)
* 67.6% coverage
* What is missing:
  * DB Failure during hook action (TBD - what is the expected behavior? Is that interesting to test?)
  * Performance under load - not sure a load on actions/hooks is relevant
  * Concurrent actions/hooks execution, which derives concurrent DB accesses. This gets more interesting if load is involved (in case load is relevant)

### ```pkg/auth```
* DB is used to store installation metadata, users and policies
* Only 6.8% coverage
* Some benchmarks exists that covers effectivePolicies (multi-table JOIN)
* What is missing:
  * Need to increase code coverage
  * Comparison benchmark to toggle between Tables and KV and verify there is no degradation
  * Migration tests:
    * verify that data is migrated correctly
    * Authentication transactions in during migration (data that was written to tables is read from KV)

### ```pkg/diagnostics```
* Only read access
* No test coverage. Need to verify all is working the same after migration

### ```pkg/graveler/ref```
* Branch locking (read)
* Repos, branches, commits and tags - read/write
* 95.5% testing coverage
* No performance/benchmarking
* Missing:
  * Migration tests
  * Comparison benchmark

### ```pkg/graveler/retention```
* No direct DB access. Accesses are done using ```pkg/graveler/ref```, which should make the DB transition seamless
* 42.9% coverage in unit testing
* Missing performance tests that might help detect degradation

### ```pkg/graveler/staging```
* 89.1% coverage
* Covers all staged KV functionality
* No performance/benchmarking
* Missing:
  * Migration tests
  * Comparison benchmark

### Res-Dump/Restore
Currently this functionality is not covered directly, but since it relies on '''pkg/graveler/ref''' for DB access, the DB migration should be seamless
It can be leveraged, however, to extend the cover of '''pkg/graveler/ref''' and for performance, as it is quite exhaustive (traverses all branches and commits and tags, per repo)

## Execution Plan

### KVM1
* Data level migration tests infrastructure
  * Migrate data from Table to KV, dumps both and compares
  * Implement dumpers for `gateway_multiprts`
  * Implement tests for '''pkg/multiparts'''
* Infrastructure for running migration during a system test execution
  * System test to run migration during multiparts upload
  * Currently there is a single simple multiparts system test (single file, 7 parts) - this is also an opportunity to expand that
* KV Store unit tests 
* `multiparts.Tracker` benchmark to run on both Table DB and KV Store (use feature flag to toggle) and verify there is no degradation
  * Define sequence(s) of actions to perform (Create/Get/Delete etc.)
  * Run each sequence with feature flag off and on
  * Compare results and fail if KV performance is more than [TBD]% slower
  * Need an infrastructure 

### Next MS
TBD


## Open questions
* What performance degradation is acceptable? (Can be 0)
* How do we simulate KV Store failure? Do we need it at all?
* What is the expected downtime for migrate?