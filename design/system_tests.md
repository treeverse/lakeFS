# System Tests

## Requirements
1. Test the system in the same manner users will be using it. 
   No ad-hoc initializations or mocking of any kind. 
2. Cover the most common use-cases.
3. Portability: The tests don't assume anything on the environment where the lakeFS & DB are running.
   lakeFS address is the only **required** argument for running the tests.  

## Non-Requirements
1. Test every use-case of the system, including:
    a. Every API/gateway endpoint.
    b. Every application (e.g. Spark) usage pattern.
2. Avoid repeating unit/integration tests scenarios.
3. Stress testing / benchmarks. 

## Solution
### Test
A test consists of one or more calls to the API/gateway and assertions on the actual output.
Test is a standalone unit. It shouldn't rely on any other test to pass to start execution. 

Later on we might consider adding checks on the DB or data lake layer(S3),
by adding information of the underlying system components.

#### Test-Cases
TBD

### Tests binary
Tests will be built into a single binary and run sequentially or in parallel.

### When will it run? Where?
As a standalone unit, the tests binary can be executed from anywhere as long as it can reach the lakeFS endpoint.
We encourage developers to run the tests locally as part of the development cycle. 

#### CI
Github workflow will be triggered when all other workflows finished successfully and reviewers approved.
The workflow will run the lakeFS app with local postgres DB on a dedicated server,
execute the tests binary and report the results. Failed run will block the PR merge.

Underline storage (S3/GCS/Minio/other) will be created once for all test runs.
Tests are orthogonal to the lakeFS setup which could vary in the backing storage layer,
postgres version and other configurations.

Objects will be deleted by setting expiry for the entire bucket in the storage layer.
Isolation is kept by setting unique file prefixes which is determined by the lakeFS namespaces.
