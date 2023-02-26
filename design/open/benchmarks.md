# Benchmarks Tests

## Requirements
1. Measure the latency percentiles (50th, 90th, 95th, 99th 99.9) for a batch of repeated basic operations (e.g. 10K put requests),
   at a predefined parallelism level.
2. Report the number of failed requests per operation.
3. Show trends of benchmarks tests execution times across different versions.
4. Portability: The tests don't assume anything on the environment where the lakeFS & DB are running.
   lakeFS address is the only **required** argument for running the tests.

## Non-Requirements
1. An active gatekeeper - single slow running cycle shouldn't block the build.
2. Expect 100% reliability of all actions - when performing thousands of operations,
   some (exact percent TBD) will be allowed to fail. When measuring an external app,
   it's the app responsibility to retry on transient errors.
3. Avoid repeating unit/integration/system tests scenarios.

## Solution
### Test Execution
1. For each parallelism level, the flow will create a repository and will PUT 10k files in it.
    a. Each filename starts with 1 of the common shared prefixes (~100 common prefixes) 
2. 10k Read requests of those files.
3. Read each one of the common prefixes.
4. Commit the files.
5. Branch out and repeat 1-4.
6. Delete all files.

### CI
GitHub workflow is triggered on a merge to master.
The workflow will deploy a temporary environment that includes the latest lakeFS app, and an RDS instance.
Workflow collects and structures the tests results from the lakeFS instance, 
and sends them to a dedicated `prometheus` server.
The deployed environment is deleted after each collection of the results. 

### Results
A `Grafana` dashboard will display the benchmark tests results

## Future metrics
Collect and display metrics from the RDS instance for additional analysis of potential bottlenecks. 
