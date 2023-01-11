# Test Spark App

In order to test lakeFS with Spark we use docker-compose to run REPO/TAG lakeFS image with Spark master ('spark') and two workers ('spark-worker-1' and 'spark-worker-2').
Spark access lakeFS using s3a endpoint, which we confiure while we run `spark-submit`, the endpoint will try to resolve the names: `s3.docker.lakefs.io` and `example.s3.docker.lakefs.io`.
Running the services inside the default network and set a specific subnet enables us to set specific IP address to lakeFS and add additional host to resolve to the same address - 10.5.0.55 in our case.
Every new bucket we would like to access though our gateway will require additional entry that should be mapped to lakefs (10.0.1.50).

### Running it

After docker compose env is up and running we need to verify that the Spark application - found under `app/` folder is composed and packaged:

```shell
sbt package
```

The docker-compose yaml holds a service entry called `spark-submit` which uses the same settings as a Spark worker just with profile `command`, which will not start by default, unless "command" profile is specified.
Using this entry and the volume mount to our workspace we submit the spark app.

- `setup-exporter-test.sh` is responsible to setup the repository for the Exporter test. Repositories in the GC tests are created in the test itself.
- The scripts `run-gc-test.sh` and `run-exporter-test.sh` are responsible to prepare the test environment and run the respective test `spark-submit` job.

### Exporter test

The app itself is the word count example that store the information into csv format.  Loading the csv infomation and validate specific data in order to verify that the right data is loaded.

### GC tests

The tests use two branches `a<id>` and `b<id>`, and according to the test scenarios, files are uploaded/deleted from them, and the branches themselves might be deleted as well (if specifying `-1` as the value of `delete_commit_days_ago`: `"delete_commit_days_ago": -1`).  
If you wish to add additional tests to the test suit of GC, update the test table in `gc_test.go` with the new test scenario. Give the test case a new unique id and append it to the branch names (`a<id>` and `b<id>`).  
Example:
```go
{
  id: "3", // note the id
  policy: api.GarbageCollectionRules{
      Branches: []api.GarbageCollectionRule{
        {BranchId: "a3", RetentionDays: 1},
        {BranchId: "b3", RetentionDays: 3},
	  },
      DefaultRetentionDays: 5,
  },
  branches: []branchProperty{
    {name: "a3", deleteCommitDaysAgo: 4},
	{name: "b3", deleteCommitDaysAgo: 2},
  },
  fileDeleted:  false,
  description:  "The file is not deleted because of the retention policy of the second branch",
  directUpload: false,
},
```