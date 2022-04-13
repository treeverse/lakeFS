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

- The scripts `setup-gc-test.sh` and `setup-exporter-test.sh` are responsible to setup the repository for the respective tests (Garbage collection test and Exporter test).
- The scripts `run-gc-test.sh` and `run-exporter-test.sh` are responsible to prepare the test environment and run the respective test `spark-submit` job.

### Exporter test

The app itself is the word count example that store the information into csv format.  Loading the csv infomation and validate specific data in order to verify that the right data is loaded.

### GC tests

The tests use two branches `a<id>` and `b<id>`, and according to the test scenarios, files are uploaded/deleted from them, and the branches themselves might be deleted as well (if specifying `-1` as the value of `delete_commit_days_ago`: `"delete_commit_days_ago": -1`).  
If you wish to add additional tests to the test suit of GC, update the `gc-tests/test_scenarios.json` file with the new test scenario and mind giving it a new unique id and append it to the end of the branch names (`a<id>` and `b<id>`).  
Example:
```json lines
{
    "id": 3,   // Notice the id
    "policy": {
      "default_retention_days": 5,
      "branches": [                               // Notice the branches names
        {"branch_id": "a3", "retention_days": 1},  
        {"branch_id": "b3", "retention_days": 3}
      ]
    },
    "branches": [                                 // Notice the branches names
      {
        "branch_name": "a3",
        "delete_commit_days_ago": 4
      },
      {
        "branch_name": "b3",
        "delete_commit_days_ago": 2
      }
    ],
    "file_deleted": false,
    "description": "The file is not deleted because of the retention policy of the second branch"
  }
```