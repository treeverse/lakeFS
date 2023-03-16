# Test Spark App

In order to test lakeFS with Spark we use Docker Compose to run REPO/TAG lakeFS image with Spark master ('spark') and two workers ('spark-worker-1' and 'spark-worker-2').
Spark access lakeFS using s3a endpoint, which we configure while we run `spark-submit`, the endpoint will try to resolve the names: `s3.docker.lakefs.io` and `example.s3.docker.lakefs.io`.
Running the services inside the default network and set a specific subnet enables us to set specific IP address to lakeFS and add additional host to resolve to the same address - 10.5.0.55 in our case.
Every new bucket we would like to access though our gateway will require additional entry that should be mapped to lakefs (10.0.1.50).

### Running it

After docker compose env is up and running we need to verify that the Spark application - found under `app/` folder is composed and packaged:

```shell
sbt package
```

The Compose file holds a service entry called `spark-submit` which uses the same settings as a Spark worker just with profile `command`, which will not start by default, unless "command" profile is specified.
Using this entry and the volume mount to our workspace we submit the spark app.

### Exporter test

- `setup-exporter-test.sh` is responsible to setup the repository for the Exporter test.
- `run-exporter-test.sh` is responsible to prepare the test environment and run the respective test `spark-submit` job.

The app itself is the word count example that store the information into csv format.  Loading the csv infomation and validate specific data in order to verify that the right data is loaded.
