# Export

## Requirements
The functionality currently provided by the [lakeFS Export utility](https://docs.lakefs.io/reference/export.html) but without a dependency on Apache Spark.
This includes:
1. Export all data from a given lakeFS reference (commit or branch) to a designated object store location.
1. Export only the diff between the HEAD of a branch and a commit reference (helpful for continuous exports).
1. Success/Failure indications on the exported objects. 


## Non-Requirements
1. Support distribution across machines.
1. Support from lakeFS.


## Possible Solutions external to lakeFS

### Spark client stand-alone

Create a stand-alone Docker container that runs the Spark client.

In order to use Docker containers, users will need to download it if necessary, learn it, and have it in their toolset.

Usage example:

```shell 
docker run lakefs-export --conf spark.hadoop.lakefs.api.url=https://<LAKEFS_ENDPOINT>/api/v1 \
--conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY_ID> \
--conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_ACCESS_KEY> \
--packages io.lakefs:lakefs-spark-client-301_2.12:0.1.0 \
--class io.treeverse.clients.Main export-app example-repo s3://example-bucket/prefix \
--branch=example-branch 
``` 

Pros:
- Utilizes the existing Spark client, and gives the same functionality. 
 
Cons:
- Require creating and maintaining the standalone container.
- Still requires the user to understand Spark indirectly - for example know how to read logs or errors that arise from running Spark stand-alone.

### Using the java client
Reimplement the spark client export functionality by creating a new Exporter class that doesn't use spark.
This class can be part of the java cliet, or a new "Export" client that uses the java client.

The usage will be with java code (in a similar way of using custom code with the spark client).

In order to use Java code, users will need to download it if necessary, learn it, and have it in their toolset.

Usage example:

```java
class Export{
    public static void main(String[]args){
        Exporter exporter = new Exporter(apiClient, repoName, rootLocation);
        exporter.exportAllFromBranch(branchName);
        exporter.exportFrom(branchName, commitRef);
    }
}
```

Pros:
- An intermediate solution that is compatible with the issue.

Cons:
- Require checking how spark is being used in the current implementation (in order to rewrite its functions). Not necessarily possible in a naive way.
- Require developing and maintaining a second export functionality.

### Using Rclone
[Rclone](https://rclone.org/) is a command line program to sync files and directories between cloud providers.
Users can gain the export functionality by using Rclone's [copy](https://rclone.org/commands/rclone_copy/), [sync](https://rclone.org/commands/rclone_sync/), and [check](https://rclone.org/commands/rclone_check/) commands.

The copy command can be used to copy files from lakeFS to a designated object store location.
The sync command can be used to export only the diff between a specific branch and a commit reference, since sync makes the source and the dest identical (modifying destination only).
The check command can be used as a success/failure indication on the exported objects, since it checks if the files in the source and destination match, and logs a report of files which don't match.

Usage example:

```shell
rclone copy source:lakefs:example-repo/main/ dest:s3://example-bucket/prefix

rclone sync source:lakefs:example-repo/main/ dest:s3://example-bucket/prefix

rclone check source:lakefs:example-repo/main/ dest:s3://example-bucket/prefix
```

Pros:
- Rclone gives the functionality of exporting data from lakeFS to a designated object store location. 
- Simple to use. 
- Doesn't require maintaining a new feature.

Cons:
- Require a designated object store location that doesn't contain any other data but the data associated to the lakeFS branch. That is because Rclone's sync command will delete all files that don't exist in the branch.   


## Chosen solution

Wrap Rclone with a python script to match the behavior of the new export functionality to the one of the Spark client.
The script will run over a Docker container, which will have all the necessary dependencies.
The relevant Docker image will be published to Docker Hub. 
This solution utilizes Rclone and its features, it is easy to implement and achives the required behavior.

For example:
```shell
docker run -e LAKEFS_ACCESS_KEY=XXX -e LAKEFS_SECRET_KEY=YYY -e LAKEFS_ENDPOINT=https://<LAKEFS_ENDPOINT>/ -e S3_ACCESS_KEY=XXX -e S3_SECRET_KEY=YYY -it lakefs-export repo-name s3://some-bucket/some_path/ --branch="branch-name"
```

The script will call:
```shell
rclone sync source:lakefs:example-repo/main/ dest:s3://example-bucket/prefix
rclone check source:lakefs:example-repo/main/ dest:s3://example-bucket/prefix
```
And then will add a success/failure file according to the check command result.