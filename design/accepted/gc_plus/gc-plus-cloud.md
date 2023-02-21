# Managed Uncommitted Garbage Collection for lakeFS cloud - Execution Plan

## Goals
1. Run UGC for lakeFS cloud users
2. Allow Cloud users to clean up storage space by deleting objects from the underlying storage for cost efficiency
3. Support new and existing cloud users (both repository structures)

## Non-Goals
1. Remove lakeFS limitations during the UGC job run
2. Improve UGC OSS support
3. Support non-S3 installations

### Background
For every cloud installation, The MUGC will run by default (sweep=true) without visibility to the user to control its configuration.
We should have to ability to control the MUGC for the user - change configuration, or stop it.

### Diagram
![MUGC architecture](diagrams/mugc.png)

### Managed UGC job
**AWS Cloud Watch** - use Cloud Watch events to run an AWS Lambda function on a schedule.
Create a rule to trigger the lambda script that will run the UGC job. It will collect the logs and use them for monitoring and observations of the runs.

**AWS lambda function** - a function that runs a script that will be triggered periodically by the [AWS cloud watch](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/RunLambdaSchedule.html) every 6 hours.
The script will include:
* From lakeFS cloud control plane:
    * Get organization and installations
    * Get UGC configuration for the spark job (configured role, secret and access key, installation endpoint, repository)
    * For each installation - list repositories
* For each repository - trigger UGC spark job running on EMR serverless application
    * Retry in case of failing

### Permissions
UGC needs two types of permissions to operate:
- lakeFS permissions - ```prepare uncommitted``` API operation permission.
  can be added to the current lakeFS GC user - `managed-gc` another policy `UGCRules`.
  Need to make sure that defining a GC user is a mandatory step of cloud environment setup, and that the UGC rules are added to it.
```
{
    "creation_date": 1675520563,
    "id": "UGCRules",
    "statement": [
        {
            "action": [
                "retention:PrepareGarbageCollectionUncommitted"
            ],
            "effect": "allow",
            "resource": "*"
        }
    ]
}
```
- User storage namespace permissions - The user should provide an AWS role to list, write, and delete (can use the role as the GC the user configures when creating a new cloud installation).

**Backup & Restore**

Before running the UGC on the repository, perform a backup operation. Copy the files to the prefix ```_lakefs/retention/ugc/backup/run-id/```
The user should enable bucket versioning to delete it after 2 days.
The backup will be used to restore in case the UGC deleted unwanted files.

**Deployment of new UGC version**

The JAR the MUGC spark job is using will be located on s3 (ugc-jars-bucket). When wanting to upgrade the UGC version on the MUGC, upload a new jar to the bucket and change the spark job configuration to use the new one.
We should have the option to configure the JAR per organization, in case we want to revert a version on a specific user.

**UGC client and server compatibility**

When upgrading the lakeFS server version or UGC client version to a cloud user needs to check if both fit together.

**SLA** - 24 hours to delete a file from the moment it became eligible for deletion.

### Metrics and Logging
- Collect the UGC runs logs and metrics using [AWS Cloud Watch](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-metrics.html)
- Alerti configuration / on-call guide - triggering for failed job runs P3/P4 alerts (depending on SLA), instructions to the R&D on the UGC management.
- Access to the cloud user metadata for debugging (`_lakeFS`). Where the prepare uncommitted file is saved and the run report.

### Testing
All the tests are prerequisites for releasing the MUGC.
- Test UGC on scale (WIP)
    - (Existing cloud user dev repo) Performance and correctness - mark only
        - Ensure only objects that are eligible for deletion are marked for deletion
- Existing cloud user production repo (WIP)
    - Old repository structure performance
- Compatibility tests - UGC client and lakeFS server
- Test sweep functionality on production
- E2E tests for UGC cloud (TODO - add details)

### Performance improvements
- In case one of the tests will suggest a long running time, consider adding performance improvement:
    1. [Optimaized listing on old repository structure](https://github.com/treeverse/lakeFS/issues/4620)
    2. [Implement optimized run flow](https://github.com/treeverse/lakeFS/issues/4489)
    3. [Efficient listing on committed entries](https://github.com/treeverse/lakeFS/issues/4600)

From the testing UGC on a scale of existing users, it is suggested that the bottleneck on the UGC run is in listing the committed entries (meta ranges and ranges) more than 99% of the job run time. Therefore, I suggest starting with performance improvement of it.

### Milestones
1. Pre-tests
    * Existing cloud user tests - scale, correctness, production
    * Performance improvements / Bug fixes (if needed)
2. Managed UGC job infrastructure
    * AWS lambda funtion script
        * Get UGC configuration from the control plane
        * Adding permissions to the UGC
        * EMR serverless spark application step
    * AWS cloud watch triggering of the lambda function
3. Tests and visibility
    * E2E tests
    * Monitoring, alerting, and playbooks
