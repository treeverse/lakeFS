# Managed Uncommitted Garbage Collection for lakeFS cloud - Execution Plan

## Goals
1. Run UGC for lakeFS cloud users
2. Allow Cloud users to clean up storage space by deleting objects from the underlying storage for cost efficiency
3. Support new and existing cloud users (both repository structures)

## Non-Goals
1. Remove lakeFS limitations during the UGC job run
2. Improve UGC OSS support

### User Story
When creating a new cloud installation - on the GC initializations page,
add UGC initialization (default run job=true for all repositories, default sweep = true).
It can get the same rules as the GC.
Add a link to the documentation costrains (lakeFS writes during the job, explain debugging and report).

### Permissions
Create default UGC user with permissions for every new cloud env (The permissions to Enigma was added to the GC user)
- lakeFS permissions - prepare uncommitted gc
- Bucket permissions - role to list, write, delete

### Managed UGC job
- Control plane interface (design with cloud native team)
  - Add step for the create cloud installation
  - Configuration for the spark job (configured role, secret and access key, installation endpoint, repository)
  - Backup & Restore
- Deployment of new UGC version
- UGC client and server compatibility
- Run the UGC periodically (AWS lambda cron job, Airflow DAG, EMR serverless, etc.)
- Declare SLA (might require performance improvements)

### Metrics and Logging
- Add UGC logging and metrics to the cloud monitoring tools
- Alerti configuration / on-call guide - for failed job runs add P3/P4 alerts 
- Access to the cloud user metadata for debugging

### Testing
- Test UGC on scale
  - (Enigma dev repo) Performance and correctness - mark only (WIP)
    - Ensure only objects that are eligible for deletion are marked for deletion
- Enigma production repo
  - Old repository structure performance
- Compatibility tests - UGC client and lakeFS server
- Test sweep functionality
- E2E tests for UGC cloud

### Performance improvements
- In case one of the tests will suggest long running time, consider adding performance improvement (incremental run, parallel listing, etc.)
  * [Optimaized listing on old repository structure](https://github.com/treeverse/lakeFS/issues/4620)
  * [Efficient listing on committed entries](https://github.com/treeverse/lakeFS/issues/4600)
  * [Implement optimized run flow](https://github.com/treeverse/lakeFS/issues/4489)

### Milestones
