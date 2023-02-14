# Managed Uncommitted Garbage Collection for lakeFS cloud - Execution Plan

## Goals
1. Run UGC for lakeFS cloud users
2. Allow Cloud users to clean up storage space by deleting objects from the underlying storage for cost efficiency
3. Support new and existing cloud users (both repository structures)

## Non-Goals
1. Remove lakeFS limitations during the UGC job run
2. Improve UGC OSS support

### User Story
For every cloud installation, The UGC will run as default as a background job (sweep=true) without visibility to the user to control its configuration.
We should have to ability to control the MUGC for the user - change configuration, or stop it.

### Permissions
Create default UGC user with permissions for every new cloud env (The permissions to Enigma was added to the GC user)
- lakeFS permissions - prepare uncommitted gc
- Bucket permissions - role to list, write, delete (can get the same role as the GC)

### Managed UGC job
- Control plane interface (design with cloud native team)
  - Add a step to initialize the MUGC job when creating a new cloud installation
  - Configuration for the spark job (configured role, secret and access key, installation endpoint, repository)
  - Backup & Restore
- Deployment of new UGC version
- Migration (Enigma)
- UGC client and server compatibility
- Run the UGC periodically (AWS lambda cron job, Airflow DAG, EMR serverless, etc.)
- Declare SLA (might require performance improvements)

### Metrics and Logging
- Add UGC logging and metrics to the cloud monitoring tools
- Alerti configuration / on-call guide - for failed job runs add P3/P4 alerts 
- Access to the cloud user metadata for debugging

### Testing
All the tests are pre-requisites for releasing the MUGC.
- Test UGC on scale (WIP)
  - (Enigma dev repo) Performance and correctness - mark only
    - Ensure only objects that are eligible for deletion are marked for deletion
- Enigma production repo (WIP)
  - Old repository structure performance
- Compatibility tests - UGC client and lakeFS server
- Test sweep functionality
- E2E tests for UGC cloud

### Performance improvements
- In case one of the tests will suggest long running time, consider adding performance improvement:
  1. [Optimaized listing on old repository structure](https://github.com/treeverse/lakeFS/issues/4620)
  2. [Implement optimized run flow](https://github.com/treeverse/lakeFS/issues/4489)
  3. [Efficient listing on committed entries](https://github.com/treeverse/lakeFS/issues/4600)

### Milestones
1. Pre-tests
   * Enigma tests - scale, correctness, production
   * Performance improvements / Bug fixes (if needed)
2. Managed UGC job infrastructure
    * Cron job - decide on which platform the MUGC will run (same as GC, new one)
    * Permissions - add permissions for the UGC job
    * Control plane implementation - add a step in the installation provisioning to trigger and manage the UGC job
3. Tests and visibility
   * E2E tests
   * Metrics and logging