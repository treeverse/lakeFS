# Uncommitted Garbage Collection - Milestone 2 Execution Plan

Uncommitted Garbage Collection [Proposal](https://github.com/treeverse/lakeFS/blob/master/design/accepted/gc_plus/uncommitted-gc.md)

## Milestone 1

The first beta version that was released included:
1. Implementation of the clean run flow for old and new repository structures (without optimizations)
2. Mark & Sweep
3. Integration tests
4. Backup & Restore - minimal support using rclone

## Milestone 2

### Goals
1. Removing the limitation of a read-only lakeFS during the GC+ job run
2. Performance improvements - better parallelization of the storage namespace traversal
3. Implementing Optimized (Incremental) Run

### Non-Goals
1. Support for non-S3 repositories
   * Azure
   * GCP
2. Incorporation of committed & uncommitted GC into a single job
    * Including GC changes, configuration, and behavior changes to fit GC
3. Metrics and Logging additions
4. Deployment to lakeFS Cloud
5. Improve Backup & Restore

### Plan

* marks dependency

1. Required changes by lakeFS:
    * [[Get/Link]PhysicalAddress](https://github.com/treeverse/lakeFS/issues/4476)
        * [Validation of cutoff time](https://github.com/treeverse/lakeFS/issues/4695)
    * [StageObject API](https://github.com/treeverse/lakeFS/issues/4480)
    * [CopyObject API](https://github.com/treeverse/lakeFS/issues/4477)
        * [S3 Gateway CopyObject](https://github.com/treeverse/lakeFS/issues/4478)
        * [lakeFSFS renameObject method](https://github.com/treeverse/lakeFS/issues/4479)
    * [Track copied objects in ref-store](https://github.com/treeverse/lakeFS/issues/4562)

2. Performance improvements:
    * [Optimaized listing on old repository structure](https://github.com/treeverse/lakeFS/issues/4620)
    * [Efficient listing on committed entries](https://github.com/treeverse/lakeFS/issues/4600)
    * Benchmarks - verify uncommitted GC [performance requirements](https://github.com/treeverse/lakeFS/blob/e316cafe7717bb3203e4018837a41415aa61f74b/design/accepted/gc_plus/uncommitted-gc.md?plain=1#L185) are kept

3. [Implement optimized run flow](https://github.com/treeverse/lakeFS/issues/4489):
    * Start with Integration tests (TDD)

By the end of this milestone, we will release a new beta version that includes all the additions.

**Due date: 01/01/2023**
