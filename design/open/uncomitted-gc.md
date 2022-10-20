# Uncommitted Garbage Collector

## Motivation

Uncommitted data which is no longer referenced (due to branch deletion, reset branch etc.) is not being deleted by lakeFS.
This may result excessive storage usage and possible compliance issues.
To solve this problem two approaches were suggested:
1. A batch operation performed as part of an external process (GC)
2. An online solution inside lakeFS

Several attempts for an online solution have been made, most of which are documented [here](Link-to-hard-delete-proposal).
This document will describe the **offline** GC process for uncommitted objects.

Garbage collection of uncommitted data will be performed using the same principles of the current GC process.
The basis for this is a GC Client (i.e. _Spark_ job) consuming objects information from both lakeFS and directly from the underlying object storage, 
and using this information to determine which objects on the bucket can be deleted.

For simplicity, in this document we will reduce this problem to the repository and branch level.

## Design

### Required changes by lakeFS

The following are necessary changes in lakeFS in order to implement this proposal successfully.

#### Objects Path Conventions

1. Repository objects will be stored under the prefix `<storage_namespace>/repos/<repo_uid>/`
2. For each repository branch, objects will be stored under the repo prefix with the path `branches/<branch_id>/`
3. Each lakeFS instance will create a unique prefix partition under the branch path. This will be used by the specific
lakeFS instance to store the branch's objects. This prefix will be composed of two parts:
   1. Lexicographically sortable, descending time based serialization
   2. A unique identifier for the lakeFS instance
      `<sortable_descending_serialized_uid>_<lakefs_instance_uid>`  
This serialized partition prefix will allow partial scans of the bucket when running the optimized GC
4. The lakeFS instance will track the count of objects uploaded to the prefix and create a new partition every < TBD > objects uploaded

The full object path for an object `file1` uploaded to repository `my-repo` on branch `test-branch` will be as follows:  
    
    <lakeFS_bucket>/repos/my-repo/branches/test-branch/<lakeFS_instance_partition_uid>/<file1_uid>

#### StageObject

The StageObject operation will only be allowed on addresses outside the repository's storage namespace. This way, objects added using this operation are never collected by GC.

#### Get\Link PhysicalAddress 

1. GetPhysicalAddress to return a validation token along with the address.
2. The token will be valid for a specified amount of time and for a single use.
3. LinkPhysicalAddress to verify token valid before creating an entry.
    1. Doing so will allow us to use this time interval to filter objects that might have been uploaded and waiting for
       the link API and avoid them being deleted by the GC process.
    2. Objects that were uploaded to a physical address issued by the API and were not linked before the token expired will
       eventually be deleted by the GC job.

#### CopyObject

1. Copy object in the same branch will work the same - creating a new staging entry using the existing entry information.
2. For objects that are not part of the branch, use the underlying adapter copy operation.
3. "Move/Rename" operation will need to use the new version of CopyObject instead of StageObject.

With the above changes, bucket data is partitioned by repositories, branches and finally a bounded partition.
This allows running the GC process at branch level, reduces the data scan size and allows parallelization.

#### PrepareUncommittedForGC

A new API which will create meta-ranges and ranges using the given branch uncommitted data. These files
will be saved in a designated path used by the GC client to list branch's uncommitted objects.  
For the purpose of this document we'll call this the `GC commit` (feel free to suggest a better term :) )

#### Reading data from lakeFS

Reading data from lakeFS will be similar to the current GC process - using the SSTable reader to quickly list branch objects.
The above-mentioned API will enable doing so for all branch objects (committed and uncommitted).

### GC Flows

The following describe the GC process run flows in the branch scope:

#### Flow 1: Clean Run

1. Run PrepareUncommittedForGC
2. Read all addresses from branch commits -> `lakeFS DF`
3. Read all addresses from branch `GC commit` -> `lakeFS DF`
4. Read all objects on branch path directly from object store (can be done in parallel by 'partition') -> `Branch DF`
5. Subtract results `lakeFS DF` from `Branch DF`
6. Remove files newer than < TOKEN_EXPIRY_TIME > and special paths
7. The remainder is a list of files which can be safely removed
8. Finally, save the current run's `GC commit` the last read partition and newest commit id in a designated location on the branch path

#### Flow 2: Optimized Run

Optimized run uses the previous GC run output, to perform a partial scan of the branch to remove uncommitted garbage.

##### Step 1. Analyze Data and Perform Cleanup for old entries (GC client)

1. Run PrepareUncommittedForGC
2. Read addresses from branch's new commits (all new commits down to the last GC run commit id) -> `lakeFS DF`
3. Read addresses from branch `GC commit` -> `lakeFS DF`
4. Subtract results `lakeFS DF` from previous run's `GC commit`
5. The result is a list of files that can be safely removed

>**Note:** This step handles cases of objects that were uncommitted during previous GC run and are now deleted

##### Step 2. Analyze Data and Perform Cleanup for new entries (GC client)
1. Read all objects on branch path down to the previous run's last read partition (can be done in parallel by 'partition') -> `Branch DF`
2. Subtract `lakeFS DF` from `Branch DF`
3. Filter files newer than < TOKEN_EXPIRY_TIME > and special paths
4. The remainder is a list of files which can be safely removed
5. Finally, save the current run's `GC commit` the last read partition and newest commit id in a designated location on the branch path

## Limitations

* Since this solution relies on the new repository structure, it is not backwards compatible. Therefore, another solution will be required for existing 
repositories
* This solution partitions the branch data into segments that allow parallel listing of branch objects. In cases where
the branch path is extremely large, it might still take a lot of time to perform a clean run.
* For GC to work optimally, it must be executed on a timely basis. Long periods between runs might result in excessively long runs.