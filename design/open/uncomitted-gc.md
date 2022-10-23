# Uncommitted Garbage Collector

## Motivation

Uncommitted data which is no longer referenced (due to branch deletion, reset branch etc.) is not being deleted by lakeFS.
This may result excessive storage usage and possible compliance issues.
To solve this problem two approaches were suggested:
1. A batch operation performed as part of an external process (GC)
2. An online solution inside lakeFS

Several attempts for an online solution have been made, most of which are documented [here](Link-to-hard-delete-proposal).
This document will describe the **offline** GC process for uncommitted objects.

## Design

Garbage collection of uncommitted data will be performed using the same principles of the current GC process.
The basis for this is a GC Client (i.e. _Spark_ job) consuming objects information from both lakeFS and directly from the underlying object storage,
and using this information to determine which objects on the bucket can be deleted.

The GC process is composed of 3 main parts:
1. Listing namespace objects
2. Listing of lakeFS repository committed objects
3. Listing of lakeFS repository uncommitted objects

Objects that are found in 1 and are not in 2,3 can be then safely deleted by the Garbage Collector.

### 1. Listing namespace objects

Since listing might be a long operation for very large repositories, we want to find a way to optimize it.
The suggested way is to split the repository structure into fixed size (upper bounded) partitions which will allow scanning each partition 
concurrently using multiple workers.
The partitions will also be created in a manner which will allow us to take advantage of a common object storage property, which lists the objects
in a lexicographical order. Using this property we can implement additional optimizations on the GC process.

![Repository Structure](diagrams/uncommitted-gc-repo-struct.png)

### 2. Listing of lakeFS repository committed objects

Similar to the way GC works today, use repository meta-ranges and ranges to read all committed objects in the repository. 

### 3. Listing of lakeFS repository uncommitted objects

Creating special meta-ranges and ranges using branches' uncommitted data, we can leverage the already existing GC logic to
quickly read all uncommitted objects in the repository.

### Required changes by lakeFS

The following are necessary changes in lakeFS in order to implement this proposal successfully.

#### Objects Path Conventions

Uncommitted GC must scan the bucket in order to find objects that are not referenced by lakeFS.
To optimize this process, suggest the following changes:

1. Divide the repository namespace into time-and-size based partitions
2. The partitions names will be lexicographically sortable, descending strings which will enables listing the repository
objects from new to old.
3. LakeFS will create a new partition on a timely basis (for example: hourly) or when it has written < TBD > objects to it.
4. Each partition will be written by a single lakeFS instance in order to track partition size.
5. Tge sorted partitions will enable partial scans of the bucket when running the optimized GC.

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

#### S3 Gateway CopyObject

1. Copy object in the same branch will work the same - creating a new staging entry using the existing entry information.
2. For objects that are not part of the branch, use the underlying adapter copy operation.

#### Move/RenameObject
Clients working through the S3 Gateway can use the CopyObject + DeleteObject to perform a Rename or Move operation.
For clients using the OpenAPI this could have been done using StageObject + DeleteObject.
To continue support of this operation, introduce a new API to rename an object which will be scoped to a single branch.

#### PrepareUncommittedForGC

A new API which will create meta-ranges and ranges using the given branch uncommitted data. These files
will be saved in a designated path used by the GC client to list branch's uncommitted objects.  
For the purpose of this document we'll call this the `BranchUncommittedMetarange`

### GC Flows

The following describe the GC process run flows in the branch scope:

#### Flow 1: Clean Run

1. Run PrepareUncommittedForGC on all branches
2. Read all addresses from Repository commits -> `Committed DF`
3. Read all addresses from branch `BranchUncommittedMetarange` -> `Uncommitted DF`
4. Read all objects directly from object store (can be done in parallel by 'partition') -> `Branch DF`
5. Subtract results `Committed DF` and `Uncommitted DF` from `Branch DF`
6. Remove files newer than < TOKEN_EXPIRY_TIME > and special paths
7. The remainder is a list of files which can be safely removed
8. Finally, save the following in a designated location in the repository:
   1. The current run's `Uncommitted DF` (as a parquet file) 
   2. The last read partition 
   3. The GC run start timestamp 

#### Flow 2: Optimized Run

Optimized run uses the previous GC run output, to perform a partial scan of the branch to remove uncommitted garbage.

##### Step 1. Analyze Data and Perform Cleanup for old entries (GC client)

1. Run PrepareUncommittedForGC
2. Read addresses from branch's new commits (all new commits down to the last GC run commit id) -> `lakeFS DF`
3. Read addresses from branch `BranchUncommittedMetarange` -> `lakeFS DF`
4. Subtract results `lakeFS DF` from previous run's `BranchUncommittedMetarange`
5. The result is a list of files that can be safely removed

>**Note:** This step handles cases of objects that were uncommitted during previous GC run and are now deleted

##### Step 2. Analyze Data and Perform Cleanup for new entries (GC client)
1. Read all objects on branch path down to the previous run's last read partition (can be done in parallel by 'partition') -> `Branch DF`
2. Subtract `lakeFS DF` from `Branch DF`
3. Filter files newer than < TOKEN_EXPIRY_TIME > and special paths
4. The remainder is a list of files which can be safely removed
5. Finally, save the following in a designated location in the repository:
    1. The current run's `Uncommitted DF` (as a parquet file)
    2. The last read partition
    3. The GC run start timestamp

## Limitations

* Since this solution relies on the new repository structure, it is not backwards compatible. Therefore, another solution will be required for existing 
repositories
* For GC to work optimally, it must be executed on a timely basis. Long periods between runs might result in excessively long runs.