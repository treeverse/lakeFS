# Uncommitted Garbage Collector

## Motivation

Uncommitted data which is no longer referenced (due to branch deletion, reset branch etc.) is not being deleted by lakeFS.
This may result excessive storage usage and possible compliance issues.
To solve this problem two approaches were suggested:
1. A batch operation performed as part of an external process (GC)
2. An online solution inside lakeFS

Several attempts for an online solution have been made, most of which are documented [here](Link-to-hard-delete-proposal).
This document will describe the **offline** GC process for uncommitted objects.

For simplicity, in this document we will reduce this problem to the repository level. 

## Design

Garbage collection of uncommitted data will be performed in 2 flows. Initially, we will require to perform a full scan of 
the bucket / path to inventory all the existing objects and determine which are not part of the repository anymore (not committed or staged).
After performing the initial run, consecutive runs can be performed in a more efficient by using the already analyzed data to reduce
run time.

In order to optimize the GC run, we will use the following methods:
1. Use the Storage Provider Inventory capabilities
2. Save a list of uncommitted objects TODO: add how

**Note:** Both AWS S3 and Azure Blob provide a built-in inventory feature which can be scheduled to create an inventory file, which lists all
objects on the given path at a given point in time. Though Google Storage does not provide this functionality as is, we can simulate this behavior
using the [Assets API](https://cloud.google.com/asset-inventory/docs/exporting-to-cloud-storage) or [Audit Logs](https://cloud.google.com/storage/docs/audit-logging).
{: .note }

The basic idea for the design is to have two inventory files, one created by the Storage Provider and the other by lakeFS.
These files will then be consumed by a GC client (i.e. _Spark_) and used to determine which objects can be deleted from the bucket. This approach has several advantages:
1. Minimize the amount of listings performed on the bucket (either by lakeFS or the GC client)
2. Most of the heavy lifting is being done on the GC Client side
3. Take advantage of the GC Client's ability to process big datasets efficiently

### Terminology

For the purpose of this document we will use the following terms:
1. `Storage Inventory File` - File created by the Storage Provider Inventory Job containing list of all repository objects at the given time
2. `Uncommitted Inventory File` - File containing the list of uncommitted objects at the given time - created by lakeFS
3. `Committed Objects List` - List of repository's committed objects from a given point of time (or genesis) TODO: clarify point of time committed vs uncommitted
4. `Uncommitted Objects List` - List of repository's uncommitted objects at the given time
5. `Repository Inventory File` - File containing the list of all repository objects (committed + uncommitted) at the given point in time
6. `Last Commit` - The newest commit scanned by the last GC run

### Required changes by lakeFS

1. StageObject API allowed only for address outside the repo namespace
    1. Prevent race between staging object and GC job
2. GetPhysicalAddress to return a validation token along with the address.
    1. The token will be valid for a specified amount of time and for a single use
3. LinkPhysicalAddress to verify token valid before creating an entry
    1. Doing so will allow us to use this time interval to filter objects that might have been uploaded and waiting for
       the link API and avoid them being deleting by the GC process
    2. Objects that were uploaded to a physical address issued by the API and were not linked before the token expired will
       eventually be deleted by the GC job.
4. Introduce a new lakeFS API to process and create the lakeFS `Repository Inventory File`.

### Flow 1: Clean Run

Will run when `Uncommitted Inventory File` doesn't exist and will scan the entire namespace.

#### Step 1. Build lakeFS inventory file (lakeFS API)

1. Create `Committed Objects List` from genesis
2. Create `Uncommitted Objects List`
3. Write `Uncommitted Inventory File` using the given list - this will be used by future GC run using `Optimized Run`.
4. Write `Repository Inventory File` using the given lists

#### Step 2. Analyze Data and Perform Cleanup (GC client)

1. Perform subtraction of `Repository Inventory File` from `Storage Inventory File`
2. Filter out all objects newer than <token expiry time>
3. The remainder is a list of files which can be safely removed

### Flow 2: Optimized Run

Optimized run uses the previous GC run output, to perform a partial scan of the repository to remove uncommitted garbage.

#### Step 1. Build lakeFS inventory file (lakeFS API)

1. Create `Committed Objects List` from commits up to `Last Commit` timestamp
2. Create `Uncommitted Objects List`
3. Write `Uncommitted Inventory File`
4. Write `Repository Inventory File` using the given lists. 

#### Step 2. Analyze Data and Perform Cleanup for old entries (GC client)
1. Read previous runs' `Uncommitted Inventory File`
2. Perform subtraction of `Repository Inventory File` from result
3. The result is a list of files that can be safely removed

**Note:** This step handles cases of objects that were uncommitted during previous GC run and are now deleted
{: .note }

#### Step 3. Analyze Data and Perform Cleanup for new entries (GC client)
1. Read `Storage Inventory File`
2. Filter files with creation date <= last GC run timestamp 
3. Perform subtraction of `Repository Inventory File` from result
4. Filter out all files newer than <token expiry time>
5. The remainder is a list of files which can be safely removed
