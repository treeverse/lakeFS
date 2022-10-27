# Uncommitted Garbage Collection - Execution Plan

Uncommitted Garbage Collection [Proposal](link-to-proposal)

## Overview

Implementing uncommitted GC can be divided into 3 main changes:
1. lakeFS additions and modifications
2. Implementing uncommitted GC logic on client side
3. Modifying existing GC

All changes in lakeFS are independent changes, work on lakeFS changes can start immediately.
Some GC changes are dependent on lakeFS changes, these are prioritized as part of the plan.  
All tasks are tracked under the [GC+ label](https://github.com/treeverse/lakeFS/labels/GC%2B) in GitHub

## Changes on lakeFS

The following are a list of changes in lakeFS by order of priority, according the dependency constraints 

1. Implement PrepareUncommittedForGC API
   - Creates metaranges and ranges for a selected branch's staging area, and writes them to a designated GC path
   - After this is done, we can implement new uncommitted logic in GC

2. Modify [Get/Link]PhysicalAddress
    - Issue validation token on GetPhysicalAddress with an expiry time and add it to valid token list
    - LinkPhysicalAddress will validate token and remove it from valid token list

3. Implement Move/RenameObject API
    - Get entry from branch
    - Stage new entry using old entry data and new path 
    - Perform delete for old entry (write tombstone)

4. Modify Gateway CopyObject API
    - For copy operations on different branches, change CopyObject behavior to perform "full copy" using the underlying 
   storage adapter's Copy method.

5. Modify lakeFSFS renameObject method
    - Use CopyObject + DeleteObject when working via Gateway
    - Use RenameObject when working with OpenAPI

6. lakeFS clients
    - Any action required on clients?
    - Do we need to make them version aware?
    - Should we issue a notification to upgrade clients?

7. Modify StageObject API
    - Return error if given address is inside repository namespace

8. Implement new repository structure
    - Root prefix for new lakeFS data - `data/`
    - Use slice naming conventions as defined in proposal to create object path
    - Track object upload count
    - Create new slice by time or count

## Changes on GC

- New logic in GC to support uncommitted garbage collection
  - Create uncommitted metaranges
  - Ingest uncommitted data from lakeFS
  - Write uncommitted data to the object store (as part of the reports)
  - Implement the optimized run deltas
- Incorporate uncommitted changes into the current GC flow
  - Implement Run ID proposal + [changes](https://github.com/treeverse/lakeFS/issues/4469)
  - Add additional metadata to the GC report (last scanned slice)
  - Support mark and sweep for uncommitted 

## Testing

- Functionality - Basic GC functionality, check that committed GC logic not broken, uncommitted GC works as expected
- Data Integrity - Ensure only objects that are eligible for deletion are hard deleted from repository
- Performance - Verify uncommitted GC performance requirements are kept
- Existing repository - test first run on an old repository, with and without objects in new repository structure. Ensure consecutive runs work as expected (in Optimized Run)

## Milestone

- Implement Clean Run (including all lakeFS change) with minimal performance requirements met
- Implement GC changes - Optimized Run + mark and sweep
- Deployment to lakeFS Cloud
