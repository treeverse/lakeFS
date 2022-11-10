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
   - Creates uncommitted objects list files and save them in a designated GC path on the object store
   - After this is done, we can implement new uncommitted logic in GC

2. Modify [Get/Link]PhysicalAddress
    - Issue validation token on GetPhysicalAddress with an expiry time and add it to valid token list
    - LinkPhysicalAddress will validate token and remove it from valid token list

3. Add tracking of copied objects in ref-store
    - On each copy operation - add an entry to the copied object table in ref-store
    - Periodically delete entry or during defined operations (commit/ delete/reset branch)

4. Implement CopyObject API
    - Read staging token from branch
    - Get entry from branch
    - If entry is staged
      - Update copy table
      - Create a shallow copy
    - If entry is committed or on different branches - perform a full copy using the underlying storage adapter copy method
    - Stage new entry using old entry data and new path

5. Modify Gateway CopyObject API
    - See details above

6. Modify lakeFSFS renameObject method
    - Use CopyObject + DeleteObject when working via Gateway or OpenAPI

7. lakeFS clients
    - Any action required on clients?
    - Do we need to make them version aware?
    - Should we issue a notification to upgrade clients?

8. Modify StageObject API
    - Return error if given address is inside repository namespace

9. Implement new repository structure
    - Root prefix for new lakeFS data - `data/`
    - Use slice naming conventions as defined in proposal to create object path
    - Track object upload count
    - Create new slice by time or count

## Changes on GC

- New logic in GC to support uncommitted garbage collection
  - Prepare uncommitted for GC
  - Read uncommitted data from lakeFS
  - Implement the optimized run deltas
- Incorporate uncommitted changes into the current GC flow 
  - Changes required for committed data to support uncommitted flow 
  - Support mark and sweep for uncommitted 
  - integrate uncommitted and committed GC flows 

## Testing

- Functionality - Basic GC functionality, check that committed GC logic not broken, uncommitted GC works as expected
- Data Integrity - Ensure only objects that are eligible for deletion are hard deleted from repository
- Performance - Verify uncommitted GC performance requirements are kept
- Existing repository - test first run on an old repository, with and without objects in new repository structure. Ensure consecutive runs work as expected (in Optimized Run)

## Milestone

- Implement Clean Run (including all lakeFS change) with minimal performance requirements met
- Implement GC changes - Optimized Run + mark and sweep
- Deployment to lakeFS Cloud
