# Hard Delete Uncommitted Data

## Decision

No solution found that covers all scenarios and edge cases in an adequate way. Decided to shelve the effort for finding an
online solution for now, and concentrate on the offline solution.

## Motivation

Uncommitted data which is no longer referenced (due to branch deletion, reset branch etc.) is not being deleted by lakeFS.
This may result excessive storage usage and possible compliance issues.
To solve this two approaches were suggested:
1. A batch operation performed as part of an external process (GC)
2. An online solution inside lakeFS

**This document details the latter.**

Several approaches were considered and described in this document. All proposal suffer from unresolved design issues, and
so no specific approach was selected as a solution.

### Base assumptions

The following assumptions are required in all the suggested proposals:
1. Copy operation requires to be changed and implemented is full copy. (*irrelevant for suggestion no.3)
2. lakeFS is not responsible for retention of data outside the repository path (data ingest)
3. An online solution is not bulletproof, and will require a complementary external solution to handle edge cases as well as
existing uncommitted garbage.

## 1. Write staged data directly to object store on a defined path

This proposal suggests moving the staging area management from the ref store to the object store and defining a structured path for
the object's physical address on the object store.

### Performance degradation when using the object store

Due to the basic concept of handling the staging data in the objects store, this proposal suffers from a significant performance 
degradation in one of lakeFS's principal flows - **listing**. Listing is used in flows such as: list branch objects, getting objects and committing.
We tested listing of ~240000 **staged objects** using `lakeFS ls` and comparing it with `aws s3 ls` with the following results:
> ./lakectl fs ls lakefs://niro-bench/main/guy-repo/itai-export/medium/ >   6.42s user 0.78s system 44% cpu 16.089 total

> aws s3 ls s3://guy-repo/itai-export/medium/ > /dev/null  56.82s user 2.34s system 74% cpu 1:19.79 total

### Design

Objects will be stored in a path relative to the branch and staging token. For example: file `x/y.z` will be uploaded to path
`<bucket-name>/<repo-name>/<branch-name>/<staging-token>/x/y.z`.  
As result, uncommitted objects are all objects on a path of staging tokens which were not yet committed.  
When deleting a branch, each object under the branch's current staging token can be deleted.

### Blockers

1. Commit flow is not atomic:
   1. Start write on staged object
   2. Start Commit
   3. Commit flow writes current object to committed tables
   4. Write to object is completed - changing the data after it was already committed

### Opens
1. Solve the blocker - how to prevent data modification after commit? 
2. How to manage ingested data?
3. How to handle SymLink and stageObject operations?

## 2. Reference Counter on Object Metadata

This proposal suggested to maintain a reference counter in the underlying object's metadata and use the different block adapters
capabilities to perform atomic, concurrently safe incrementation of this counter.
The way we are handling the staged data in lakeFS, will not change in any other way.
**The proposal is not viable since though Google Storage and Azure provide this capability, AWS S3 does not.** 

This solution incurs an additional metadata read (and possible write) in case it is in the repo namespace in selected operations.

### Blockers

1. AWS does not support conditional set of metadata

### Flows

#### Upload Object

1. Add a reference counter to the object's metadata and set it to one.
2. Write the object to the object store the same way we do today.

#### Stage Object

1. Object path is in the repo namespace:
   1. read its metadata, 
      1. If counter > 0, increment the counter and update the metadata on the object. Otherwise, treat as deleted. 
   2. If the object's write to object store fails - rollback the metadata update. 
2. Object path is outside the repo namespace:
   1. lakeFS will not handle its retention - therefore it will have to be deleted by the
   user or by a dedicated GC process

#### LinkPhysicalAddress

Assume this API uses a physical address in the repo namespace
1. Read object's metadata
2. Increment counter and write metadata
3. Add staging entry

#### Delete Object

1. Read object's metadata
2. Reference counter exists:
   1. Decrement counter
   2. if counter == 0
      1. Hard-delete object 
3. If reference counter doesn't exist:
   1. Retention is not handled in lakeFS

#### Reset / Delete Branch

When resetting a branch we throw all the uncommitted branch data.
We can leverage the new KV drop async functionality to also check and hard-delete objects as needed

### Atomic reference counter

For each storage adapter (AWS, GC, AZ), we will use a builtin logic to provide this functionality

#### AWS

AWS doesn't allow to update an object or its metadata after creation. In order to update the metadata a copy object should be performed with the new metadata values. The only 
conditionals currently available are on the objects timestamp and ETag. Unfortunately ETags are modified only on objects content change and does not take into account changes in metadata.

#### Azure

Store the reference counter in the blob's metadata and use `set blob metadata` API with the supported conditional header on the ETag to perform an If-Match operation and update
the reference counter


#### Google Store

Store the reference counter in the blob's metadata, use the `patch object` API with the `ifMetagenerationMatch` conditional.


## 3. Tracking references in staging manager

Staging manager will track physical addresses of staged objects and the staging tokens referencing them.
We will introduce a new object to the database:

      key    = <repo>/references/<physical_address>/<staging_token>  
      value  = state enum (staged|deleted|comitted)
               last_modified timestamp

On Upload object to a branch, we will add a key in the references path with the physical address and current staging token and 
mark it as staged.   
On Delete object from a staging area, we will update the reference key with value `deleted`  
On Commit, we will update the reference key with value `comitted`

A background job will be responsible for scanning the references prefix, and handling the references according to the state.
The `last_modified` parameter is used to prevent any race conditions between reference candidates for deletion and ongoing operations 
which might add references for this physical address. The assumption is that when all references of a physical address 
are in `deleted` state, after a certain amount of time (TBD), this physical address cannot be referenced anymore (all staging tokens were either dropped or committed).

### Background delete job (pseudo-code)

1. Scan `reference` prefix (can use after prefix for load balancing)
2. For each `physical_address` read all entries
      1. If found state == 'committed' in any entry
         1. Delete all keys for `physical_address`, by order of state: deleted -> staged -> committed
      2. If state == 'deleted' in all entries and min(`last_modified`) < `TBD`
         1. Hard-delete object
         2. Delete all keys for `physical_address`

### Key Update Order of Precedence

1. `commited` state takes precedence over all (terminal state - overrides any other value) - uses **Set**
2. `deleted` state can only be done on entries which are not `committed` and uses **SetIf**
3. `staged` state can only be done on entries which are not `committed` and uses **SetIf** 

### Flows

#### Upload Object

1. Write blob
2. Write reference entry to the database as staged
3. Add staged entry to database
4. If entry exists in current staging token - mark old reference as deleted
**Open:** Efficiently deal with overrides 

#### Stage Object

1. Object path is in the repo namespace:
   1. Add reference entry to the database 
2. Object path is outside the repo namespace:
   1. lakeFS will not handle its retention
3. Add staged entry

#### GetPhysicalAddress
Get physical address will add a reference to the generated unique physical address with state `deleted`
We will then return a valid token / expiry timestamp to the user in addition to the physical address
The user will need to pass the token to LinkPhysicalAddress

#### LinkPhysicalAddress
Add a validation to check if the token provided / or timestamp expired.
Set reference state as staged.

Assume this API uses a physical address in the repo namespace
1. Add reference entry to the database
2. Add staging entry

#### Delete Object

1. If object staged
   1. Read reference key
   2. If not `committed` 
      1. Change reference state to `deleted` and update `last_modified`

#### Commit

1. Mark all entries in staging area as `committed`
2. Swap staging token
3. Create commit
4. Update branch commit ID

#### Reset / Delete Branch

When resetting a branch we throw all the uncommitted branch data, this operation happens asynchronously via `AsyncDrop`.
We can leverage this asynchronous job to also check and perform the hard-delete
1. Scan deleted staging token entries
2. If entry is `tombstone` remove entry reference key
3. Otherwise, modify reference state to `deleted` and update `last_modified`

## 4. Staging Token States

This is an improvement over the 3rd proposal which suggests tracking the state on the staging token instead of objects,
while still keeping track of references (without state)

This will be done using the following objects:

References will be tracked in a list instead of an explicit entry per reference
    
    key    = <repo>/references/<physical_address>  
    value  = list(staging tokens)

For each staging token of a deleted / reset branches a deleted entry will be added

    <repo>/staging/deleted/<staging_token>

For each commit we will add the staging tokens to a committed entry.
Adding a reference is done using `set if` and a retry mechanism to ensure consistency.

    key    = <repo>/staging/committed/<physical_address>  
    value  = list(staging tokens)

### Flows

#### Upload Object

1. Write blob
2. Read reference entry
3. Perform `set if` on the reference with the additional staging token
4. Add staged entry to database

* Delete object will not remove the reference
* References to objects are kept as long as staging token is not deleted or committed.

#### Delete Object

Deleted object will remain the same. We do not delete references in this flow.

#### Stage Object
Allow stage object operations only on addresses outside the repository namespace
lakeFS will not manage the retention of these objects

#### GetPhysicalAddress

Provide a validation token with the issued physical address. The issued address can be used only once, thus
we can assume the given address is not committed or referenced anywhere else.
Once an issued address was used by LinkPhysicalAddress, we will delete the token ID from the list of valid tokens.

1. Issue a JWT token with the physical address
2. Save token ID in db
3. return token with physical address to the user 

#### LinkPhysicalAddress

1. Given physical address, and token
   1. Check token validity
   2. If token valid
      1. Remove token ID
      2. Add reference
      3. Add entry in staging area

#### CopyObject

The new flow ensures that we do not add a reference for a committed object after we removed all its references as part of 
the background delete job, and preventing the accidental deletion of committed objects.

1. While retry not exhausted
   1. Read reference key, if key found
      1. Read reference list for address, if list is empty
         1. Assume address is deleted - return error  
         (only way it is empty is if background job is currently working on this reference during the delete flow)
      2. Else - add branch staging token to reference list and perform `set if`
         1. If predicate failed - retry
   2. If key not found - assume address was committed - perform copy without adding a new reference

#### Commit

1. Perform commit flow in the same manner as today
2. On successful commit - create entry using `commit_id` and ordered list of tokens under the `committed` prefix 

#### Reset / Delete Branch

1. Perform reset / delete flow in the same manner as today
2. On successful execution - create entries for all staging tokens under the `deleted` prefix

### Background delete job

* Scan committed tokens
    * For each committed object - remove reference key
    * For each staging token in committed entry - "Move staging token to deleted"   
      All objects in committed staging tokens that were not actually committed are candidates for deletion, therefore
  we can either execute the deleted tokens logic, or create entries for these tokens under the deleted prefix (implementation detail)
    * Remove `staging/committed` key
    * Delete staging area
* Scan deleted tokens  
  * For each token - remove references for all objects on staging token
  * If it is the last reference for that object (list is empty after update) - perform hard-delete
  * Delete staging area

### Handling key override on same staging token - improvement

TBD

## 5. No Copies

The idea is to make sure we do not enable copies, when no two entries in stage will point to the same physical object we will enable delete of the physical address in the following:

- Upload - get previous entry and delete previous physical address unless staging token was updated
- Revert - branch / prefix / object, when entry is dropped we can delete the physical address
- Delete - repo/branch/object, delete the physical address after entry no longer exists

We should enable the following to enable the physical delete:

- Make sure we don't have the same physical address by sign links we return in our API. The logical path will be part of the signature that will be part of the physical address we generate. This will enable us to block and use of a physical address outside the repo/branch/entry.
- The S3 gateway `put` operation with the copy support will use the underlying adapter to copy the data. Will require from each adapter to support/emulate a 'copy' operation.
* (optional) Enable 'move' API as alternative to 'copy' we have seen that moving objects from one location to another by copy+delete of metadata will enable easy support for lakeFSFS move. This can be implemented by marking the entry on staging as 'locked'. During the move operation - lock+copy+delete+unlock operation. In case of any failure, as we don't have transactions, we may keep two entries, or keep locked object without ever delete its physical address.

TODO(Barak): list more cases and how it will be address in this solution.

### Object Locking

Add a new field in the staging area struct to indicate whether the entry is currently locked or not
```
type Value struct {
	Identity []byte
	Data     []byte
	Locked   bool
}
```
We can then use `SetIf` to ensure concurrently safe locking mechanism.
When deleting a staged entry (either by DropKey, ResetBranch, DeleteBranch) we will perform Hard Delete of object only in case 
the entry is not locked.

### Flows

#### Move operation
1. Get entry
2. "Lock" entry (`SetIf`)
3. Copy entry to new path on current staging token
4. Delete old entry
5. "Unlock" entry (new entry) 

#### Upload Object

1. Get entry
2. "Lock" entry if exists (override scenario) (`SetIf`)
3. Write blob
4. Add staged entry (or update)
5. Delete physical address if previously existed

#### Delete Object
1. Get entry
2. "Lock" entry (`SetIf`) to protect from delete - move race
3. Delete staging entry
4. Hard delete object

### Races and issues

#### 1. Commit - Move Race
1. Start Move:
   1. Get entry
   2. "Lock" entry
2. Start Commit:
   1. Change staging token
   2. write commit data (old entry path was written)
3. Move:
   1. Create new entry on new staging token
   2. Delete old entry
   3. "Unlock" physical address

Now we have an uncommitted entry pointing to a committed physical address. If we reset the branch we will delete
committed data!

#### 2. Delete - Move Race
1. Start Delete:
   1. Get entry
   2. Check if not "locked"
2. Start Move:
   1. Get entry
   2. "Lock" entry
   3. Copy entry to new path on current staging token
3. Delete:
   1. Hard Delete object from store

In this situation we have an uncommitted entry which points to an invalid physical address.
**Resolved by locking the entry on delete flow as well**

#### 3. Concurrent Move
Concurrent move should be blocked as it may cause multiple pointers for the same physical address.
The most straightforward way to do so is to rely on the absence of the lock.
The problem with this approach is that permanently locked addresses (which is a protection mechanism in case of catastrophic failures)
can never be moved -which leads us to the stale lock problem.

#### 4. Stale lock problem
We use the lock mechanism to protect from race scenarios on Upload (override), Move and Delete. As such, an entry with a stale
lock will prevent us from performing any of these operations.
We can overcome it with a time based lock - but this might present additional challenges to the proposed solution.