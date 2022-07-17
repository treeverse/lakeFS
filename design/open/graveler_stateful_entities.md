# Graveler Stateful Entities

## Abstract

Transitioning from PostgresSQL based metadata management DB to [generic KV Store design](https://github.com/treeverse/lakeFS/blob/master/design/accepted/metadata_kv/index.md) 
introduced challenges in regard to atomicity and error handling.
Previously, we relied on the transactional property of PSQL, to perform all-or-nothing operations to perform flows which requires 
the modification of multiple entities atomically. Together with the built-in lock mechanism we were able to perform atomic, and concurrently safe operations.
In the KV design, we rely on a very narrow interface, which does not support transactions. Additionally, the KV design does not rely on locks for operations.
This complicates atomicity, error handling and rollback operations.
This design can also pave to way for future support of [long-running hooks](https://github.com/treeverse/lakeFS/issues/3020), which require entities to be in an initial in-accessible/operable state,
until the pre-action hooks are completed, and in case of failure deleted without implications on the end user.
Proceeding the [stateful design for repositories](https://github.com/treeverse/lakeFS/pull/3661) , this document suggests adding states for all (most) of the graveler entities to 
support both goals.

## Supported Entities

All entities which currently support pre/post actions on creation and deletion must support states. These include:
* Branches
* Commits
* Tags

All entities which require support of transactional / concurrent / rollback operations:
* Repositories
* Branches

## Supported States

### `INIT`

The entity was created, in DB but not accessible to the user. Will move to `ACTIVE` state until initialization is completed. 

### `ACTIVE`

The entity is in a valid state and accessible to the user

### `FAILED` / `INACTIVE`

Terminal state - Inaccessible to the user, and a candidate for deletion

## Error Handling

Since error handling is defined the specific use case it will not be discussed under the current document, but generally 
we will have 2 types of error handling:
1. Setting entity to `FAILED` state for future deletion
2. Performing a rollback operation for all the actions 
3. Retry mechanism

#### Transaction Examples:

_Delete Branch_  
* Set Branch state to `FAILED`. 
* Perform staging token and sealed token deletion. 
  * Once all tokens are deleted, safely delete the branch. 
  * If token deletion fails, branch can remain marked `FAILED` state, for future deletion

_Create Repository_  
* Create repository in `INIT` state. 
* Created default branch and initial commit. 
  * If successful move repository state to `ACTIVE`
  * If default branch creation failed - move repository to `FAILED` state (and possibly the commit), to be deleted in a future time.

#### Long Running Hooks Example:

_Create Tag_  
* Create Tag in `INIT` state
* Run pre-create actions 
  * On pre-create actions successful move tag to `ACTIVE` state
  * In case of failure / timeout - delete tag.
