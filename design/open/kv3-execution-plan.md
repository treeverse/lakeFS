
# General
## Tables
Graveler uses 5 DB tables:
* `graveler_repositories`,`graveler_branches`,`graveler_commits`,`graveler_tags` - used by `pkg/graveler/ref`
* `graveler_staging_kv` - used by `pkg/graveler/staging`

`graveler_repositories`, `graveler_commits` and `graveler_tags` are immutable and does not update entries, and so should be straight forward to implement over our `KV` (`graveler_commits` actually performs update for commit generation, but this is only valid for `refs-restore` which can only take place on a `bare` repo and so it is of no concern)</br>
`graveler_branches` is currently being protected by a lock, during commit actions, and in order to support lock-free commits with `KV`, the [following design](https://github.com/treeverse/lakeFS/blob/b3204edad00f88f8eb98524ad940fde96e02ab0a/design/open/metadata_kv/index.md#graveler-metadata---branches-and-staged-writes) is proposed. However, as a first step we can implement it using a naive table lock, just to get it going, or better, a `branch-key` based locking, to prevent staging during commits</br>
`graveler_staging_kv` creates entries based on the current branch `staging_token` and is protected from creating entries during a commit under the same lock. This should also be supported lock-free-ly, as referred to by the above proposal, and can also be solved with naive locking, as a first step</br>
Note: All the above tables (along with the late `auth_users`) are also used by `pkg/diagnostics` for diagnostics collection (RO).

## Entities
### Repository
A repository is identified by a unique name (type `RepositoryID`, a `string`) and contains its storage name space (type `StorageNamespace`, a string), default branch ID (type `BranchID`, a string) and its creation date (type `Time`).
Repositories are iterable, and so all repositories should exist in a common partition (`graveler`)
As this design suggests, among other things, that each repository should be a represented in a dedicated partition and that all other repository related entities will exist under this partition, and since we would like to use the existence/inexistence of a repository entry, to identify a valid/deleted repository, respectively, the repository entry should also include a random `UniqueID`, which, along with the repo name, will be the partition key for that repository entities. That way it will be easy to distinguish between a partition related to "aRepo" that was already deleted and a partition related to a new "aRepo" that currently exist, as their partition names differ by the `UniqueID` (`aRepo/UniqueID1` vs `eRepo/UniqueID2`).
Having that said, it is still essential to verify and agree that each data access essentially implies fetch of the repository entry, in order to (a)verify its existence and (b) get the correct `UniqueID` in order to build the partition key
```go
type RepositoryToken string

type Repository struct { 
  StorageNamespace  StorageNamespace  `db:"storage_namespace" json:"storage_namespace"` 
  CreationDate      time.Time         `db:"creation_date" json:"creation_date"`
  DefaultBranchID   BranchID          `db:"default_branch" json:"default_branch"`
  RepositoryToken   RepositoryToken   `json:"repository_token"`     // new field
}
```
### Branch
A branch is identified by its unique name **within** a repository, hance the combination `<REPO_NAME>`+`<BRANCH_NAME>` uniquely identifies a branch in the system. Since all repository entities (except for the aforementioned Repository entity itself) belong to the repository partition, the Branch will be identified by its unique name within that partition.
Other than that, a branch contains its current commit ID (type `CommitID`, a string) and its current staging token (type `StagingToken` a string). 
In order to support the `commit flow`, proposed in [lakeFS on KV design](https://github.com/treeverse/lakeFS/blob/b3204edad00f88f8eb98524ad940fde96e02ab0a/design/open/metadata_kv/index.md#committer-flow), a branch will also have the propsed `sealed_tokens` field - an array of `StagingToken`s, as specified in the design doc
```go
type Branch struct {
  CommitID      CommitID        `json:"commit_id"`
  StagingToken  StagingToken    `json:"staging_token"`
  SealedTokens  []StagingToken  `json:"sealed_tokens"`        // new field
}
```
### Tag
A tag is identified by its unique ID (type `TagID`, a string) within a repository, hence will be a unique tag identifier within the repository partition. It contains a commit ID
```go
type TagRecord struct {
  TagID    TagID    `json:"tag_id"`
  CommitID CommitID `json:"commit_id"`
}
```
### Commit
A commit is identified by a unique commit ID within a repository. It contains the following commit data: version (type `CommitVersion`, an int), committer (string) and commit message (string), metarange ID (type `MetaRangeID`, a string), creation date (type `Time`), parent commits (type `CommitParents`, an array of `CommitID`), metadata (string => string mapping) and generation (int).
As a commit exists within a repository context, it can be identified by its unique ID (Commit ID) withing the repository partition
```go
type Commit struct {
  Version      CommitVersion `db:"version" json:"version"`
  Committer    string        `db:"committer" json:"committer"`
  Message      string        `db:"message" json:"message"`
  MetaRangeID  MetaRangeID   `db:"meta_range_id" json:"meta_range_id"`
  CreationDate time.Time     `db:"creation_date" json:"creation_date"`
  Parents      CommitParents `db:"parents" json:"parents"`
  Metadata     Metadata      `db:"metadata" json:"metadata"`
  Generation   int           `db:"generation" json:"generation"`
}
```
### Staged Object
Uniquely identified by a staging token and the object path within the branch. It contains the object data (type `DBEntry`) serialized as `protobuf`, and the checksum of this data. Looks like we can simply reuse the same `DBEntry` type as it is
In order to minimize bottlenecks in our implementation, each staging token will be represented by designated partition, where the `staging_token` acts as partition key, and the staged object paths, as keys within. Note the since `staging_token` is unique within a branch (moreover, it contains the repository name and the branch name as prefixes) it is an appropriate partition key
```go
type Value struct {
  Identity []byte `db:"identity" json:"identity`
  Data     []byte `db:"data" json:"data`
}
```

## Iterators
The following iterators are implemented and used by `pkg/graveler`:
  * `branch_iterator` - iterates branches by repository. Has an option to order by ID (trivial) or by CommitID.
  Iterating by commit Id will be implemented by fetching all the branches for the repository, and sorting by CommitID in memory. 
  * `commit_iterator` - iterates a commit ancestry by running BFS. Not an `iterator` in the `KV` sense. Should be supported as is with `KV` `Get`
  * `commit_ordered_iterator` - iterates commits of a repo, ordered by commit ID (regular `KV` `Scan` supports that). However - has an option to iterate over not-first-parent commits only (e.g. commits that are not a first parent of any other commit). Probably could (and should) be implemented by a specific `Next()` for that case
  * `repository_iterator`, `tag_iterator` - simple iterators ordered by ID. `KV` standard `Scan` should be sufficient
  * `staging/iterator` - iterates over a specific `staging_token` (as prefix) by order of key. Should be good with `KV` `Scan`

## Deleting a Repository
Unlike other entities' delete, which are standalone, deleting a repository results in deleting all branches, commits and tags correlated to that repository. While being protected by a transaction for `SQL` we need to handle failures for `KV`. 
Most (if not all) `graveler`'s operations, perform a repository existence check by issuing a call to `GetRepository`, which performs a DB access. This call will also be used to get the repository `UniqueID` and create the partition key to access this repository related entities. We will also use it as a validation that the repository indeed exists. In order to delete a repository, first create the partition key using the aforementioned method, then delete the repository entry from the `graveler` partition, thus making it inaccessible and so not exist. The next step is to use the created partition key to delete all entries relevant to that repository. Note that a failure before the repository entry is deleted, leaves the repository in place and accessible, and any failure after the repository entry was deleted, leaves both the repository partition and the related staging partitions unreachable for graveler. Also note that creating a new repository with the same name as the deleted one (given that at least the repository entry was removed successfully) will generate a new `UniqueID`, still leaving any leftovers from a previous failed delete unreachable. Either ways, the DB remains consistent.
We should implement a periodic background garbage collector to clean partitions that were failed to delete. In order to do so, we will need to maintain a list of partitions to delete - can be done by starting a repository deletion by adding its `<REPO_NAME>-<UNIQUE_ID>` partition to a `to-delete` list in the DB. As this is for space efficiency and tidyness purposes, and should not affect the correct operation of `graveler`, will be done out of KV3 scope. [#3590](https://github.com/treeverse/lakeFS/issues/3590) to handel this

## Testing
`pkg/graveler/ref` and `pkg/graveler/staging` currently has a **very high** test coverage. This should make it easier to trust the transition to `KV`. However, one notable aspect that is the absence of `benchmarks`. As `graveler` will introduce new algorithms to support lock-free commits, there is a risk of performance degradation which need to be identified
A decision is to be made regarding the required benchmarks - which scenarios do we really want to test. One example is multiple parallel commits, that will most definitely fail each other until all retries will succeed - while this is an exhaustive scenario, which stress the performance to an edge, it might not be as interesting as a "real life" scenario. The bottom line is we can predict that such a scenario will show performance degradation for our not-lock-free commit algorithm, but it is probably not interesting. On the opposite, a good example is a single long commit, with multiple staging operations taking place at the same time. This scenario is much more common in real life usage and a performance regression (`KV` vs `SQL`) in this scenario is something we better discover sooner than later. Part of [#3571](https://github.com/treeverse/lakeFS/issues/3571) is to define which benchmarks we want to test, but Long-Commit-Multiple-Staging scenario above is a good starting point and might prove to be sufficient

## Dump/Restore Refs
Uses `graveler` ref-store for dumping a whole repository (branches, commits and tags) and restoring it to a bare repository. As there are no direct DB accesses, this should go seamlessly, but definitely need to be verified</br>
Moreover, as these are quite exhaustive operations, may be used as part of performance verification

## Suggested Key Schema
We will use 3 types of partitions. One general partition, named `graveler`, that will hold all the repositories entries. That will allow to access a repository, without being familiar with any partition, other than the main `graveler` partition. It will also easily support the need to iterate over the repositories. The existence/inexistence of a repository entry in this partition, solely determines if a repository exists or not.
Each repository will have a designated partition with all its relevant entities (commits, branches and tags - but not staged objects). The partition name will be the repository named, with a random `UniqueID` that will be generated upon the repository creation. The addition of this `UniqueID` will allow to distinguish between different partitions that were created for **different** repositories with the same name (possible in case a deletion of the repository failed when deleting the partition. See **Deleting a Repository** above. In order to get the partition key, each operation should perform a `GetRepository` the get the correct `UniqueID`, This will also ensure the repository is not deleted.
Each staging token will have a designated partition, named after the token value. This will provide load balancing between different staging tokens for staging and commits. The staging token contains the repo name, the branch name and a `uuid` making it a good partition name in both aspects of uniqueness, and ease to identify with a branch
* Partition `graveler`
  * Repository - `repos/<REPO_NAME>`

* Partition `<REPO_NAME>-<UNIQUE_ID>`
  * Branch     - `branches/<BRANCH_NAME>`
  * Commit     - `commits/<COMMIT_ID>`
  * Tag        - `tags/<TAG_ID>`

* Partition `<STAGING_TOKEN>`     // Note: the token contains the repo name and branch name
  * Staged Obj - `key/<OBJ_KEY>`

### Key Schema Support of Required Functionalities
**Note** Only covering non trivial functionalities
* `commit_iterator` - this is not an iterator in the `KV` sense, but rather an iteration algorithm where a commit is scanned and are parents are queued for later scan. There is no real `KV` iteration. Instead, a `Get` is used for each commit, by its ID, which is trivially supported by the above **Commit** key
* `commit_ordered_iterator` - scan by order of commitID, which is the natural behavior of our `KV iterator` implementation, and will be supported trivially. The option to scan only `not-first-parent` commits is TBD (still being discussed)
* `branch_iterator`, sorted by branch name - trivial. sorted by commitID - TBD (still being discussed)
* Repository Deletion - as suggested, maintaining all repository relevant entries under a unique partition will make it easy to delete as an entire partition. The repository key itself is outside of the partition (in a general common partition, to support listing) and will be deleted first. Once deleted, the entire partition will become unavailable, making the data state consistent either ways

## Update Commits, staging and lock-free
For the scope of KV3 commit implementation, we will prevent commits overriding each other by using `KV`'s `SetIf` on the branch update, making sure no racing commit is already finished and being overridden. In case a commit fails to finish due to another commit "beating" it to the finish line - a retry should be attempted. This implementation will require to support multiple staging tokens, as proposed by [lakeFS on KV design proposal](https://github.com/treeverse/lakeFS/blob/b3204edad00f88f8eb98524ad940fde96e02ab0a/design/open/metadata_kv/index.md#committer-flow).
Lock-free commits, as described in [Change KV storage proposal to guarantee progress](https://github.com/treeverse/lakeFS/pull/3091), will be handled out of the scope of KV3 milestone

# Execution Plan
* Agree on keys schema (see here after) - [#3567](https://github.com/treeverse/lakeFS/issues/3567)
* Supporting `KV` along side `SQL` (same as was done for previous modules)
  * Step 1: Building a initial `KV graveler` and a `DB graveler`. `KV graveler` should delegate all calls to `DB graveler` (no `KV` usage at all for this step) in order to allow `KV` implementation by the block - [#3568](https://github.com/treeverse/lakeFS/issues/3568)
  * Step 2: Basic implementation of `KV` usage within the `KV graveler` from the previous item:
    * Repository support (except for deletion) - [#3591](https://github.com/treeverse/lakeFS/issues/3591)
    * Branch support - [#3592](https://github.com/treeverse/lakeFS/issues/3592)
    * Tags support - [#3593](https://github.com/treeverse/lakeFS/issues/3593)
    * Commits support (not including optimistic locking commit flow) - [#3594](https://github.com/treeverse/lakeFS/issues/3594)
    * Staging support - [#3595](https://github.com/treeverse/lakeFS/issues/3595)
  * Step 3: Optimistic Lock Commits, as described above - a working commit algorithm that prevents commits from overriding each other, by using `KV`'s `SetIf` to set the branch's commit, and failing (and retrying) to commit if `SetIf` fails, as described above. [#3569](https://github.com/treeverse/lakeFS/issues/3569)
  * Step 4: Decide and implement a working solution for **Deleting a Repository** - [#3570](https://github.com/treeverse/lakeFS/issues/3570)
  (Steps 3 and 4 are independent)
* Benchmarks for common operations - to be executed manually, on both `SQL` and `KV` to verify performance are satisfactory - [#3571](https://github.com/treeverse/lakeFS/issues/3571)
* Migration of existing `SQL` data to `KV`. Most of the tables are trivial to migrate. I believe `graveler_branches` migration should also be trivial, with no specific consideration for `sealed_tokens` but that needs to be verified - [#3572](https://github.com/treeverse/lakeFS/issues/3572)
  * KV Migration Test is, obviously, included
* Add `graveler` migration to `Esti` Migration Tests - [#3573](https://github.com/treeverse/lakeFS/issues/3573)
* Optional - Dump/Restore Refs tests for `SQL` and `KV` - [#3574](https://github.com/treeverse/lakeFS/issues/3574)
* Optional - `pkg/diagnostics` support of KV. Should be trivial - [#3575](https://github.com/treeverse/lakeFS/issues/3575)
