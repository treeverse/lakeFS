
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
As this design suggests, among other things, that each repository should be a represented in a dedicated partition and that all other repository related entities will exist under this partition, and since we would like to use the existence/inexistence of a repository entry, to identify a valid/deleted repository, respectively, the repository entry should also include a random `guid`, which, when concatenated with the repo name, will be the partition key for that repository entities. That way it will be easy to distinguish between a partition related to "aRepo" that was already deleted and a partition related to a new "aRepo" that currently exist, as their partition names differ by the `guid` (`aRepo-guid1` vs `eRepo-guid2`).
Having that said, it is still essential to verify and agree that each data access essentially implies fetch of the repository entry, in order to (a)verify its existence and (b) get the correct `guid` in order to build the partition key
### Branch
A branch is identified by its unique name **within** a repository, hance the combination `<REPO_NAME>`+`<BRANCH_NAME>` uniquely identifies a branch in the system. Since all repository entities (except for the aforementioned Repository entity itself) belong to the repository partition, the Branch will be identified by its unique name within that partition.
Other than that, a branch contains its current commit ID (type `CommitID`, a string) and its current staging token (type `StagingToken` a string). 
In order to support the `commit flow`, proposed in [lakeFS on KV design](https://github.com/treeverse/lakeFS/blob/b3204edad00f88f8eb98524ad940fde96e02ab0a/design/open/metadata_kv/index.md#committer-flow), a branch will also have the propsed `sealed_token` field - an array of `StagingToken`s, as specified in the design doc
### Tag
A tag is identified by its unique ID (type `TagID`, a string) within a repository, hence will be a unique tag identifier within the repository partition. It contains a commit ID
### Commit
A commit is identified by a unique commit ID within a repository. It contains the following commit data: version (type `CommitVersion`, an int), committer (string) and commit message (string), metarange ID (type `MetaRangeID`, a string), creation date (type `Time`), parent commits (type `CommitParents`, an array of `CommitID`), metadata (string => string mapping) and generation (int).
As a commit exists within a repository context, it can be identified by its unique ID (Commit ID) withing the repository partition
### Staged Object
Uniquely identified by a staging token and the object path within the branch. It contains the object data (type `DBEntry`) serialized as `protobuf`, and the checksum of this data. Looks like we can simply reuse the same `DBEntry` type as it is
In order to minimize bottlenecks in our implementation, each staging token will be represented by designated partition, where the `staging_token` acts as partition key, and the staged object paths, as keys within. Note the since `staging_token` is unique within a branch (moreover, it contains the repository name and the branch name as prefixes) it is an appropriate partition key

## Iterators
The following iterators are implemented and used by `pkg/graveler`:
  * `branch_iterator` - iterates branches by repository. Has an option to order by ID (trivial) or by CommitID.
  Iterating by commit Id will be implemented by fetching all the branches for the repository, and sorting by CommitID in memory. 
  * `commit_iterator` - iterates a commit ancestry by running BFS. Not an `iterator` in the `KV` sense. Should be supported as is with `KV` `Get`
  * `commit_ordered_iterator` - iterates commits of a repo, ordered by commit ID (regular `KV` `Scan` supports that). However - has an option to iterate over not-first-parent commits only (e.g. commits that are not a first parent of any other commit). Probably could (and should) be implemented by a specific `Next()` for that case
  * `repository_iterator`, `tag_iterator` - simple iterators ordered by ID. `KV` standard `Scan` should be sufficient
  * `staging/iterator` - iterates over a specific `staging_token` (as prefix) by order of key. Should be good with `KV` `Scan`

## Deleting a Repository
Unlike other entities' delete, which are standalone, deleting a repository results in deleting all branches, commits and tags correlated to that repository. While being protected by a transaction for `SQL` we need to handle failures for `KV`. One option that comes in mind is to flag the repository as `"being_deleted"` (maybe a `KV` entry for `being_deleted` repos, that can be scanned), clean everything, and if successful remove the flag. Another option is to remove the repository entry first, and deploy a background GC that scans for branches/commits/tags that are related to a non-exist repository and delete them. Other ideas?
**Update:** It looks like most (if not all) `graveler`'s operations, perform a repository existence check by issuing a call to `GetRepository`, which performs a DB access. If that is indeed the case, we can easily "mark" a repository as deleted by removing the repository entry from the common partition `graveler`, thus making it unreachable. We can later iterate all entries under this repository partition and delete them one by one, preceding the branch entry deletion by iteratively deleting all relevant staging partitions. Failures can be handled by a background garbage collector (which, BTW, can handle the initial deletion too), but that is only for space saving purposes and should have no effect on the deleted repository itself, once the repository entry is deleted. A point to consider - what happens if a repository entry is deleted, the data in the relevant repository partition fails to delete, and a new repository with the same name is created? I think this is solvable by adding a guid to the repository and use it as the partition key (as suggested in the `Repository` entity above)
Another option - As suggested by @itaiad200 - performing the deletion in a way that preserves consistency with each possible failure - e.g. staging partitions need to be deleted before the branch, as not to leave any hanging in case of failure, commits cannot be deleted before branches, as this may leave a branch pointing to a deleted commit, the default branch cannot be deleted until the repository is deleted, etc. Part of the discovery should define the correct order
Issue [#3570](https://github.com/treeverse/lakeFS/issues/3570) should handle this decision

## Testing
`pkg/graveler/ref` and `pkg/graveler/staging` currently has a **very high** test coverage. This should make it easier to trust the transition to `KV`. However, one notable aspect that is the absence of `benchmarks`. As `graveler` will introduce new algorithms to support lock-free commits, there is a risk of performance degradation which need to be identified
**Update:** A decision is to be made regarding the required benchmarks - which scenarios do we really want to test. One example is multiple parallel commits, that will definitely fail each other until all retries will succeed - while this is an exhaustive scenario, which stress the performance to an edge, it might not be as interesting as a "real life" scenario. The bottom line is we can predict that such a scenario will show performance degradation for our not-lock-free commit algorithm, but it is probably not interesting. Another example is a single long commit, with multiple staging operations taking place at the same time. This scenario is much more common in real life usage and a performance regression in this scenario is something we better discover sooner than later. This note was added to [#3571](https://github.com/treeverse/lakeFS/issues/3571)

## Dump/Restore Refs
Uses `graveler` ref-store for dumping a whole repository (branches, commits and tags) and restoring it to a bare repository. As there are no direct DB accesses, this should go seamlessly, but definitely need to be verified</br>
Moreover, as these are quite exhaustive operations, may be used as part of performance verification

## Suggested Key Schema
**Note:** This is an initial suggestion that seems to support the required iterators. It tries to encapsulate the immutable parts of identifying an object (e.g. a branch can be identified by its name, but not by a commit, as it (a) changes and (b) a commit can be shared by branches. a repo and a tag has one:many relation, so a tag key contains its repo etc.) It is totally open to discussion and improvements.
**Update:** We will use multiple partitions. One general named TBD (`graveler`) and another partition for each repository, named after that repository. The idea is to isolate the entire repository data, as either ways each operation is bounded to within a specified repository. Repositories themselves will be in the `graveler` repository, as to allow listing of all repositories. As no longer needed, the `<REPO_NAME>` can be removed from all keys under the `<REPO_NAME>` partition
* Partition `graveler`
  * Repository - `repos/<REPO_NAME>`

* Partition `<REPO_NAME>`
  * Branch     - `branches/<BRANCH_NAME>`
  * Commit     - `commits/<COMMIT_ID>`
  * Tag        - `tags/<TAG_ID>`
  * Staged Obj - `staged/<BRANCH_NAME>/<STAGING_TOKEN>/<OBJ_KEY>`

### Key Schema Support of Required Functionalities
**Note** Only covering non trivial functionalities
* `commit_iterator` - this is not an iterator in the `KV` sense, but rather an iteration algorithm where a commit is scanned and are parents are queued for later scan. There is no real `KV` iteration. Instead, a `Get` is used for each commit, by its ID, which is trivially supported by the above **Commit** key
* `commit_ordered_iterator` - scan by order of commitID, which is the natural behavior of our `KV iterator` implementation, and will be supported trivially. The option to scan only `not-first-parent` commits is TBD (still being discussed)
* `branch_iterator`, sorted by branch name - trivial. sorted by commitID - TBD (still being discussed)
* Repository Deletion - as suggested, maintaining all repository relevant entries under a unique partition will make it easy to delete as an entire partition. The repository key itself is outside of the partition (in a general common partition, to support listing) and will be deleted first. Once deleted, the entire partition will become unavailable, making the data state consistent either ways

## Update Commits, staging and lock-free
we will follow @arielshaqed suggestion to prevent commits overriding each other by using `KV`'s `SetIf` on the branch update, making sure no racing commit is already finished and being overridden. In case a commit fails to finish due to another commit "beating" it to the finish line - a retry should be attempted. This implementation will require to support multiple staging tokens, as proposed by [lakeFS on KV design proposal](https://github.com/treeverse/lakeFS/blob/b3204edad00f88f8eb98524ad940fde96e02ab0a/design/open/metadata_kv/index.md#committer-flow). Lock-free commits will be handled out of the scope of KV3 milestone

# Execution Plan
* Agree on keys schema (see here after) - [#3567](https://github.com/treeverse/lakeFS/issues/3567)
* Supporting `KV` along side `SQL` (same as was done for previous modules)
  * Step 1: Building a initial `KV graveler` and a `DB graveler`. `KV graveler` should delegate all calls to `DB gravler` (no `KV` usage at all for step) in order to allow partial `KV` support.
  [#3568](https://github.com/treeverse/lakeFS/issues/3568) was modified to reflect that, and should result with continuation tasks for the `KV` support steps within
  * Step 2: Basic implementation of `KV` usage within the `KV graveler` from the previous item. Tasks to be defined as part of [#3568](https://github.com/treeverse/lakeFS/issues/3568)
  * Step 3: Optimistic Lock Commits, as described above - a working commit algorithm that prevents commits from overriding each other, by using `KV`'s `SetIf` to set the branch's commit, and failing (and retrying) to commit if `SetIf` fails, as described above. [#3569](https://github.com/treeverse/lakeFS/issues/3569)
  * Step 4: Decide and implement a working solution for **Deleting a Repository** - [#3570](https://github.com/treeverse/lakeFS/issues/3570)
  (Steps 3 and 4 are independent)
* Benchmarks for common operations - to be executed manually, on both `SQL` and `KV` to verify performance are satisfactory - [#3571](https://github.com/treeverse/lakeFS/issues/3571)
* Migration of existing `SQL` data to `KV`. Most of the tables are trivial to migrate. I believe `graveler_branches` migration should also be trivial, with no specific consideration for `sealed_tokens` but that needs to be verified - [#3572](https://github.com/treeverse/lakeFS/issues/3572)
  * KV Migration Test is, obviously, included
* Add `graveler` migration to `Esti` Migration Tests - [#3573](https://github.com/treeverse/lakeFS/issues/3573)
* Optional - Dump/Restore Refs tests for `SQL` and `KV` - [#3574](https://github.com/treeverse/lakeFS/issues/3574)
* Optional - `pkg/disagnostics` support of KV. Should be trivial - [#3575](https://github.com/treeverse/lakeFS/issues/3575)
