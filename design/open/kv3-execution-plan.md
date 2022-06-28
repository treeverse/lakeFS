
# General
## Tables
Graveler uses 5 DB tables:
* `graveler_repositories`,`graveler_branches`,`graveler_commits`,`graveler_tags` - used by `pkg/graveler/ref`
* `graveler_staging_kv` - used by `pkg/graveler/staging`

`graveler_repositories`, `graveler_commits` and `graveler_tags` are immutable and does not update entries, and so should be straight forward to implement over our `KV` (`graveler_commits` actually performs update for commit generation, but this is only valid for `refs-restore` which can only take place on a `bare` repo and so it is of no concern)</br>
`graveler_branches` is currently being protected by a lock, during commit actions, and in order to support lock-free commits with `KV`, the [following design](https://github.com/treeverse/lakeFS/blob/b3204edad00f88f8eb98524ad940fde96e02ab0a/design/open/metadata_kv/index.md#graveler-metadata---branches-and-staged-writes) is proposed. However, as a first step we can implement it using a naive table lock, just to get it going, or better, a `branch-key` based locking, to prevent staging during commits</br>
`graveler_staging_kv` creates entries based on the current branch `staging_token` and is protected from creating entries during a commit under the same lock. This should also be supported lock-free-ly, as referred to by the above proposal, and can also be solved with naive locking, as a first step</br>
Note: All the above tables (along with the late `auth_users`) are also used by `pkg/diagnostics` for diagnostics collection (RO). 

## Iterators
The following iterators are implemented and used by `pkg/graveler`:
  * `branch_iterator` - iterates branches by repository. Has an option to order by ID (trivial) or by CommitID (Used by GC. How do we support that?)
  * `commit_iterator` - iterates a commit ancestry by running BFS. Not an `iterator` in the `KV` sense. Should be supported as is with `KV` `Get`
  * `commit_ordered_iterator` - iterates commits of a repo, ordered by commit ID (regular `KV` `Scan` supports that). However - has an option to iterate over not-first-parent commits only (e.g. commits that are not a first parent of any other commit). Probably could (and should) be implemented by a specific `Next()` for that case
  * `repository_iterator`, `tag_iterator` - simple iterators ordered by ID. `KV` standard `Scan` should be sufficient
  * `staging/iterator` - iterates over a specific `staging_token` (as prefix) by order of key. Should be good with `KV` `Scan`

## Deleting a Repository
Unlike other entities' delete, which are standalone, deleting a repository results in deleting all branches, commits and tags correlated to that repository. While being protected by a transaction for `SQL` we need to handle failures for `KV`. One option that comes in mind is to flag the repository as `"being_deleted"` (maybe a `KV` entry for `being_deleted` repos, that can be scanned), clean everything, and if successful remove the flag. Another option is to remove the repository entry first, and deploy a background GC that scans for branches/commits/tags that are related to a non-exist repository and delete them. Other ideas?

## Testing
`pkg/graveler/ref` and `pkg/graveler/staging` currently has a **very high** test coverage. This should make it easier to trust the transition to `KV`. However, one notable aspect that is the absence of `benchmarks`. As `graveler` will introduce new algorithms to support lockless commits, there is a risk of performance degradation which need to be identified

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

# Execution Plan
* Agree on keys schema (see here after) - [#3567](https://github.com/treeverse/lakeFS/issues/3567)
* Supporting `KV` along side `SQL` (same as was done for previous modules)  
  * Step 1: Naive implementation with locking - `lakeFS` on KV can start with empty DB and run all graveler operations correctly, although performance may be degraded and exhaustive operations (concurrent commits and uploads, anything else?) should not be considered - [#3568](https://github.com/treeverse/lakeFS/issues/3568)
  * Step 2: Lock-free Commits - as described above - [#3569](https://github.com/treeverse/lakeFS/issues/3569)
  * Step 3: Decide and implement a working solution for **Deleting a Repository** - [#3570](https://github.com/treeverse/lakeFS/issues/3570)
  (Steps 2 and 3 are independent)
* Benchmarks for common operations - to be executed manually, on both `SQL` and `KV` to verify performance are satisfactory - [#3571](https://github.com/treeverse/lakeFS/issues/3571)
* Migration of existing `SQL` data to `KV`. Most of the tables are trivial to migrate. I believe `graveler_branches` migration should also be trivial, with no specific consideration for `sealed_tokens` but that needs to be verified - [#3572](https://github.com/treeverse/lakeFS/issues/3572)
  * KV Migration Test is, obviously, included
* Add `graveler` migration to `Esti` Migration Tests - [#3573](https://github.com/treeverse/lakeFS/issues/3573)
* Optional - Dump/Restore Refs tests for `SQL` and `KV` - [#3574](https://github.com/treeverse/lakeFS/issues/3574)
* Optional - `pkg/disagnostics` support of KV. Should be trivial - [#3575](https://github.com/treeverse/lakeFS/issues/3575)

