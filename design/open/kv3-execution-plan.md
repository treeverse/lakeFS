
# General
## Tables
Graveler uses 5 DB tables:
* `graveler_repositories`,`graveler_branches`,`graveler_commits`,`graveler_tags` - used by `pkg/graveler/ref`
* `graveler_staging_kv` - used by `pkg/graveler/staging`

`graveler_repositories`, `graveler_commits` and `graveler_tags` are immutable and does not update entries, and so should be straight forward to implement over our `KV` (`graveler_commits` actually performs update for commit generation, but this is only valid for `refs-restore` which can only take place on a `bare` repo and so it is of no concern)</br>
`graveler_branches` is currently being protected by a lock, during commit actions, and in order to support lockless commits with `KV`, the [following design](https://github.com/treeverse/lakeFS/blob/b3204edad00f88f8eb98524ad940fde96e02ab0a/design/open/metadata_kv/index.md#graveler-metadata---branches-and-staged-writes) is proposed. However, as a first step we can implement it using a naive table lock, just to get it going, or better, a `branch-key` based locking, to prevent staging during commits</br>
`graveler_staging_kv` creates entries based on the current branch `staging_token` and is protected from creating entries during a commit under the same lock. This should also be supported lockless-ly, as referred to by the above proposal, and can also be solved with naive locking, as a first step</br>
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

# Execution Plan
* Agree on keys schema (see here after)
* Supporting `KV` along side `SQL` (same as was done for previous modules)  
  * Step 1: Naive implementation with locking - `lakeFS` on KV can start with empty DB and run all graveler operations correctly, although performance may be degraded and exhaustive operations (concurrent commits and uploads, anything else?) should not be considered
  * Step 2: Lockless Commits - as described above
  * Step 3: Decide and implement a working solution for **Deleting a Repository**
  (Steps 2 and 3 are independent)
* Benchmarks for common operations - to be executed manually, on both `SQL` and `KV` to verify performance are satisfactory.
* Migration of existing `SQL` data to `KV`. Most of the tables are trivial to migrate. I believe `graveler_branches` migration should also be trivial, with no specific consideration for `sealed_tokens` but that needs to be verified
  * KV Migration Test is, obviously, included
* Add `graveler` migration to `Esti` Migration Tests
* Optional - Dump/Restore Refs tests for `SQL` and `KV`
* Optional - `pkg/disagnostics` support of KV. Should be trivial
