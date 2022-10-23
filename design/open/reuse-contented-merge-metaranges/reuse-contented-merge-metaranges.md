# Proposal: Re-use metaranges to retry contended merges

## Why

Large merges can be slow, and we are performing a lot of work to speed them
up.  This design is to speed up the case when multiple branches merge
concurrently into the same destination branch.  The goal is to speed up
concurrent merges on both non-KV and KV systems.

On **Postgres-based (non-KV) systems**, merges wait for each other on the
branch lock, slowing successive merges down.  What makes it _worse_ is that
the destination branch advances but the source does not.  So in each
successive concurrent merge the destination is even more divergent from the
base.

On **KV-based systems** (the near future!), there is no branch locker.
Concurrent merges race each other for completion.  The winning merge updates
the destination branch, and _all other merges lose and have to restart_.  So
on each concurrent merge, every successive merge attempt takes even longer.

This proposal cannot prevent concurrent merges from racing.  Instead it
makes retries considerably more efficient.  Any range that is affected by
only one concurrent merge will be written just _once_, no matter how many
times it is retried.  So additionally retries of the merge that is writing
that range will be able to skip _reading_ its two sources on every retry.
This should speed everything up!

### KV or not KV?

Importantly, this proposal is independent of whether or not we use KV.
_Before_ KV you can use it by removing the branch locker and making the
branch update operation conditional.  And then it **improves** concurrent
merge performance by allowing parallel execution of almost all of the
time-consuming tasks.  So it increases the amount of work performed but it
performs that work in parallel and reduces the latency of the slow
concurrent merges.

_After_ KV, not only does it restore performance to this improved case -- it
also removes much of the duplicate work that concurrent merges perform.

## How

The basic idea is to allow retries of a concurrent merge to re-use previous
work.  This is similar to the "overlapping `sealed_tokens`" full-commit
method of the [commit flow][commit_flow].  However here there are no staging
tokens; the previous work of a merge attempt is the metarange that it
generated.

![Branches A and B diverge from branch main.  Merge A and B into main concurrently.  A succeeds in merging and bumps HEAD of main.  B succeeds in merging and generates a metarange, but now it cannot bump HEAD of main.  Now try again to merge, but _use the newly-generated merged metarange instead of the HEAD of B.](./diagram.jpg)

There are two ways for a merge to fail:
* **Conflict:** The objects at some path on the merge base, source and
  destination are all different and no strategy was specified to resolve it.

  This failure is inherent and occurs regardless of additional concurrent
  merges.  It occurs while generating the merged metarange.  It cannot be
  fixed; correct behaviour is simply to report it to the user.
* **Losing a race:** No merge conflict occurred, but when trying to add the
  resulting merge commit to the destination we discover that the HEAD of the
  destination branch has changed.

  This failure is due to losing a race against a concurrent merge or commit.
  It must either be retried or abandoned (and, presumably, retried later).

Naturally only race failures are of interest.  When a merge fails because of
a merge, it must be retried.  However we can restart it using _the generated
merged metarange_ as source and the original destination as the base!  (See
"Correctness" below for why this yields the correct result.)

The performance gain occurs during the retry: We expect the new source to
share many ranges with the desired result and with the destination:

* A range that was the same in the base and the destination, but changed
  only between base and original source will still generally be the same in
  the base and destination.  And rewriting it from the new source yields the
  same range -- it will be reused in the result.

  **Result:** The range created in the first attempt will be recreated in
  the second attempt, and will not need to be uploaded to the backing object
  store.  (We might later decide to cache the merged range result in memory,
  which will let us skip even recreating that range!)
* A range that was the same in the base and the source, but changed only
  between base and original destination, will still be unchanged to the new
  source.  No range needs to be read or generated -- we did not slow this
  case down.
* A range that changed in all of base, old source, and old destination, will
  still need to be read and analyzed.  But the resulting range will be
  unchanged from the new source, so the merge is trivial: only changes from
  the old to new destination need be applied, and we expect many ranges not
  to have such changes.

One example where this assumption holds is when merging a branch that
branched out a while back.  The first merge will bring in everything that
has changed in other areas of the repository, so the second and following
merges should be much smaller.

Another example where we _expect_ this to work well is repeated multi-object
writes that are powered by merges: These will have frequent merges that
consist solely of a "directory", which is just consecutive or
nearly-consecutive files.  Regular writes use a new "directory" and will
quickly start using separate ranges.  Overwrites write to the same
"directory" each time, and will continue to race other overwites to that
directory -- but _not_ with concurrent writes to _other_ directories.  This
use-case is _currently in progress_ for Spark with the lakeFS
OutputCommitter, and we will likely perforn it for Iceberg and probably
Delta.

### Alternative

Rather than immediately retry a merge that loses a race, lakeFS could return
a failure with an additional "hints" field containing the resulting
metarange ID.  The client can then try again, supplying lakeFS with the same
hints.  This allows lakeFS to use the correct metarange in the retry.[^1]

This improves client control of retries.  In some cases it may cause retries
to be load-balanced onto a less busy lakeFS, which might improve fairness.

### Correctness

Why is it correct to use the new metarange?  It is sufficient to show that
merging from the new metarange as source will yield the same sequence of
objects as merging from the old metarange.  How this sequence is split into
ranges is important for efficiency, but yields an indistinguishable object
store.

In the sequel it helps to think of "deleted" objects that as having
particular contents.  The behaviour of deleted objects in a merge regarding
results and conflicts is exactly the same as of objects with some particular
unique contents.

| base | src | dst/base' | result/src' | dst' | final result | comment                        |
|------|-----|-----------|-------------|------|--------------|--------------------------------|
| A    | B   | A         | B           | A    | B            |                                |
| A    | A   | B         | B           | B    | B            |                                |
| A    | A   | A         | A           | B    | B            | (Only) dst changed during race |
| A    | B   | C         | conflict    | --   | conflict     | Conflicts never proceed        |
| A    | B   | A         | B           | C    | conflict     | Conflict with updated dst      |
| A    | A   | B         | B           | C    | C            |                                |


[commit_flow]:  ../../accepted/metadata_kv/index.md#committer-flow

[^1]: lakeFS could sign the metarange in some way, to prevent clients
    attempting to cheat.
