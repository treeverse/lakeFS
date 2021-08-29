
### Spark Job proposal

The user can run a spark job that is in charge of enforcing the lifecycle policy.

The spark job will delete all the data (addresses) of objects matching the
lifecycle rules except for data that is referenced by other objects (objects not
matching the lifecycle rule). In order to find the matching objects efficiently
(without going over the history of all commits every time) the spark job will
conclude the Date lifecycle job ran last time. Using that Date, the job will
find only commits that might have objects matching the lifecycle policy and
didn't match in the last run.

   

This job contains five main parts:
1. Extract data from policy
2. Find potential commits, and commit branches
3. Find addresses that should be removed by lifecycle policy
4. Find deduplications
5. Delete all addresses found by lifecycle policy that have no deduplication

each step will use the information generated in the previous step 

#### Prepare data using rules
 
The first step in the spark job would be to transform the current policy into `policy table`.
The `policy table` will hold the policy for all run ids.
The main reason for this is that the policy could constantly be changed, we need some way to remember what was the policy for each rule.
Once we already have it we would use it to generate information needed for the next step to find the potential commits
and will be used along the steps for validating if the policy is matched.

The `policy table` will contain as follows:
- run_id:  a unique id generated for this run
- rule_id: an id provided by the user - has no influence but, an ID provided by the user.
- prefix:  the prefix defined in the rule
- branch:  
- date_to_be_deleted: the date all the objects containing the prefix and exist in the branch should be deleted 

For example the policy
```json
{
   "rule1" : {
   "prefix": "foo/bar",
   "days": 10,
   "enabled": true,
   "branch_days": {
   "b1": 5,
   "b2": 8 
    }
   },
   "rule2" : {
   "prefix": "foo/zoo",
   "enabled": true,
   "branch_days": {
      "b1": 5
   }
   }
}
```


assuming today is 20/01/1998
would translate into

| run_id | rule_id | prefix   | branch | date_to_be_deleted   |
|--------|---------|----------|--------|----------------------|
| 2      | rule1   | foo/bar  |        |    10/01/1998        |
| 2      | rule1   | foo/bar  |   b1   |    15/01/1998        |
| 2      | rule1   | foo/bar  |   b2   |    18/01/1998        |
| 2      | rule2   | foo/zoo  |   b1   |    15/01/1998        |

Assuming last run was on 19/01/1998 and contained only rule one we would now have

| run_id | rule_id | prefix   | branch | date_to_be_deleted   |
|--------|---------|----------|--------|----------------------|
| 2      | rule1   | foo/bar  |        |    10/01/1998        |
| 2      | rule1   | foo/bar  |   b1   |    15/01/1998        |
| 2      | rule1   | foo/bar  |   b2   |    18/01/1998        |
| 2      | rule2   | foo/zoo  |   b1   |    15/01/1998        |
| 1      | rule1   | foo/bar  |        |    09/01/1998        | 
| 1      | rule1   | foo/bar  |   b1   |    14/01/1998        | 
| 1      | rule1   | foo/bar  |   b2   |    17/01/1998        | 

Using the `policy table` with the information about the run_id that deleted objects (i.e The `policy table` doesn't mean the data was deleted, could be a dry run or a run that failed for some reason) 
we could generate the data relevant for preparing the commits

In the case provided above ( assuming run_id 1 finished successfully and deleted all files)
We will generate the following data
`current_rule` table:

| prefix   | branch | date_to_be_deleted | last_deleted  |
|----------|--------|--------------------|---------------|
|  foo/bar |        |    10/01/1998      |   09/01/1998  |
|  foo/bar |   b1   |    15/01/1998      |   14/01/1998  |
|  foo/bar |   b2   |    18/01/1998      |   17/01/1998  |
|  foo/zoo |   b1   |    15/01/1998      |               |
 
#### Finding potential commits, and commit branches

No harm would be done by running lifecycle on all commits every time.
We would basically try to remove the same addresses again and again with each run adding some new addresses.
The main issue with running on all commits is adding unnecessary work that would get extremely large over time.
For that reason, we would like to reduce the number of commits based on previous lifecycle runs (that finished and deleted successfully).

##### Definitions

- potential commits - commits that may contain addresses that should be removed as part of the lifecycle policy and weren't removed as part of the last lifecycle run.
- commit branches - all the branches that have access to the commit

#### algorithm

From the `current_rule` table we extract for each branch the minimum `last_deleted`

| branch | last_deleted  |
|--------|---------------|
|        |   09/01/1998  |
|   b1   |               |
|   b2   |   17/01/1998  |

To enrich the example lets add another branch b3 with last_deleted 05/01/1998

| branch | last_deleted  |
|--------|---------------|
|        |   09/01/1998  |
|   b1   |               |
|   b2   |   17/01/1998  |
|   b3   |   05/01/1998  |

* Empty branch == All
* Empty last_deleted == All (the beginning of the branch)
We pass that data to lakeFS

In lakeFS we need to find all the commits matching all Dates
That is 
- all commits on b1
- all commits on b2 after 17/01/1998 -> (we will take because it's part of all )
- all commits on b3 after 05/01/1998
- all commits on any branch after 09/01/1998

We would like to take all commits from the intersection of these rules
Some examples
- commit is only accessible from the head of b1 - all commits
- commit is only accessible from the head of b3 - all commits after 05/01/1998
- commit is only accessible from the head of b1 or b3 - commits after 05/01/1998
- commit is only accessible from the head of b1 and b3 and b2 - commits after 09/01/1998

> Note! This could be improved by doing these intersections for each rule independently, It could potentially reduce the number of commits, I believe it is not worth the complication for now.   

lakeFS will receive this data and return/write all the potential commits (with commit branches) for this run.

#### Find addresses that should be removed by lifecycle policy

From the previous stage we got a table of the following form commit_id | branches
Using that we:
1. get rang_id from commit_id
2. group by range_id selecting the union of branches
3. get (key, address) from ranges
4. group by (keys, address) selecting the union of branches
5. select all addresses matching the policy rules (which did not match policy rule last run)

#### Find deduplications

##### Definitions
deduplications - for a given path P with address A, a deduplication would be a different path P` referencing the same address A

In the case of lifecycle, before removing an address, we would first need to check that it has no deduplications that should not be removed as part of lifecycle.
For example:
Assuming we have rule for prefix `foo/bar` and for prefix `foo/tar`
- `foo/bar/a 1`
- `foo/bar/b 4`
- `foo/tar/a 1`
- `foo/tar/b 2`
- `foo/other/c 2`

In the given case:
- 4 could be removed because it has no deduplications
- 1 has deduplications but it could be removed due to the fact that all deduplications should be removed by lifecycle
- 2 can't be removed because it has a deduplication that should be accessible after lifecycle job is done (foo/other/c)

> **_NOTE:_**  In case an address wasn't removed due to deduplication it will not be removed in the following run even if the deduplication was deleted. i.e regarding the previous example address 2 wasn't removed due to deduplication, in case foo/other/c was somehow deleted, when running the job again, we might assume address 2 should now be removed because it matches lifecycle policy and has no deduplication. We will not support that case

##### Algorithm
1. For every address check if it is a deduplication:
      - If it appears in commits that are not in the potential commits -> it's a deduplication
      - If it appears in a commit that is a `potential commit` in a path that does not match any rule -> it's a deduplication
2. filter the deduplications out from the addresses to be removed
3. write the result as a table ( run_id, address, paths, matched_rules) to  `_lakefs/retention/lc/addresses`  partitioned by date


#### Delete addresses
The last step would be to delete all the addresses that should be removed. this step was already implemented as part of GC.
After deleting we will write some kind of success/status file to `_laefs/retention/lc/stats/[date].csv`

> **_NOTE:_** This algorithm could be optimized when running together with garbage collection by working with only `active commits` (and not all commits, both in finding potential commits and in finding deduplications)