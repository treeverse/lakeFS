// TODO(barak): documentation of the new data model
/*

   MVCC using Postgres

   The important tables are

   branches:
       repository_id integer
       id integer // number of branch
       name character varying(64)
       next_commit integer // commit/generation number. incremented with each commit on the branch.
                           // when an entry is created, it gets this number as its lower commit range number.
                           // when an entry is deleted - this number it the higher commit range number.

   lineage:
   a "slave" table of branches. describe the list of branches from which this branch "inherits" it's content.
     branch_id integer // master branch ID
     ancestor_branch integer // branch from which this branch, or its ancestor, was "branched"
     precedence integer // "distance" of the ancestor from the branch in the "branching" order
     effective_commit integer NOT NULL, // the commit number that was effective when the branch was created
     branch_commits int4range DEFAULT '[1,)'::int4range, // enables querying on previous commits

   entries:
       branch_id        integer
       key              varchar // path in lakeFS
       commits          int4range  // low bound (inclusive) - commit number when this entry was created
                                   // top bound (exclusive) - commit number when this entry was deleted/overridden
       physical_address varchar(64) // name in underlying block storage

    main views:

    lineage_v - adds the branch content itself to the lineage table - uses union
    SELECT lineage.branch_id,
       lineage.precedence,
       lineage.ancestor_branch,
       lineage.effective_commit,
       lineage.branch_commits
      FROM lineage
   UNION ALL
    SELECT branches.id AS branch_id,
       0 AS precedence,
       branches.id AS ancestor_branch,
       branches.next_commit AS effective_commit,
       '[1,)'::int4range AS branch_commits
      FROM branches;

   entries_lineage_v - gets all content for a branch.
   to get the actual current content, the query should be:
   select * from entries_lineage_v where displayed_branch= X and rank=1 and upper_inf(commits) // upper unbounded (infinity)

   SELECT l.branch_id AS displayed_branch,
       e.branch_id AS source_branch,
       e.key,
       e.commits,
       e.physical_address,
       e.creation_date,
       e.size,
       e.checksum,
       l.precedence,
       rank() OVER (PARTITION BY l.branch_id, e.key ORDER BY l.precedence, (lower(e.commits)) DESC) AS rank
      FROM entries e
        JOIN lineage_v l ON l.ancestor_branch = e.branch_id
     WHERE lower(e.commits) <= l.effective_commit;

*/
package catalog
