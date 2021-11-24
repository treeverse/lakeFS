# Next Generation Metastore - Project


## Milestone 1 - lakeFS Metastore Proposal

Items to discover listed with hierarchy as dependency:

- Metastore per repo, or n:m?
- Map and select one of the two ways to implement the metastore. Understand each suggested design, list pros and cons.
  - Metastore as additional functionality that lakeFS provides (described in the doc)
  - Metastore implemented above lakeFS (**TDB** describe as alternative in the doc)
- Data model
  - file per entity, using graveler
  - diff / merge data - do we use graveler to diff, can it extend to understand the entities. do we need to associate data changes with metadata changes? how it effect the conflict resolution.
  - import / export with existing metastore - do we support just hive? do we enable export and import of the complete data model?
  - How do we store statistics?
- Passing lakeFS's reference/branch information from the metastore client.
- Co-exist with other metastores.

- Authentication with lakeFS - optional?
- Hook do we need to support in what level

## Milestone 2 - TDB

- layout list of task based on final proposal, specify dependency between each task  if any



