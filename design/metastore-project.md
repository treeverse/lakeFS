# Next Generation Metastore - Project


## Milestone 1 - lakeFS Metastore Proposal

Items to discover listed with hierarchy as dependency:

- lakeFS metastore implementation over lakeFS or as part of lakeFS.
- Co-exist with other metastores.
- Authentication with lakeFS - optional?
- Passing lakeFS's reference/branch information from the metastore client.
- Data model
  - file per entity, using graveler
      - diff / merge data - do we use graveler to diff, can it extend to understand the entities. do we need to associate data changes with metadata changes? how it effect the conflict resolution.
  - import / export with existing metastore - do we support just hive? do we enable export and import of the complete data model?
  - How do we store statistics?
- Hook do we need to support in what level

## Milestone 2 - TDB

- layout list of task based on final proposal, specify dependency between each task  if any



