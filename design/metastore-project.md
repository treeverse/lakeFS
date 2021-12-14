# Next Generation Metastore - Project


## Milestone 1 - lakeFS Metastore Proposal


- *High-level design*: Map and select one of the two ways to implement the lakeFS Metastore. Understand each suggested design option, and list pros and cons. The potential architectures are: 
  - Metastore as additional functionality inside lakeFS 
  - Metastore as an external client to lakeFS

- *Data model*: Define the Data model for Metastore entities. Consider metadata access patterns, typical Metastore operations, and the operations lakeFS will provide over metadata.
  Questions we like to address:
  - Can (and should) we use Graveler to model metadata?
  - How diff, merge and commit operations will look like?
  - How are we going to tie data versioning to metadata versioning?
  - How will conflict resolution look like, and will it be affected?
  - How to enable import and export from an existing Metastore?
  - How to model Metastore's statistics?

- *Communication with lakeFS*: Investigate the options for passing lakeFS references (repository/branch/ref) from Metastore clients to lakeFS Metastore. 
  Questions we like to address:
  - How can lakeFS metastore acn get the information as a remote Hive Metastore? Any alternatives without passing the data?
  - How lakeFS Metastore can co-exist with other Metastores?

- *Metastore hooks*: What does it take to support hooks? Does anybody put it into use? is it still relevant?

- *Authentication* with lakeFS - optional. 


Open items for the design document:

- Define the relationship between Metastore and lakeFS repositories. 1:1, 1:n, m:n?

