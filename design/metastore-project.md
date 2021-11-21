# Next Generation Metastore - Project


## Milestone 1 - lakeFS Metastore Proposal

Items for discover listed with hierarchy as dependency:

- Passing reference/branch information
    - Authentication with lakeFS
    - Side by side - very similar to the current work on side-by-side work with lakeFS and the current object-storage, the user like to run the same application with minimum changes, while enabling some of the data to live inside lakeFS. What needs to change or requirements in order to have the same in the metadata level? Do we need to sync information, manage part of the data and enable mapping on the client side? Can we replace the client side implementation? do we need to spin different endpoints to serve different repositories, branches in lakeFS for parts of the metadata? fallback to existing metastore?
- Data model - Graveler to enable 3-way diff / merge that will work in optimal way.


## Milestone 2 - TDB

- layout list of task based on final proposal, specify dependency between each task  if any



