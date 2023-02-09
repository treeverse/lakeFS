# Managed Uncommitted Garbage Collection for lakeFS cloud - Execution Plan

## Overview

### Goals
1. Run UGC for lakeFS cloud users
2. Allow Cloud users to clean up storage space by deleting objects from the underlying storage for cost efficiency.
3. support both repository structure (new and old) cloud users to run this SLA.

### Non-Goals
1. UGC safe writes
2. bette OSS support

### User Story

control plane interface
when creating a new cloud installation - on the garabage collection page, 
add uncoomitted garbage collection initialization with (default=true)
document costrains (lakefs writes during, explain reporxt)
default sweep = true?

1. permissions
Create user UGC with premissions for every new cloud env (enigma was added the permmision to the GC user)
Lakefs permmision- prepera uncommittedgc
Bucket permmision- read, write, list, delete

it will be a diffrent user then the gc user / the same ?
we want to combine the job together?

1. managed / cron job
- control plane interface (cloud native)
  - ugc use configuration for the spark job
  - confugrue min age seconds time
  - Step for the create cloud installation
    default = true
  - deployment of new UGC version 
  - ugc client and server compatability
  - 
- run the ugc peridioticly (aws lambda cron job, airflow dag, emr serverless, etc..)
- declee SLA (might rewuire performance improvement)
1. metrics and logging
- add looging (run report) and metrics to the ugc to join to grafana / other cloud resources
- alerti configuration / guide to on-call
- debugging access to the user metadata

## Testing
- Test UGC on scale - 
- (Enigma dev repo) Performance and correctnnes (WIP) only mark
  - Ensure only objects that are eligible for deletion are marked for deletion 
- Enigma production repo
  - old repository structure performance
- compatability test - UGC client and lakeFS server
- test sweep functionality
- E2E tests UGC cloud

## Milestone ?

## Performance improvements
- 
- if one of the tests will suggest long renning time consider adding perfomance importvment (incremental run, parrller listind, etc...)

