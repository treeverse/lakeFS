# Delta Lake catalog exporter

## Introduction

The Delta Lake table format manages its catalog of used files through a log-based system. This log, as the name implies,
contains a sequence of deltas representing changes applied to the table. The list of files that collectively represent
the table at a specific log entry is constructed by reapplying the changes stored in the log files one by one, starting
from the last checkpoint (a file that summarizes all changes up to that point) and progressing to the latest log entry.
Each log entry contains either an `add` or `remove` action, which adds or removes a data (parquet) file, ultimately
shaping the structure of the table.

In order to make Delta Lake tables accessible to external users, we aim to export the Delta Lake log to an external
location, enabling these users to read tables backed by lakeFS and Delta Lake.

---

## Proposed Solution

Following the [catalog exports issue](https://github.com/treeverse/lakeFS/issues/6461), the Delta Lake log will be
exported to the `${storageNamespace}/_lakefs/exported/${ref}/${commitId}/${tableName}/_delta_log/` path, which resides 
within the user's designated storage bucket.  
Within the `_delta_log` directory, you will find the following components:
1. The last checkpoint (or the initial log entry if no checkpoint has been established yet).
2. All the log entries that have been recorded since that last checkpoint.

Notably, the log entries will mirror those present in lakeFS, with one key distinction: Instead of utilizing relative
logical paths, they will include absolute physical paths:

#### lakeFS-backed Delta Log entry: 
```json
{ "commitInfo": {
  "timestamp": 1699199369960,
  "operation": "WRITE",
  "operationParameters": {
    "mode": "Overwrite",
    "partitionBy": "[]"
  },
  "readVersion": 2,
  ...
  }
{ "add": { 
  "path":"part-00000-72b765fd-a97b-4386-b92c-cc582a7ca176-c000.snappy.parquet",
  ...
  }
}
{ "remove": { 
  "path":"part-00000-56e72a31-0078-459d-a577-ef2c5d3dc0f9-c000.snappy.parquet",
  ...
  }
}
```

#### Exported Delta Log entry:
```json
{ "commitInfo": {
  "timestamp": 1699199369960,
  "operation": "WRITE",
  "operationParameters": {
    "mode": "Overwrite",
    "partitionBy": "[]"
  },
  "readVersion": 2,
  ...
  }
{ "add": { 
  "path":"s3://my-bucket/my-path/data/gk3l4p7nl532qibsgkv0/cl3rj1fnl532qibsglr0",
  ...
  }
}
{ "remove": { 
  "path":"s3://my-bucket/my-path/data/gk899p7jl532qibsgkv8/zxcrhuvnl532qibshouy",
  ...
  }
}
```

---

We shall use the [delta-go](https://github.com/csimplestring/delta-go) package to read the Delta Lake log since the last
checkpoint (or first entry if none found) and generate the new `_delta_log` directory with log entries as described 
above. The directory and log files will be written to `${storageNamespace}/_lakefs/exported/${ref}/${commitId}/${tableName}/_delta_log/`.
The `tableName` will be fetched from the hook's configurations.  
The Delta Lake table can now be read from `${storageNamespace}/_lakefs/exported/${ref}/${commitId}/${tableName}`.
