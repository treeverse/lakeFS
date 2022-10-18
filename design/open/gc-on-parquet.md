## Garbage collection using Parquet format

### Motivation

Garbage collection in lakeFS is struggling at large scale.

### Why Parquet?

Parquet is more than a file format. It's a breathing open-source project aiming to provide exactly what we're looking for here: efficient data retrieval. GC is essentially a huge anti-join, and it may be very difficult to achieve a similar optimization in our dedicated format.


### Steps

#### 1. Sync
Ranges of the repository are translated into Parquet files. These are stored in a new metadata location under the repository's storage namespace.

#### 2. Sanity
Verify all the ranges required for this run are present, and that the number of entries in each range is correct.

#### 3. Start
Do an anti-join extracting all the addresses that need to be deleted.

### Considerations
1. Storage: this requires using more of the user's storage. I don't think that's an issue since metadata is usually very small compared to the data size. In the future we can also optimize the set of ranges we actually bring there.
1. Risk: we are using a copy of the metadata instead of the actual one. We need to be careful not to miss anything, since missing data may potentially cause innocent objects to be deleted.  This is a risk that any GC solution needs to take into account.
1. Ops burden: we will have to maintain and monitor the copying of the metadata. In case it fails, we need to recover it.
