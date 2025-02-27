---
title: Mount Write Mode Semantics
description: Everest mount file system semantics, limitations and and general info.
grand_parent: Features
parent: Mount
---
# Mount Write Mode Semantics

{: .d-inline-block }
experimental
{: .label .label-red }

## Consistency Model

### File System Consistency
Everest mount provides a strong read-after-write consistency model. 
This means that once a write operation is done, the data is guaranteed to be available for subsequent read operations.

### lakeFS Consistency
Local changes are reflected in lakeFS only after the changes are **committed**.
That means that the data is not available for other users until the changes are committed.
If, for example, two users mount the same branch, they will not see each other's changes until they are committed.

## Mount Write Mode File System Behavior

### Functionality Limitations
- Newly created empty directories will not reflect as directory markers in lakeFS.
- lakeFS allows having 2 path keys that one is a "directory" prefix of the other, for example the following 2 lakeFS keys are valid: `animals/cat.png` and `animals` (empty object) but since a file system cannot contain both a file and a directory of the same name it will lead to an undefined behavior depending on the Filesystem type (e.g., dir and dir/file).

### File System Behavior

#### Not Supported:
- Rename is not supported.
- Temporary files are not supported.
- Hard/symbolic links are not supported.
- POSIX file locks (lockf) are not supported.
- POSIX permissions are not supported- default permissions are given to files and dirs.
- A deleted file's name cannot be used as a directory type later and the same for opposite types (e.g, Not allowed: touch foo; rm foo; mkdir foo;).
- Calling remove on a directory type will fail explicitly with an error.

#### Behavior modified:
- Modifying file metadata (chmod, chown, chgrp, time) will result in noop (the file metadata will not be changed). 
- Remove:
  - (a) Open handlers to a file will error if the file is removed during a read operation.
  - (b) Removal is not an atomic operation, calling remove and open at the same time might result in a race condition where the open might succeed.
