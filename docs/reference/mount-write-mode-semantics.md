---
title: Mount Write Mode Semantics
description: Everest mount write mode semantics.
grand_parent: Features
parent: Mount
---
# Consistency Model

## File System Consistency
Everest mount provides a strong read-after-write consistency model. 
This means that once a write operation is done, the data is guaranteed to be available for subsequent read operations.

## lakeFS Consistency
Local changes are reflected in lakeFS only after the changes are **committed**.
That means that the data is not available for other users until the changes are committed.
If, for example, two users mount the same branch they will not see each other's changes until they are committed.

# Mount Write Mode Limitations

## OS Limitations
- Mount write mode is currently supported only on macOS.

## Everest Partial-Read Mode
- Mount write mode is not supported with Everest partial-read mode.

## Functionality Limitations
- Newly created empty directories will not reflect as directory marker in lakeFS.

## File System Limitations
- Rename is not supported.
- Temporary files are not supported.
- Hard/symbolic links are not supported.
- POSIX file locks (lockf) are not supported.
- A file system directory cannot contain both a file and a directory of the same name (e.g., dir and dir/file).
- A deleted file's name cannot be used as a directory type later and the same for opposite types (e.g, Not allowed: touch foo; rm foo; mkdir foo;).
- Calling remove on a directory type will fail explicitly with an error.
- Remove is not an atomic operation.