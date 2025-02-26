---
title: Mount Write Mode Limitations
description: Limitations of the mount write mode
grand_parent: Features
parent: Mount
---

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
