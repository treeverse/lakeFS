#!/bin/sh

# Generate sst files that will serve as an input jpebble parser tests
export CGO_ENABLED=0
cd /local
go run sst_files_generator.go
echo "done"