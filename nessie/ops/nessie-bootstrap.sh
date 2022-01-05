#!/bin/bash
apk add --no-cache util-linux
go test -v ./nessie --system-tests
